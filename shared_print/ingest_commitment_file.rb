require 'hathidata';
require 'hathidb';
require 'hathilog';
require 'json';
require 'set';
require 'date';
require 'scrub';

# Description:
# A shared print member sends us a file with their commitments.
# They have to be listed in the shared print members file and must have records in shared_print pool.
# They must also have a profile file telling this script which column is which. If none exists,
# a copy of the default profile will be made, and then it needs to be edited.
# When parsing the commitments file, only lines with 0 warnings will be added to the DB.
# Lines with warnings get printed to a separate file (@bad_file) with comments.
# Commitments with OCLC numbers that don't match the shared print pool will... be deleted?

# For each member, generate a shared print profile based on default.json.
# One will be made for you if you try to run this script without one.

# Call like so:
#   ruby ingest_commitment_file.rb <member_id> <path_to_input_file>
# In case you just want to ADD records (partial update) add --update:
#   ruby ingest_commitment_file.rb <member_id> <path_to_input_file> --update

def run
  db          = Hathidb::Db.new();
  @conn       = db.get_conn();
  @member_id  = ARGV.shift;
  @input_file = ARGV.shift;
  @update     = ARGV.include?('--update');
  @log        = Hathilog::Log.new({
      :file_name => "shared_print/ingest/#{@member_id}_$ymd.log.txt",
      :log_level => 1
    });

  # Queries
  @in_pool_sql          = "SELECT COUNT(*) AS c FROM shared_print_pool WHERE member_id = ?";
  @get_max_other_id_sql = "SELECT COALESCE(MAX(id), 0) + 1 AS max_id FROM shared_print_other";
  @insert_other_sql     = "INSERT INTO shared_print_other (id, sp_program, retention_date, indefinite) VALUES (?,?,?,?)";
  @get_other_id_sql     = "SELECT other_commitment_id FROM shared_print_commitments WHERE member_id = ?";
  @delete_other_sql     = "DELETE FROM shared_print_other WHERE id = ?";
  @delete_sql           = "DELETE FROM shared_print_commitments WHERE member_id = ?";
  @resolve_oclc_sql     = "SELECT oclc_x FROM oclc_resolution WHERE oclc_y = ?";
  @get_oclc_diff_sql    = %w{
  SELECT spc.resolved_oclc
    FROM shared_print_commitments AS spc
    LEFT JOIN shared_print_pool   AS spp
    ON (spc.member_id = spp.member_id AND spc.resolved_oclc = spp.resolved_oclc)
    WHERE spc.member_id = ?
    AND spp.resolved_oclc IS NULL
  }.join(' ');
  @get_other_id_by_ocn  = "SELECT other_commitment_id FROM shared_print_commitments WHERE resolved_oclc = ? AND member_id = ?";
  @del_oclc_diff_sql    = "DELETE FROM shared_print_commitments WHERE member_id = ? AND resolved_oclc = ?";


  # Prep queries
  @in_pool_q          = @conn.prepare(@in_pool_sql);
  @get_max_other_id_q = @conn.prepare(@get_max_other_id_sql);
  @insert_other_q     = @conn.prepare(@insert_other_sql);
  @get_other_id_q     = @conn.prepare(@get_other_id_sql);
  @delete_other_q     = @conn.prepare(@delete_other_sql);
  @delete_q           = @conn.prepare(@delete_sql);
  @resolve_oclc_q     = @conn.prepare(@resolve_oclc_sql);
  @get_oclc_diff_q    = @conn.prepare(@get_oclc_diff_sql);
  @get_other_id_by_ocn_q = @conn.prepare(@get_other_id_by_ocn);
  @del_oclc_diff_q    = @conn.prepare(@del_oclc_diff_sql);

  @counts       = {};
  @profile_data = {};
  @req_fields   = {};
  @dat_file     = nil;

  # Make sure these match the ENUM declarations in ingest.sql.
  @enums = {
    'local_shelving_type'   => Set.new(%w[cloa clca sfca sfcahm sfcaasrs]),
    'lending_policy'        => Set.new(%w[blo]),
    'scanning_repro_policy' => Set.new(['do not reproduce']),
  };
  @sp_program_enum = Set.new(%w[coppul east flare ivyplus mssc recap ucsp viva other]);
  @scrubber        = MemberScrubber.new({'data_type' => 1, 'logger' => Hathilog::Log.new()});

  # Enough setup. Setup makes weak. Run makes strong. Run!
  check_member();
  check_profile();
  if @update == false then
    delete_previous_commitments();
  end
  parse_file();
  load_data();
  check_loaded_records();
  log_counts();
  @log.close();
end

def check_member
  # Make sure member_id matches a kosher shared print member.
  sp_member = false;
  Hathidata.read('shared_print_members.tsv') do |line|
    if line.strip == @member_id then
      sp_member = true;
    end
  end
  if !sp_member then
    @log.f("#{@member_id} is not a shared print member.");
    exit(1);
  end
  records_in_pool = 0;
  @in_pool_q.enumerate(@member_id) do |row|
  records_in_pool = row[:c].to_i;
  end
  if records_in_pool == 0 then
    @log.f("#{@member_id} has no records in shared print pool.");
    exit(1);
  end
  countx(:records_in_pool, records_in_pool);
end

def check_profile
  # Make sure member has an OK shared print profile file.
  profile         = Hathidata::Data.new("shared_print_profiles/#{@member_id}.json");
  default_profile = Hathidata::Data.new("shared_print_profiles/default.json");
  if profile.exists? then
    @log.d("Found profile #{profile.path}");
    profile_json = [];
    profile.open('r');
    profile.file.each_line do |line|
      profile_json << line.strip;
    end
    profile.close();
    @profile_data = JSON.parse(profile_json.join(' '));
  else
    # If none exists, copy default.json and abort.
    @log.d("Could not find profile #{profile.path}, generating default");
    profile.open('w');
    default_profile.open('r').file.each_line do |line|
      profile.file.print(line);
    end
    profile.close();
    @log.f("END.");
    exit(1);
  end

  # Check profile data
  default_json = [];
  default_profile.open('r').file.each_line do |line|
    default_json << line.strip;
  end
  default_profile.close();
  default_data = JSON.parse(default_json.join(' '));

  ## Ensure all required elements are present
  default_data.keys.each do |def_k|
    if default_data[def_k] == "REQ" then
      @req_fields[def_k] = true;
      if !@profile_data.has_key?(def_k) then
        @log.f("#{profile.path} missing REQ attribute #{def_k}");
        exit(1);
      end
    end
  end

  ## Ensure all elements in profile are allowed and their values OK.
  @profile_data.keys.each do |pro_k|
    if !default_data.has_key?(pro_k) then
      @log.f("#{pro_k} not an OK element in #{profile.path}");
      exit(1);
    elsif @profile_data[pro_k] =~ /OPT|REQ/ then
      @log.f("#{pro_k} not instantiated in #{profile.path}");
      exit(1);
    elsif @profile_data[pro_k].class.to_s != 'Fixnum' then
      @log.f("#{pro_k} not a Fixnum in #{profile.path}");
      exit(1);
    end
  end
end

def delete_previous_commitments
  @get_other_id_q.enumerate(@member_id) do |row|
    @delete_other_q.execute(row[:other_commitment_id]);
  end
  @log.d(@delete_sql);
  @delete_q.execute(@member_id);
end

# Parse to MySQL-ready file using profile
def parse_file
  @dat_file   = Hathidata::Data.new("ingest_commitment_#{@member_id}.dat").open('w');
  @bad_file   = Hathidata::Data.new("#{@member_id}_print_retention_warnings_$ymd.tsv").open('w');
  commitments = Hathidata::Data.new(@input_file).open('r');
  line_count  = 0;
  warn_count  = 0;

  commitments.file.each_line do |line|
    line_count += 1;
    line.chomp! # Was strip but that removes leading tab :(
    # Skip header
    if line_count == 1 then
      # Copy line header to .bad file, add comments col.
      @bad_file.file.puts("lineno\tcomments\t" + line);
      next;
    end
    warnings   = [];
    query_cols = @profile_data.clone;
    line_cols  = line.split("\t").map{ |x| x.strip.downcase };
    if line_cols.size < query_cols.values.max then
      warnings << 'short_line';
    end

    query_cols.each do |k,v|
      # Check that required fields aren't empty
      if @req_fields.has_key?(k) && (line_cols[v].nil? || line_cols[v] == '') then
        @log.w("Missing value for required field #{k} on line #{line_count}");
        warnings << "missing_#{k}";
      end
      # Check that enums aren't violated
      if @enums.has_key?(k) && !line_cols[v].empty? && !@enums[k].include?(line_cols[v]) then
        @log.w("#{line_cols[v]} violates #{k} enum declaration on line #{line_count}");
        warnings << "bad_enum_#{k}";
      end

      query_cols[k] = line_cols[v];

      ## Normalize/scrub individual fields here.
      if k =~ /^(local|resolved)_oclc$/ then
        # Scrub oclc
        original = query_cols[k];
        begin
          query_cols[k] = @scrubber.parse_oclc(query_cols[k]);
        rescue => e
          @log.e("#{e.class} when parsing oclc '#{original}'");
        end
        if query_cols[k] == 0 then
          @log.w("'#{original}' is a bad #{k} on line #{line_count}");
          warnings << "bad_#{k}";
        end
      end
    end

    # Special treatment of "other_commitments"
    # Syntax allowed:
    # single   | program : [date|indefinite]
    # multiple | program1 : [date|indefinite] ; program2 : [date|indefinite]
    # Date format: YYYY-MM-DD
    # Indefinite format: "indefinite", use this if there is no end date.
    if query_cols.has_key?('other_commitments') && !query_cols['other_commitments'].nil? then
      pairs    = query_cols['other_commitments'].split(';');
      ok_pairs = [];
      other_id = nil;
      pairs.each do |p|
        (program, date) = p.split(':');
        # Check that program doesn't violate enum
        program.downcase!
        program.strip!
        date.downcase!
        date.strip!
        if !@sp_program_enum.include?(program) then
          @log.w("#{program} is invalid enum value for other_commitments on line #{line_count}");
          warnings << "bad_other_commitments";
          next;
        end
        # Make sure date is kosher.
        # Date can be 'indefinite'.
        if date != 'indefinite' then
          (yyyy, mm, dd) = date.split('-').map{|x| x.to_i};
          begin
            Date.new(yyyy, mm, dd);
          rescue ArgumentError
            @log.w("Bad date #{date} for other_commitment on line #{line_count}");
            warnings << "bad_date_other_commitments";
            next;
          end
        end
        ok_pairs << [program, date];
      end
      # Insert and save id in query_cols.
      if ok_pairs.size > 0 then
        @get_max_other_id_q.enumerate do |row|
          other_id = row[:max_id];
        end
        ok_pairs.each do |program, date|
          indef = 0;
          if date == 'indefinite' then
            indef = 1;
            date  = nil;
          end
          @insert_other_q.execute(other_id, program, date, indef);
          countx(:other_commitment_ok);
        end
      end
      query_cols.delete('other_commitments');
      query_cols['other_commitment_id'] = other_id;
    end

    # Only print to @dat_file if no warnings. Print to @bad_file otherwise.
    if warnings.size > 0 then
      warn_count += 1;
      # Count your ble-, um, warnings.
      warnings.map{|x| countx(x)};
      @bad_file.file.puts("#{line_count}\t" + warnings.join(",") + "\t" + line);
    else
      # Make sure query_cols contains member_id.
      # If it wasnt given in the file, use the ARGV one.
      query_cols['member_id'] ||= @member_id;

      # Use provided resolved_oclc or resolve local_oclc.
      if !query_cols.has_key?('resolved_oclc') then
        query_cols['resolved_oclc'] = resolve_oclc(query_cols['local_oclc']);
      end

      @dat_file.file.puts(query_cols.keys.sort.map{ |x| query_cols[x] }.join("\t"));
      countx(:output_file_lines);
    end
  end

  countx(:input_file_line_count, line_count-1);
  countx(:input_file_warn_lines, warn_count);
  countx(:input_file_ok_count, line_count - 1 - warn_count);

  if warn_count > 0 then
    @log.w("#{warn_count} lines skipped due to warnings");
  end
  commitments.close();
  @bad_file.close();
  @dat_file.close();
end

def resolve_oclc (oclc)
  # Take an oclc number as input. Look it up in resolution table.
  # If no hit, return input.
  resolved_oclc = oclc;
  @resolve_oclc_q.enumerate(oclc) do |row|
    resolved_oclc = row[:oclc_x];
    countx(:oclc_resolution_lookups);
    break;
  end
  return resolved_oclc;
end

def load_data
  # Make sure profile data contains member_id, even though it is optional.
  @profile_data['member_id'] ||= true;
  if @profile_data.has_key?('other_commitments') then
    @profile_data.delete('other_commitments');
    @profile_data['other_commitment_id'] = true;
  end

  @profile_data['resolved_oclc'] ||= true;

  # Load file
  load_sql = %W{
    LOAD DATA LOCAL INFILE ?
    INTO TABLE shared_print_commitments
    (#{@profile_data.keys.sort.join(',')})
}.join(' ')
  @log.i(load_sql);
  load_q = @conn.prepare(load_sql);
  load_q.execute(@dat_file.path);
  # @dat_file.delete;
end

def check_loaded_records
  # Check that all inserted OCLCs match what the member has in shared_print_pool
  @get_oclc_diff_q.enumerate(@member_id) do |row|
    bad_ocn = row[:resolved_oclc];
    @log.w("oclc #{bad_ocn} does not match shared print pool, rejected");
    countx(:oclc_not_in_sp);

    # Check if there are any other_commitments that need to be deleted first.
    @get_other_id_by_ocn_q.enumerate(bad_ocn, @member_id) do |row_oc|
      @delete_other_q.execute(row_oc[:other_commitment_id]);
    end
    # Delete commitment
    @del_oclc_diff_q.execute(@member_id, bad_ocn);
  end
end

def log_counts
  @counts.keys.sort.each do |k|
    @log.i("#{k}:#{@counts[k]}");
  end
end

def countx (name, incr=1)
  name = name.to_s;
  @counts[name] ||= 0;
  @counts[name] += incr;
end

if $0 == __FILE__ then
  run();
end
