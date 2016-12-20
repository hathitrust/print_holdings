require 'hathidata';
require 'hathidb';
require 'hathilog';
require 'json';
require 'set';
require 'date';

# Todos:
# Make tables [x]
# Data validation
# Nice logging

db         = Hathidb::Db.new();
conn       = db.get_conn();
member_id  = ARGV.shift;
input_file = ARGV.shift;
log        = Hathilog::Log.new();

# Queries
in_pool_sql          = "SELECT COUNT(*) AS c FROM shared_print_pool WHERE member_id = ?";
get_max_other_id_sql = "SELECT COALESCE(MAX(id), 0) + 1 AS max_id FROM shared_print_other";
insert_other_sql     = "INSERT INTO shared_print_other (id, sp_program, retention_date) VALUES (?,?,?)";
get_other_id_sql     = "SELECT other_commitment_id FROM shared_print_commitments WHERE member_id = ?";
delete_other_sql     = "DELETE FROM shared_print_other WHERE id = ?";
delete_sql           = "DELETE FROM shared_print_commitments WHERE member_id = ?";
get_oclc_diff_sql    = %w{
    SELECT spc.oclc
    FROM shared_print_commitments AS spc
    LEFT JOIN shared_print_pool   AS spp
    ON (spc.member_id = spp.member_id AND spc.oclc = spp.oclc)
    WHERE spc.member_id = ?
    AND spp.oclc IS NULL
}.join(' ');

in_pool_q           = conn.prepare(in_pool_sql);
get_max_other_id_q  = conn.prepare(get_max_other_id_sql);
insert_other_q      = conn.prepare(insert_other_sql);
get_other_id_q      = conn.prepare(get_other_id_sql);
delete_other_q      = conn.prepare(delete_other_sql);
delete_q            = conn.prepare(delete_sql);
get_oclc_diff_q     = conn.prepare(get_oclc_diff_sql);


# Make sure these match the ENUM declarations in ingest.sql.
enums = {
  'local_shelving_type' => Set.new(%w[cloa clca sfca sfcahm sfcaasrs]),
  'lending_policy'      => Set.new(%w[a b]),
};
sp_program_enum = Set.new(%w[test east west north south]);

# Check member_id
sp_member = false;
Hathidata.read('shared_print_members.tsv') do |line|
  if line.strip == member_id then
    sp_member = true;
  end
end
if !sp_member then
  log.f("#{member_id} is not a shared print member.");
  exit(1);
end
records_in_pool = 0;
in_pool_q.enumerate(member_id) do |row|
  records_in_pool = row[:c].to_i;
end
if records_in_pool == 0 then
  log.f("#{member_id} has no records in shared print pool.");
  exit(1);
end

# Check profile file
profile         = Hathidata::Data.new("shared_print_profiles/#{member_id}.json");
default_profile = Hathidata::Data.new("shared_print_profiles/default.json");
profile_data    = {};

if profile.exists? then
  log.d("Found profile #{profile.path}");
  profile_json = [];
  profile.open('r');
  profile.file.each_line do |line|
    profile_json << line.strip;
  end
  profile.close();
  profile_data = JSON.parse(profile_json.join(' '));
else
  # If none exists, copy default.json and abort.
  log.d("Could not find profile #{profile.path}, generating default");
  profile.open('w');
  default_profile.open('r').file.each_line do |line|
    profile.file.print(line);
  end
  profile.close();
  log.f("END.");
  exit(1);
end

# Check profile data
default_json = [];
default_profile.open('r').file.each_line do |line|
  default_json << line.strip;
end
default_profile.close();
default_data = JSON.parse(default_json.join(' '));
req_fields = {};
## Ensure all required elements are present
default_data.keys.each do |def_k|
  if default_data[def_k] == "REQ" then
    req_fields[def_k] = true;
    if !profile_data.has_key?(def_k) then
      log.f("#{profile.path} missing REQ attribute #{def_k}");
      exit(1);
    end
  end
end

## Ensure all elements in profile are allowed and their values OK.
profile_data.keys.each do |pro_k|
  if !default_data.has_key?(pro_k) then
    log.f("#{pro_k} not an OK element in #{profile.path}");
    exit(1);
  elsif profile_data[pro_k] =~ /OPT|REQ/ then
    log.f("#{pro_k} not instantiated in #{profile.path}");
    exit(1);
  elsif profile_data[pro_k].class.to_s != 'Fixnum' then
    log.f("#{pro_k} not a Fixnum in #{profile.path}");
    exit(1);
  end
end

# Delete previous commitments
get_other_id_q.enumerate(member_id) do |row|
  delete_other_q.execute(row[:other_commitment_id]);
end
log.d(delete_sql);
delete_q.execute(member_id);

# Parse to MySQL-ready file using profile
dat_file    = Hathidata::Data.new("ingest_commitment_#{member_id}.dat").open('w');
commitments = Hathidata::Data.new(input_file).open('r');
line_count  = 0;
warn_count  = 0;

commitments.file.each_line do |line|
  line_count += 1;
  next if line_count == 1;
  warning = false;
  query_cols = profile_data.clone;
  line_cols  = line.split("\t").map{ |x| x.strip.downcase };
  if line_cols.size < query_cols.values.max then
    log.w("Short line on line #{line_count}");
    warning = true;
  end

  query_cols.each do |k,v|
    # Check that required fields aren't empty
    if req_fields.has_key?(k) && (line_cols[v].nil? || line_cols[v] == '') then
      log.w("Missing value for required field #{k} on line #{line_count}");
      warning = true;
    end
    # Check that enums arent violated
    if enums.has_key?(k) && !enums[k].include?(line_cols[v]) then
      log.w("#{line_cols[v]} violates #{k} enum declaration on line #{line_count}");
      warning = true;
    end

    query_cols[k] = line_cols[v];
  end
  ## Normalize/scrub individual fields here.
  ## Do extra stuff for lending_policy/other_commitments
  ## or whatever col should be broken out as separate table.

  # Special treatment of "other_commitments"
  # Syntax allowed:
  # single   | program : date
  # multiple | program1 : date1; program2 : date2
  # Date format: YYYY-MM-DD
  if query_cols.has_key?('other_commitments') && !query_cols['other_commitments'].nil? then
    pairs    = query_cols['other_commitments'].split(';');
    ok_pairs = [];
    other_id = nil;
    pairs.each do |p|
      (program, date) = p.split(':');
      # Check that program doesn't violate enum
      program.downcase!
      program.strip!
      if !sp_program_enum.include?(program) then
        log.w("#{program} is invalid enum value for other_commitments on line #{line_count}");
        warning = true;
        next;
      end
      # Make sure date is kosher.
      (yyyy, mm, dd) = date.split('-').map{|x| x.to_i};
      begin
        Date.new(yyyy, mm, dd);
      rescue ArgumentError
        log.w("Bad date #{date} for other_commitment on line #{line_count}");
        warning = true;
        next;
      end
      ok_pairs << [program, date];
    end
    # Insert and save id in query_cols.
    if ok_pairs.size > 0 then
      get_max_other_id_q.enumerate do |row|
        other_id = row[:max_id];
      end
      ok_pairs.each do |program, date|
        insert_other_q.execute(other_id, program, date);
      end
    end
    query_cols.delete('other_commitments');
    query_cols['other_commitment_id'] = other_id;
  end

  # Only print to dat file if no warnings.
  if warning then
    warn_count += 1;
  else
    # Make sure query_cols contains member_id.
    # If it wasnt given in the file, use the ARGV one.
    query_cols['member_id'] ||= member_id;
    dat_file.file.puts(query_cols.keys.sort.map{ |x| query_cols[x] }.join("\t"));
  end
end
if warn_count > 0 then
  log.w("#{warn_count} lines skipped due to warnings");
end
commitments.close();
dat_file.close();

# Make sure profile data contains member_id, even though it is optional.
profile_data['member_id'] ||= true;
if profile_data.has_key?('other_commitments') then
  profile_data.delete('other_commitments');
  profile_data['other_commitment_id'] = true;
end
# Load file
load_sql = %W{
    LOAD DATA LOCAL INFILE ?
    INTO TABLE shared_print_commitments
    (#{profile_data.keys.sort.join(',')})
}.join(' ')
log.d(load_sql);
load_q = conn.prepare(load_sql);
load_q.execute(dat_file.path);

# dat_file.delete;

# Check that all inserted OCLCs match what the member has in shared_print_pool
get_oclc_diff_q.enumerate(member_id) do |row|
  log.w("oclc #{row[:oclc]} does not match shared print pool");
  # Delete it?
  # If so, also check other commitments.
end
