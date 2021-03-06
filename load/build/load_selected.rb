require 'hathilog';
require 'hathidb';
require 'hathienv';
require 'hathidata';
require 'hathiquery';
require 'fileutils';

# Given a file data/builds/current/load.tsv, with tab-separated
# member_ids and item_types, this script will load those files
# from the memberdata dir, to the HT00x directory and from there
# into the holdings_memberitem table.
#
# Before running, make sure that all the right files are in the
# right place and have been preprocessed as needed. Make backups
# of the currently loaded files before reloading.
#
# The script will pretty much scream and die if anything isn't
# to its liking.

@log             = Hathilog::Log.new({:file_name => 'builds/current/load_selected_rb.log'});
@db              = Hathidb::Db.new();
@conn            = @db.get_conn();
@member_data_dir = Hathidata::Data.new('memberdata/').path;
@ht_dir          = Hathidata::Data.new('loadfiles/').path;
@ht_backup_dir   = Hathidata::Data.new('backup/loadfiles/').path;
@dry_run         = false; # if @dry_run then skip updates and stuff. Turn on with -n flag.

# Takes a file with instructions about which files to copy and load.
def get_infiles
  hdin = Hathidata::Data.new('builds/current/load.tsv');
  if !hdin.exists? then
    msg = "Could not find #{hdin.path}";
    @log.f(msg);
    raise msg;
  end

  # Keeps 'chi' => 'mono' and the like.
  members    = {};
  # Hash, so we can tell which are bad member_ids.
  member_ids = get_memberids();

  # Expecting infile format as one member_id and item_type per line,
  # separated by tab, just like so:
  # ^chi<TAB>mono$
  # ^chi<TAB>multi$
  # ... etc.

  @log.d("#{hdin.path} has the following to say:");
  hdin.open('r').file.each_line do |line|
    line.strip!;
    next if line[/^#/];
    next if line[/^\s*$/];
    m = /^([a-z\.\-]+)\t(mono|multi|serial)$/.match(line);
    if m != nil then
      member_id = m[1];
      type      = m[2];
      if !member_ids.has_key?(member_id) then
        msg = "member_id #{member_id} is not an active member in holdings_htmember.";
        @log.f(msg);
        raise msg;
      end
      @log.d("#{member_id} | #{type}");
      if !members.has_key?(member_id) then
        members[member_id] = [];
      end
      members[member_id] << type;
    else
      msg = "Failed to recognize #{line} as an OK input.";
      @log.f(msg);
      raise msg;
    end
  end
  hdin.close();
  infiles = [];

  @log.d("Checking input files and dirs...");
  members.keys.sort.each do |k|
    mem_dir = @member_data_dir + k;
    if Dir.exists?(mem_dir) then
      @log.d("#{mem_dir} OK dir");
      members[k].sort.each do |type|
        path = "#{mem_dir}/HT003_#{k}.#{type}.tsv";
        if File.exists?(path) then
          infiles << path;
          @log.d("#{path} OK file");
        else
          msg = "No file #{path}";
          @log.f(msg);
          raise msg;
        end
      end
    else
      msg = "No dir for #{mem_dir}";
      @log.f(msg);
      raise msg;
    end
  end

  if infiles.length == 0 then
    msg = "No infiles found.";
    @log.f(msg);
    raise msg;
  end

  return infiles;
end

# Takes a list of files in the memberdata dir, copiess them
# to the ht00x dir. Backs up the original ht00x files, if any.
def copy_files (infiles)
  @log.d("Backing up files...");
  htfiles = [];
  backup_dir = @ht_backup_dir + Time.new().strftime("%Y%m%d");
  infiles.each do |infile|
    ht_file = @ht_dir + infile.split('/').last;
    # Check if file exists in ht00x
    if File.exists?(ht_file) then
      # If so, back it up.
      FileUtils.mkdir_p(backup_dir);
      @log.d("cp #{ht_file} #{backup_dir}");
      if @dry_run == false then
        FileUtils.cp(ht_file, backup_dir);
      end
    end
    # Overwrite with one from memberdata (copy)
    @log.d("cp #{infile} #{ht_file}");
    if @dry_run == false then
      FileUtils.cp(infile, ht_file);
    end
    htfiles << ht_file;
  end

  return htfiles;
end

# Takes a list of files in the ht00x dir. For each file, performs
# a reload of that type of record, i.e. one delete and one load.
def process_htfiles (htfiles)
  delete_sql = %w[
    DELETE FROM holdings_memberitem
     WHERE member_id = ?
       AND item_type = ?
  ].join(' ');
  load_sql = %w[
    LOAD DATA LOCAL INFILE ?
    INTO TABLE holdings_memberitem IGNORE 1 LINES
    (oclc, local_id, member_id, status, item_condition,
    process_date, enum_chron, item_type, issn, n_enum, n_chron, gov_doc)
  ].join(' ');

  @log.d(delete_sql);
  @log.d(load_sql);

  delete_query = @conn.prepare(delete_sql);
  load_query   = @conn.prepare(load_sql);

  htfiles.each do |infile|
    m = /HT003_([a-z\.\-]+)\.(mono|multi|serial).tsv/.match(infile.to_s);
    if m != nil then
      member_id = m[1];
      item_type = m[2];
      @log.d("For infile #{infile}, reload all #{member_id} #{item_type}.");
      line_count = %x(wc -l #{infile}).strip;
      @log.d("Line count: #{line_count}");
      @log.d("Counts for #{member_id} before:");
      show_counts(member_id);
      if @dry_run == false then
        # Delete
        @log.d("Deleting #{member_id} #{item_type}");
        delete_query.update(member_id, item_type);
        # Load
        @log.d("Loading #{member_id} #{item_type}.");
        load_query.update(infile);
        # Count
        @log.d("Counts for #{member_id} after:");
        show_counts(member_id);
      end
      @log.d("Done with #{member_id} #{item_type}\n");
    else
      msg = "Could not figure out member_id / item_type from #{infile}";
      @log.f(msg);
      raise msg;
    end
  end
end

# Gives a list of all active members.
# So we can bail if an invalid member_id is given.
def get_memberids ()
  member_ids = {};
  active_members_sql = Hathiquery.get_active_members;
  @log.d(active_members_sql);
  @conn.query(active_members_sql) do |row|
    member_ids[row[:member_id]] = true;
  end

  @log.d("Seeing #{member_ids.length} active members.\n" + member_ids.keys.sort.join("\n"));

  return member_ids;
end

# Useful for logging change, run before and after a reload.
def show_counts (member_id)
  sql = %w<
    SELECT
        COUNT(1) AS c,
        item_type
    FROM
        holdings_memberitem
    WHERE
        member_id = ?
    GROUP BY
        item_type
    ORDER BY
        item_type
  >.join(' ');

  query = @conn.prepare(sql);
  query.enumerate(member_id) do |row|
    @log.d("#{row[:c]} #{row[:item_type]}");
  end
end

def optimize_table ()
  @log.d("Optimizing table.");
  sql = "OPTIMIZE TABLE holdings_memberitem";
  @log.d(sql);
  res = @conn.execute(sql)
  res.each do |row|
    @log.d(row.to_a.join(" "));
  end
end

def check_values
  @log.d("Checking values");
  checks = [
   {:col => 'item_condition', :ok_vals => ['', 'BRT']},
   {:col => 'status',         :ok_vals => ['', 'CH', 'LM', 'WD']},
   {:col => 'item_type',      :ok_vals => ['mono', 'multi', 'serial']},
  ];
  checks.each do |check|
    col  = check[:col];
    vals = check[:ok_vals].map{|x| "'#{x}'"}.join(',');
    @log.d("#{col}:");
    sql = %W<
      SELECT member_id, #{col}, COUNT(#{col}) AS c
      FROM holdings_memberitem
      WHERE #{col} NOT IN (#{vals})
      GROUP BY member_id, #{col}
      ORDER BY c
    >.join(' ');
    @log.d(sql);

    bad_count = 0;
    @conn.query(sql) do |row|
      bad_count += 1;
      @log.d(row.to_a.map{|x| "'#{x}'"}.join(','));
    end
    if bad_count > 0 then
      errmsg = "#{bad_count} bad values in #{col}!";
      @log.e(errmsg);
      raise errmsg;
    end
    @log.d("#{col} OK!");
  end
  @log.d("Done checking values.");
end

# MAIN:
if $0 == __FILE__ then
  begin
    @log.d("Started\n\n\n");

    if ARGV.include?('-n') then
      @dry_run = true;
      @log.d("*** DRY RUN ***");
    end

    infiles = get_infiles();
    htfiles = copy_files(infiles);
    @conn = @db.get_conn();
    process_htfiles(htfiles);
    # check_values();
    # optimize_table() unless @dry_run;
  rescue StandardError => error
    @log.f("Something went wrong:");
    @log.f(error);
  ensure
    @log.d("Finished\n\n\n");
    @log.close();
    @conn.close();
  end
end
