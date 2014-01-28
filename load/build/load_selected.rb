require 'hathilog';
require 'hathidb';
require 'hathienv';
require 'hathidata';
require 'hathiquery';

# Given a file data/builds/current/load.tsv, with tab-separated
# member_ids and item_types, this script will load those files
# from the HT00x directory into the holdings_memberitem table.
#
# Before running, make sure that all the right files are in the
# right place and have been preprocessed as needed.
#
# The script will pretty much scream and die if anything isn't
# to its liking.

@log  = Hathilog::Log.new({:file_name => 'builds/current/load_selected.log'});
db    = Hathidb::Db.new();
@conn = db.get_conn();
@member_data_dir = '/htapps/pulintz.babel/data/phdb/MemberData';

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
  # etc.

  hdin.open('r').file.each_line do |line|
    line.strip!;
    next if line[/^#/];
    next if line[/^\s*$/];
    puts line;
    m = /^([a-z]+)\t(mono|multi|serial)$/.match(line);
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
    end
  end
  hdin.close();

  infiles = [];
  @log.d("Looping keys");
  members.keys.sort.each do |k|
    mem_dir = @member_data_dir + "/#{k}";
    if Dir.exists?(mem_dir) then
      @log.d("#{mem_dir} OK");
      members[k].sort.each do |type|
        path = "#{mem_dir}/HT003_#{k}.#{type}.tsv";
        if File.exists?(path) then
          infiles << path;
          @log.d("#{path} OK");
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

def get_memberids ()
  member_ids = {};
  active_members_sql = Hathiquery.get_active_members;
  @log.d(active_members_sql);
  @conn.query(active_members_sql) do |row|
    member_ids[row[:member_id]] = true;
  end

  return member_ids;
end

def show_counts (member_id)
  sql = %W<
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

def process_infiles (infiles)
  delete_sql = %W[
    DELETE FROM holdings_memberitem 
     WHERE member_id = ? 
       AND item_type = ?
  ].join(' ');
  @log.d(delete_sql);
  delete_query = @conn.prepare(delete_sql);

  load_sql = %W[
       LOAD DATA LOCAL INFILE ?
       INTO TABLE holdings_memberitem IGNORE 1 LINES
       (oclc, local_id, member_id, status, item_condition,
       process_date, enum_chron, item_type, issn, n_enum, n_chron)
      ].join(' ');
  @log.d(load_sql);
  load_query = @conn.prepare(load_sql);

  infiles.each do |infile|
    m = /HT003_([a-z]+)\.(mono|multi|serial).tsv/.match(infile);
    if m != nil then
      member_id = m[1];
      item_type = m[2];
      @log.d("For infile #{infile}, reload all #{member_id} && #{item_type}.");
      line_count = %x(wc -l #{infile}).strip;
      @log.d("Line count: #{line_count}");
      @log.d("Counts for #{member_id} before:");
      show_counts(member_id);
      @log.d("Deleting #{member_id} && #{item_type}");
      # delete_query.update(member_id, item_type);
      @log.d("Loading #{member_id} && #{item_type}.");
      # load_query.update(infile);
      @log.d("Counts for #{member_id} after:");
      show_counts(member_id);
      @log.d("Done with #{member_id} #{item_type}\n");
    else
      msg = "Could not figure out member_id / item_type from #{infile}";
      @log.f(msg);
      raise msg;
    end
  end
end

# MAIN:
if $0 == __FILE__ then
  begin
    @log.d("Started\n\n\n");
    infiles = get_infiles();
    process_infiles(infiles);
  ensure
    @log.d("Finished\n\n\n");
    @log.close();
    @conn.close();
  end
end
