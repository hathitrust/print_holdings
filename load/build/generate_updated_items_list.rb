require 'hathidb';
require 'hathidata';
require 'hathilog';
require 'set';

# Copied from /htapps/pulintz.babel/Code/phdb/bin/generate_updated_items_list.rb
# and rewritten a bit.

# Corresponds to 16d in the build process.

### These routines pull out lists of changed items in the "htitem_htmember_jn table from month
###    to month.  Assumes that a "new" and "old" version of the table exists in the development DB.
###    The generated lists are used by DLPS to determine which entries need to be flagged for
###    re-indexing (since to re-index all takes weeks).  These are lists of volume_ids.

## returns true if lists are different, false of they're the same
def changed_counts(oldlist, newlist)
  olds = {};
  oldlist.each do |a|
    omember, ocount = a.split("-");
    olds[omember] = (ocount.to_i > 0);
  end
  newlist.each do |b|
    nmember, ncount = b.split("-");
    # is the member in the old list
    if olds.has_key?(nmember)
      # does the access_count go from zero to non-zero, or vice versa
      return true unless olds[nmember] == (ncount.to_i > 0);
    else
      # adding a member with access_count > 0
      if ncount.to_i > 0
        return true;
      end
    end
  end
  return false;
end

def generate_volume_change_list(olddb, newdb, log)
  db    = Hathidb::Db.new();
  conn  = db.get_conn();
  count = 0

  log.d("Generating list.");

  # loop through every current volume_id and check it against the old version
  access_count_changes = [];
  gain_loss_changes    = [];

  query1     = "select volume_id from holdings_htitem";
  query2_new = conn.prepare("select member_id, access_count from #{newdb} where volume_id = ?");
  query2_old = conn.prepare("select member_id, access_count from #{olddb} where volume_id = ?");

  conn.enumerate(query1).each_slice(50000) do |slice|
    slice.each do |row|
      count      += 1;
      vid         = row[0];
      new_members = [];
      old_members = [];
      new_pairs   = [];
      old_pairs   = [];

      # New
      query2_new.execute(vid) do |row2|
        mem_id       = row2[0];
        acount       = row2[1];
        new_members << mem_id;
        mem_num_pair = "#{mem_id}-#{acount}";
        new_pairs   << mem_num_pair;
      end

      # Old
      query2_old.execute(vid) do |row2|
        mem_id       = row2[0];
        acount       = row2[1];
        old_members << mem_id;
        mem_num_pair = "#{mem_id}-#{acount}";
        old_pairs   << mem_num_pair;
      end

      gain_loss_changes    << vid unless new_members.sort == old_members.sort;
      access_count_changes << vid if     changed_counts(old_pairs, new_pairs);
    end
    if ((count % 500000) == 0)
      puts "#{count}...";
    end
    log.d(count);
  end

  puts "There are #{gain_loss_changes.length} gain-loss changes.";
  puts "There are #{access_count_changes.length} access_count changes.";
  conn.close;

  # write results
  ymd = Time.new().strftime("%Y%m%d");
  hdf = Hathidata::Data.new("volume_changes.#{ymd}.txt").open('w');

  gl_set = Set.new gain_loss_changes;
  ac_set = Set.new access_count_changes;
  puts "The gain_loss set a subset of the access_count subset.  #{gl_set.subset?(ac_set)}";
  puts "The access_count set a subset of the gain_loss subset.  #{ac_set.subset?(gl_set)}";
  gl_set.merge(ac_set);
  puts "There are #{gl_set.size} elements in the merged set";
  gl_set.each do |vid|
    hdf.file.puts(vid);
  end
  hdf.close;

  return hdf.path;
end

def load_data (path, log)
  db           = Hathidb::Db.new();
  prod_conn    = db.get_prod_conn();
  next_version = 0;
  version_sql  = "SELECT MAX(version) AS max_version FROM holdings_deltas";

  prod_conn.query(version_sql) do |row|
    next_version = row[:max_version].to_i + 1;
  end

  load_sql = %W<
    LOAD DATA LOCAL INFILE ? 
    INTO TABLE holdings_deltas (volume_id) 
    SET 
    version     = ?, 
    update_date = CURRENT_DATE
  >.join(' ');

  if %x(hostname).include?('grog') then
    log.d("Loading version #{next_version} into prod.")
    load_query_prod = prod_conn.prepare(load_sql);
    load_query_prod.execute(path, next_version);
    prod_conn.close();
  end    

  log.d("Loading version #{next_version} into dev.")

  dev_conn = db.get_conn();
  load_query_dev = dev_conn.prepare(load_sql);
  load_query_dev.execute(path, next_version);
  dev_conn.close();

end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  old_table_name = "holdings_htitem_htmember_jn_old";
  new_table_name = "holdings_htitem_htmember_jn";
  path = generate_volume_change_list(old_table_name, new_table_name, log);
  load_data(path, log);
  log.d("Finished");
end
