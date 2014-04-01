require 'hathidb';
require 'hathidata';
require 'hathilog';
require 'hathienv';
require 'set';

# Copied from /htapps/pete.babel/Code/phdb/bin/generate_updated_items_list.rb
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
    olds[omember]   = (ocount.to_i > 0);
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
  hdf = Hathidata::Data.new("volume_changes.$ymd.txt");
  if hdf.exists? then
    return hdf.path;
  end

  db    = Hathidb::Db.new();
  conn  = db.get_conn();
  count = 0

  log.d("Generating list.");

  # loop through every current volume_id and check it against the old version
  access_count_changes = [];
  gain_loss_changes    = [];

  query1     = "SELECT volume_id FROM holdings_htitem";
  query2_new = conn.prepare("SELECT member_id, access_count FROM #{newdb} WHERE volume_id = ?");
  query2_old = conn.prepare("SELECT member_id, access_count FROM #{olddb} WHERE volume_id = ?");

  conn.enumerate(query1).each_slice(50000) do |slice|
    slice.each do |row|
      count      += 1;
      vid         = row[0];
      new_members = [];
      old_members = [];
      new_pairs   = [];
      old_pairs   = [];

      # New
      query2_new.enumerate(vid) do |row2|
        mem_id       = row2[0];
        acount       = row2[1];
        new_members << mem_id;
        new_pairs   << "#{mem_id}-#{acount}";
      end

      # Old
      query2_old.enumerate(vid) do |row2|
        mem_id       = row2[0];
        acount       = row2[1];
        old_members << mem_id;
        old_pairs   << "#{mem_id}-#{acount}";
      end

      gain_loss_changes    << vid unless new_members.sort == old_members.sort;
      access_count_changes << vid if     changed_counts(old_pairs, new_pairs);
    end
    log.d("After #{count} volume_ids, #{gain_loss_changes.length} gain/loss changes and #{access_count_changes.length} access count changes");
  end

  puts "There are #{gain_loss_changes.length} gain-loss changes.";
  puts "There are #{access_count_changes.length} access_count changes.";
  conn.close;

  # write results
  hdf.open('w');

  gl_set = Set.new gain_loss_changes;
  gain_loss_changes = nil; # Hopefully freeing some memory.
  ac_set = Set.new access_count_changes;
  access_count_changes = nil; # Hopefully freeing some memory.

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
  next_version = 0;
  version_sql  = "SELECT MAX(version) AS max_version FROM holdings_deltas";

  load_sql = %W<
    LOAD DATA LOCAL INFILE ? 
    INTO TABLE holdings_deltas (volume_id) 
    SET 
    version     = ?, 
    update_date = CURRENT_DATE
  >.join(' ');

  conns = [db.get_conn()];

  # If run in dev, only do dev.
  # If run in prod, run dev THEN prod.
  if Hathienv::Env.is_prod?() then
    log.d("Running twice, one for dev and one for prod!");
    conns << db.get_prod_conn();
  end

  conns.each do |conn|
    conn.query(version_sql) do |row|
      next_version = row[:max_version].to_i + 1;
    end

    log.d("Loading version #{next_version}.")
    load_query = conn.prepare(load_sql);
    load_query.execute(path, next_version);

    conn.close();
  end
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");

  ntn  = "holdings_htitem_htmember_jn";
  otn  = "#{ntn}_old";
  path = generate_volume_change_list(otn, ntn, log);

  load_data(path, log);
  log.d("Finished");
end
