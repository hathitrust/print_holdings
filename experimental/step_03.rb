require 'hathidata'
require 'hathidb'
require 'hathilog'
require 'hathienv'

# first merge_clusters happens at viter=1780134

def setup
  if !Hathienv::Env.require_minimum_ram(6000) then
    # I know, 6000 != 7000, but the JVM and/or JRuby itself eats a bunch.
    # Or the math in Hathienv is wrong.
    raise "Not enough memory to do this. Use -J-Xmx7000m or higher."
  end
  @log = Hathilog::Log.new()
  @log.i("Started")

  db     = Hathidb::Db.new()
  @conn  = db.get_conn()
  @outfn = Hathidata::Data.new('builds/current/cluster_oclc.data_rb').open('w')

  # These get called all the time, so worth preparing.
  @queries = {
    :create_cluster         => prep_q('INSERT INTO holdings_cluster (cost_rights_id, osici, last_mod) VALUES (0, ?, SYSDATE())'),
    :insert_hchj            => prep_q('INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id) VALUES (?, ?)'),
    :get_volid_from_cluster => prep_q('SELECT volume_id FROM holdings_cluster_htitem_jn WHERE cluster_id = ?'),
    :del_hchj               => prep_q('DELETE FROM holdings_cluster_htitem_jn WHERE cluster_id = ?'),
    :del_hc                 => prep_q('DELETE FROM holdings_cluster WHERE cluster_id = ?'),
    :get_oclc_from_volid    => prep_q('SELECT oclc FROM holdings_htitem_oclc WHERE volume_id = ?'),
    :last_id                => prep_q('SELECT LAST_INSERT_ID() AS id')
  }
  
  @cluster_oclc_d = {}
  @oclc_cluster_d = {}
end

def teardown
  @conn.close()
  @outfn.close()
  @log.i("Finished")
end

def prep_q(sql)
  {:sql => sql, :prep => @conn.prepare(sql)}
end

# runs query, returns array of items
def run_list_query(query)
  items = []
  query.each do |row|
    items << row[0]
  end
  return items
end

# creates a new cluster and populates tables appropriately
def create_cluster(ocns, vol_id)
  # insert into cluster, get id
  ocn0 = ocns[0]
  @queries[:create_cluster][:prep].execute(ocn0)
  # get last inserted id
  pkid = nil
  @queries[:last_id][:prep].enumerate() do |row|
    pkid = row[:id]
  end

  # insert OCNs into cluster_oclc tables
  ocns.each do |nocn|
    oc = nocn.to_i
    if @cluster_oclc_d.include? pkid
      @cluster_oclc_d[pkid] << oc
    else
      @cluster_oclc_d[pkid] = [oc]
    end
    if @oclc_cluster_d.key? oc
      puts "this never happens"
    else
      @oclc_cluster_d[oc] = pkid
    end
  end

  @queries[:insert_hchj][:prep].execute(pkid, vol_id)
  return pkid
end

# Merges clusters together.  Uses c1's id, adds
# c2 OCNs and volume_ids to c1, resolves rights, deletes c2 entries from
# tables.
def merge_clusters (cid1, cid2)
  @log.d("Merging cluster id #{cid1}  (ocns #{@cluster_oclc_d[cid1].join(',')})")
  @log.d("... with cluster id #{cid2} (ocns #{@cluster_oclc_d[cid2].join(',')})")
  c1vids = run_list_query(@queries[:get_volid_from_cluster][:prep].enumerate(cid1))
  c2vids = run_list_query(@queries[:get_volid_from_cluster][:prep].enumerate(cid2))

  c2vids.each do |vid|
    unless c1vids.include?(vid) then
      @queries[:insert_hchj][:prep].execute(cid1, vid)
      @log.i(@queries[:insert_hchj][:sql] + " -- #{cid1}, #{vid}")
    end
  end

  # insert c2 OCNs into c1
  c2ocns = @cluster_oclc_d[cid2]
  c2ocns.each do |ocn|
    @cluster_oclc_d[cid1] << ocn
    @oclc_cluster_d[ocn] = cid1
  end
  @cluster_oclc_d[cid1].uniq!
  @cluster_oclc_d.delete(cid2)

  @queries[:del_hchj][:prep].execute(cid2)
  @queries[:del_hc][:prep].execute(cid2)
end

# Get number of records in each table, then truncate
def truncate_tables
  tables = %w[holdings_cluster holdings_cluster_oclc holdings_cluster_htitem_jn]
  tables.each do | t |
    count_q = "SELECT COUNT(*) AS c FROM #{t}"
    count_r = nil
    @conn.query("SELECT COUNT(*) AS c FROM #{t}") do |row|
      count_r = row[:c]
    end
    puts "#{count_q} -- gave #{count_r} rows"
    trunc_q = "TRUNCATE #{t}"
    puts trunc_q
    @conn.execute(trunc_q)
  end
end

def cluster_main
  puts "Grabbing volume_ids..."
  get_all_volids_sql = "SELECT DISTINCT(volume_id) FROM holdings_htitem"

  viter = 0
  @conn.query(get_all_volids_sql) do |row|
    vid = row[:volume_id]
    viter += 1
    next if vid.length < 3

    # get the OCNs for each volume_id  
    ocns = run_list_query(@queries[:get_oclc_from_volid][:prep].enumerate(vid))
    # skip htitems with no oclc number
    next unless ocns.any?
    pclusters = clusters_with_ocns(ocns)
    # if yes, aggregate

    # If the ocn(s) is/aren't in a cluster, make a new cluster.
    if pclusters.size == 0 then
      create_cluster(ocns, vid)
    else
      # If any of the ocns already are in a cluster, merge the clusters.
      cids = pclusters
      cids.sort!
      lowest_cid = cids.pop
      @queries[:insert_hchj][:prep].execute(lowest_cid, vid)

      # add all OCNs to lowest matching cluster number
      ocns.each do |ocn|
        @oclc_cluster_d[ocn] = lowest_cid
        @cluster_oclc_d[lowest_cid] << ocn
      end
      @cluster_oclc_d[lowest_cid].uniq!
      # merge remaining clusters into root cluster
      cids.each do |cid|
        merge_clusters(lowest_cid, cid.to_i)
      end
    end

    if viter % 10000 == 0
      @log.i viter
    end
  end

  @log.i "viter: #{viter}"
  puts "dumping final data structure"
  dump_data_structure()
end

# are any OCNs already participating in other clusters?
def clusters_with_ocns (ocns)
  pclusters = []
  ocns.each do |ocn|
    if @oclc_cluster_d.key?(ocn)
      cid = @oclc_cluster_d[ocn]
      pclusters << cid
    end
  end
  return pclusters.uniq  
end

def load_table
  q = "LOAD DATA LOCAL INFILE '#{@outfn.path}' INTO TABLE holdings_cluster_oclc"
  @log.i(q)
  @conn.execute(q)
end

# Exports one of the table data structures to a flatfile.  Structs are
# hashes of lists (sets). """
def dump_data_structure
  @cluster_oclc_d.each do |cid, oclcs|
    oclcs.each do |o|
      @outfn.file.puts "#{cid}\t#{o}"
    end
  end
end

if $0 == __FILE__ then
  setup()
  # Start with blank slate.
  truncate_tables()
  # Calculate clusters and write to file.
  cluster_main()
  # Load file into holdings_cluster_oclc.
  load_table()
  teardown()
end
