require 'hathidata';
require 'hathidb';
require 'hathilog';

def setup
  @log = Hathilog::Log.new();
  @log.i("Started");

  db     = Hathidb::Db.new();
  @conn  = db.get_conn();
  @outfn = Hathidata::Data.new('builds/current/cluster_oclc.data_rb').open('w')

  @volid_cluster_d = {};
  @cluster_volid_d = {};
  @cluster_oclc_d  = {};
  @oclc_cluster_d  = {};
end

def teardown
  @conn.close();
  @outfn.close();
  @log.i("Finished");
end

def run_list_query(query)
  items = []
  @conn.enumerate(query) do |row|
    items << row[0]
  end
  return items
end

# Run a generic query, return first row
def run_single_query(query)
  @conn.enumerate(query) do |row|
    return row[0]
  end
end

# creates a new cluster and populates tables appropriately
def create_cluster(ocns, vol_id)
  
  # insert into cluster, get id
  ocn0 = ocns[0]
  query2 = "INSERT INTO holdings_cluster (cost_rights_id, osici, last_mod) \
            VALUES (0, '#{ocn0}', SYSDATE())"
  @conn.execute(query2)
  pkid = run_single_query("SELECT LAST_INSERT_ID()") 
 
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

  # insert volume_id into cluster_htitem_jn
  query4 = "INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id) \
            VALUES ('#{pkid}', '#{vol_id}')"
  @conn.execute(query4)
  return pkid
end

def merge_clusters

end

# Get number of records in each table, then truncate
def truncate_tables
  tables = ['holdings_cluster', 'holdings_cluster_oclc', 'holdings_cluster_htitem_jn']
  tables.each do | t | 
    count_q = "SELECT COUNT(*) AS c FROM #{t}"
    count_r = run_single_query(count_q)
    puts "#{count_q} -- gave #{count_r} rows"
    trunc_q = "TRUNCATE #{t}" 
    puts trunc_q
    @conn.execute(trunc_q)
  end
end

def cluster_main
  puts "Grabbing volume_ids..."
  query1 = "SELECT DISTINCT(volume_id) FROM holdings_htitem LIMIT 0,5000"
  all_vids = run_list_query(query1)
  puts "#{all_vids.size} ids received..."
  
  viter = 0
  all_vids.each do |vid|
    viter += 1
    next if vid.length < 3

    ## get the OCNs for each volume_id ##
    query3 = "SELECT oclc FROM holdings_htitem_oclc WHERE volume_id = '#{vid}'"
    ocns = run_list_query(query3)
    # skip htitems with no oclc number
    next unless ocns.any?

    # are any OCNs already participating in other clusters? #
    pclusters = []
    ocns.each do |ocn|
      if @oclc_cluster_d.key?(ocn)
        cid = @oclc_cluster_d[ocn]
        pclusters << cid
      end
    end
    pclusters.uniq!

    # if yes, aggregate
    if pclusters.size > 0
      cids = pclusters
      cids.sort!
      # todo: test this
      lowest_cid = cids.pop
      query4 =  "INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id) \ 
                  VALUES (#{lowest_cid}, '#{vid}')" 
      puts query4
      @conn.execute(query4)

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
    else
      create_cluster(ocns, vid)
    end

    if viter % 1000 == 0
      @log.i viter
    end      
 
  end 

  @log.i "viter: #{viter}" 
  puts "dumping final data structure"
  dump_data_structure()
 
end

def load_table

end

def get_loadfile_path
  
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
  setup();
  # Start with blank slate.
  truncate_tables()
  # Calculate clusters and write to file.
  cluster_main()
  # Load file into holdings_cluster_oclc.
  load_table()                             
  teardown();
end
