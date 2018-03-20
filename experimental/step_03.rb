require 'hathidata';
require 'hathidb';
require 'hathilog';

def setup
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
end

def run_list_query

end

def run_single_query

end

def create_cluster

end

def merge_clusters

end

def truncate_tables

end

def cluster_main

end

def load_table

end

def get_loadfile_path
  
end

def dump_data_structure

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
