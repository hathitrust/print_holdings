require 'hathidb';
require 'set';

=begin

Copied by mwarin from /htapps/pulintz.babel/Code/phdb/bin/calc_cluster_rights.rb.

Original docstring:
This module implements a rights calculation for clusters.  It just 
utilizes the 'access' column of htitem and assigns either '2' or '1' 
('in-copyright' and public-domain', respectively) based on the 
'allow' or 'deny' indicators for a cluster.  All items must have the
same access indication, otherwise it'll be marked as '0' ('undetermined').

=end

def run_list_query(conn, query, fetch_size=50000)
  # Generic query runner, appropriate for queries that return
  # simple lists. Requires a connection object and a query to run
  items = []
  conn.fetch_size = fetch_size
    
  conn.enumerate(query).each_slice(fetch_size) do |slice|
    slice.each do |row|
      items << row[0]
    end
  end
  return items
end

def calc_cluster_rights()
  # select cluster_ids
  db   = Hathidb::Db.new();
  conn = db.get_conn()
  
  puts "Grabbing cluster ids..."
  query1 = "SELECT cluster_id FROM holdings_cluster"
  all_clusters = run_list_query(conn, query1)
    
  citer = 0
  puts "iterating..."
  all_clusters.each do |cid|
    citer += 1 
    # get the access designation of all volumes in a cluster 
    query2 = "SELECT access FROM holdings_htitem, holdings_cluster_htitem_jn 
              WHERE holdings_cluster_htitem_jn.volume_id = holdings_htitem.volume_id 
              AND cluster_id = #{cid}" 
    accesses = run_list_query(conn, query2)
        
    # calculate access right designation from results 
    accs = Set.new
    accesses.each do |a|
      accs.add(a)
    end
        
    # get appropriate update query
    rid = -1
    if (accs.length == 0)
      # shouldn't happen
      puts "problem: #{cid}, cost rights len = 0" 
      next
    elsif accs.length > 1
      # rights discrepancy, assign 0
      rid = 0
    else
      # consistent rights designations
      if accs.include?('deny')
        rid = 2
      elsif accs.include?('allow')
        rid = 1
      else
        puts "problem rights string = #{rstring} (#{cid})"
      end
    end   
    if rid < 0
      puts "'-1' rights id, shouldn't happen."
      exit
    end   
    query3 = "UPDATE holdings_cluster SET cost_rights_id = #{rid} WHERE cluster_id = #{cid}" 
    
    # execute update
    conn.update(query3)  
    
    puts citer if (citer % 500000) == 0
  end
           
  conn.close()         
  print "finished updating cluster rights."  
end

calc_cluster_rights()
