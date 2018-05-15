require 'hathidb';

lookup_type = ARGV.shift;
lookup_id   = ARGV.shift;

if lookup_type !~ /^(cluster_id|volume_id)$/ then
  raise "lookup_type must be cluster_id or volume_id";
end

@cluster_id = lookup_type == 'cluster_id' ? lookup_id : nil;
@volume_id  = lookup_type == 'volume_id'  ? lookup_id : nil;

db    = Hathidb::Db.new();
@conn = db.get_conn();

def get_cluster_id
  if @cluster_id.nil? then
    sql = "SELECT cluster_id FROM holdings_cluster_htitem_jn WHERE volume_id = ?";
    q   = @conn.prepare(sql);
    q.enumerate(@volume_id) do |row|
      @cluster_id = row[:cluster_id];
    end
  end
  puts "Cluster_id: #{@cluster_id}";
end

def get_cluster
  sql = "SELECT * FROM holdings_cluster WHERE cluster_id = ?";
  q   = @conn.prepare(sql);
  q.enumerate(@cluster_id) do |row|
    puts row.to_h;
  end
end

def get_volume_ids
  puts "## Volume ids (+ HathiFile data):";
  cols = %w[volume_id access rights oclcs item_type];
  sql = %W<
    SELECT #{cols.map{|x| 'hi.' + x}.join(', ')}
    FROM holdings_cluster_htitem_jn AS hc
    JOIN holdings_htitem AS hi ON (hc.volume_id = hi.volume_id)
    WHERE cluster_id = ?
    ORDER BY volume_id
  >.join(' ');
  q = @conn.prepare(sql);
  puts cols.join("\t");
  q.enumerate(@cluster_id) do |row|
    puts cols.map{|x| row[x]}.join("\t");
  end
end

def get_oclcs
  puts "## OCLCs:";
  sql = "SELECT oclc FROM holdings_cluster_oclc WHERE cluster_id = ? ORDER BY oclc";
  q   = @conn.prepare(sql);
  q.enumerate(@cluster_id) do |row|
    puts row[:oclc];
  end
end

def get_members
  puts "## Member ids:";
  sql = "SELECT member_id FROM holdings_cluster_htmember_jn WHERE cluster_id = ? ORDER BY member_id";
  q   = @conn.prepare(sql);
  q.enumerate(@cluster_id) do |row|
    puts row[:member_id];
  end
end

def main
  get_cluster_id();
  get_cluster();
  get_volume_ids();
  get_oclcs();
  get_members();
end

if __FILE__ == $0 then
  main();
end
