require 'hathidb';
require 'hathilog';
require 'hathidata';
require 'cost_estimator';

=begin

Following the steps in:

https://docs.google.com/a/umich.edu/document/d/1R5GdnvykPz1O_2wo_d-61BVjkanAgAJyhJpaEBBzogA/edit#

... but as one giant script instead of a dozen manual steps and scripts.

=end

$HT_DIR = '/htapps/mwarin.babel/phdb_scripts/data/memberdata/';
$log    = Hathilog::Log.new();

def create_estimate(member_id, ave_ic_cost_per_vol, db)

  puts "#####\n# #{member_id}\n#####";
  table = "holdings_memberitem_#{member_id}";
  iconn = db.get_interactive();
  create_table(table, iconn);
  load_table(table, member_id, iconn);
  volume_id_file = get_volume_ids(table, iconn);

  narrative = get_narrative(table, iconn);
  hd = Hathidata::Data.new("estimate/narrative_#{member_id}").open('w');
  hd.file.puts narrative;
  hd.close();

  run_ic_estimate(table, iconn, volume_id_file, ave_ic_cost_per_vol);

  drop_table(table, iconn);
  iconn.close();
end

def create_table(table, conn)
  [
   "DROP TABLE IF EXISTS #{table}",
   "CREATE TABLE #{table} LIKE holdings_memberitem",
  ].each do |q|
    $log.d(q);
    conn.execute(q);
  end
end

def load_table (table, member_id, conn)
  member_dir = $HT_DIR + member_id + '.estimate';
  Dir.new(member_dir).each do |p|
    if p =~ /HT003_#{member_id}\.(mono|multi|serial)\.tsv/ then

      q = %W<
        LOAD DATA LOCAL INFILE '#{member_dir}/#{p}'
        INTO TABLE #{table}
        IGNORE 1 LINES
        (oclc, local_id, member_id, status, item_condition,
        process_date, enum_chron, item_type, issn, n_enum, n_chron);
      >.join(' ');
      $log.d(p);
      $log.d(q);

      conn.update(q);
      sleep 0.5;
    end
  end
end

def pull_query(conn, query, filen)
  # outfile for table
  outf = File.open(filen, "w");
  puts "Running '#{query}'...";

  conn.enumerate(query) do |row|
    outf.puts row.join("\t");
  end

  outf.close;
end

def get_volume_ids(table, conn)
  # Airlifted in from:
  # /htapps/pete.babel/Code/phdb/bin/estimate_pull_ic_volumes.rb
  outfn   = "#{table}-volume_id.$ymd.out";
  outfile = Hathidata::Data.new(outfn);

  if File.exists?(outfile.path) then
    return outfile.path;
  end

  ### query to supplement the cost estimation ###
  query = "SELECT DISTINCT ho.volume_id FROM holdings_htitem_oclc as ho,
         holdings_htitem as h, #{table} as mt
         WHERE ho.oclc = mt.oclc
         AND h.volume_id = ho.volume_id
         AND h.access = 'deny'";

  pull_query(conn, query, outfile.path);

  $log.d("Output written to file #{outfile.path}");

  return outfile.path;
end

def run_ic_estimate(table, conn, volume_id_file, ave_cost_per_vol)
  # Airlifted in from:
  # /htapps/pete.babel/Code/phdb/bin/estimate_ic_costs_for_member.rb
  estimator = CostEstimator.new(table, conn);
  cost = estimator.estimate_cost(ave_cost_per_vol, 1, volume_id_file);
  cost_str = "%.2f" % cost;
  puts "total ic cost = $#{cost_str}";
end

# Turn 1000000000000 into 1,000,000,000,000
def separate_thousands (n)
  n.to_s.reverse.gsub(/\d{3}(?=\d)/,'\&,').reverse;
end

# Creates a little story about the data that was loaded.
def get_narrative(table, conn)

  sql1 = "select count(*) as c from #{table}";
  sql2 = "select count(*) as c, item_type from #{table} group by item_type";
  sql3 = "select count(distinct oclc) as c from #{table}";
  sql4 = "select count(distinct hho.oclc) as c from holdings_htitem_oclc as hho, #{table} as hmu where hho.oclc = hmu.oclc";
  sql5 = "select count(distinct hho.volume_id) as c from holdings_htitem_oclc as hho, #{table} as hmu where hho.oclc = hmu.oclc";

  sql6 = %W{
    SELECT SUM(allow) AS allow, SUM(deny) AS deny FROM (
    SELECT COUNT(DISTINCT ho1.volume_id) AS allow, 0 AS deny
    FROM  #{table} AS m1
    INNER JOIN holdings_htitem_oclc AS ho1 ON (m1.oclc = ho1.oclc)
    INNER JOIN holdings_htitem      AS h1  ON (ho1.volume_id = h1.volume_id)
    WHERE h1.access = 'allow'

    UNION

    SELECT 0 AS allow, COUNT(DISTINCT ho2.volume_id) AS deny
    FROM  #{table} AS m2
    INNER JOIN holdings_htitem_oclc AS ho2 ON (m2.oclc = ho2.oclc)
    INNER JOIN holdings_htitem      AS h2  ON (ho2.volume_id = h2.volume_id)
    WHERE h2.access = 'deny'
    ) AS x
    }.join(" ");

  uniq_oclc = 0;
  message   = [];

  conn.query(sql1) do |row1|
    message << "In all, we received #{separate_thousands(row1['c'])} usable holdings entries";
  end

  item_type_dict = {
    'mono'   => 'single-part monograph',
    'multi'  => 'multi-part monograph',
    'serial' => 'serial'
  };

  line = []
  conn.query(sql2) do |row2|
    line << "#{separate_thousands(row2['c'])} #{item_type_dict[row2['item_type']]}s";
  end
  message << "(#{line.join(', ')})";

  conn.query(sql3) do |row3|
    uniq_oclc = row3['c'].to_i;
    message << "containing #{separate_thousands(row3['c'])} distinct OCLC numbers.";
  end

  conn.query(sql4) do |row4|
    perc = ((row4['c'] / uniq_oclc.to_f) * 100).round(2);
    message << "Of those distinct OCLC numbers, #{separate_thousands(row4['c'])} (#{perc}%) match HathiTrust,";
  end

  conn.query(sql5) do |row5|
    message << "corresponding to #{separate_thousands(row5['c'])} HathiTrust items.";
  end

  conn.query(sql6) do |row6|
    allow   = row6['allow'].to_i;
    deny    = row6['deny'].to_i;
    whole   = (allow + deny).to_f;
    allow_p = ((allow / whole) * 100).round(2);
    deny_p  = ((deny / whole)  * 100).round(2);
    message << "Of those, #{separate_thousands(allow)} (#{allow_p}%) are in the public domain, #{separate_thousands(deny)} (#{deny_p}%) are in copyright.";
  end

  return message;
end

def drop_table(table, conn)
  q = "DROP TABLE #{table}";
  $log.d(q);
  conn.execute(q);
end

# # MAIN # #

# Take a number and some member_ids as input.
# Allows you to estimate several new members in sequence,
# assuming they have their scrubbed HT03 files in $HT_DIR.

if $0 == __FILE__ then
  ave_ic_cost_per_vol = nil;

  usage = "ruby #{__FILE__} <AVE_IC_COST_PER_VOL> <member_id(s)>";

  if ARGV.length >= 2 then
    ave_ic_cost_per_vol = ARGV.shift;
    ave_ic_cost_per_vol = ave_ic_cost_per_vol.to_f;
  end

  if ave_ic_cost_per_vol == nil then
    puts usage;
    abort;
  end

  db = Hathidb::Db.new();
  ARGV.each do |member_id|
    create_estimate(member_id, ave_ic_cost_per_vol, db);
  end
end
