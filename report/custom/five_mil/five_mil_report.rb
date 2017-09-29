require 'hathidb';
require 'hathidata';

# Email from Jeremy on 2015-04-07:

=begin

Mike is trying to get together some additional data to announce our 
passing 5 million "open" volumes in HathiTrust. Tim, would you be 
able to quantify the unique titles in the repository? Would this be 
pretty much the same as our current title count, since, if we knew 
they were the same title, we would be putting them on the same record?

Martin or Tim (I'm not sure who would be best), Mike is also looking 
for a breakdown of the 5 million open volumes by single and multi-
part monograph, and serial (it would be helpful to have both title 
and volume-level counts for these). 

By open volumes, we are counting rights attributes:

1, 7, 9-15, 17, 20-25

=end

# This script takes part of the second paragraph.

db = Hathidb::Db.new();
@conn = db.get_conn();

open_rights_attr = %w[
  pd
  ic-world
  pdus
  cc-by
  cc-by-nd-3.0
  cc-by-nd-4.0
  cc-by-nc-nd-3.0
  cc-by-nc-nd-4.0
  cc-by-nc-3.0
  cc-by-nc-4.0
  cc-by-nc-sa-3.0
  cc-by-nc-sa-4.0
  cc-by-sa-3.0
  cc-by-sa-4.0
  cc-zero-3.0
  cc-zero-4.0
];

sql = %W[
  SELECT rights, item_type, COUNT(DISTINCT oclcs) AS titles, COUNT(volume_id) AS items 
  FROM holdings_htitem
  WHERE rights IN (#{open_rights_attr.map{|x| "'#{x}'"}.join(',')})
  GROUP BY
  rights, item_type
  ORDER BY 
  rights, item_type
].join(' ');

tot_titles = 0;
tot_items  = 0;
hdout = Hathidata::Data.new('reports/five_mil_$ymd.tsv').open('w');
cols  = [:rights, :item_type, :titles, :items];
hdout.file.puts cols.join("\t");
@conn.query(sql) do |row|
  hdout.file.puts cols.map{|c| row[c]}.join("\t");
  tot_titles += row[:titles];
  tot_items  += row[:items];
end
hdout.file.puts "TOT\tTOT\t#{tot_titles}\t#{tot_items}";
hdout.close();
