require 'hathidb';
require 'hathilog';

=begin

Count the number of HathiTrust monographs that are held in print by
at least one CIC member institution library.

Break the count down by the number of CIC member institution libraries
that hold the items (There are fifteen CIC member institutions, so
that means fifteen counts -- how many monographs are held in print
by one CIC member, how many held by two CIC members, etc.)

Break those fifteen counts down by rights status (open/closed), so
now there are thirty counts.

For the entire set of items represented by each count, calculate the
average number of HathiTrust partners that hold them in print.

The result would be something on the lines of this (with a total of
30 rows, or sets of counts):

=end

$db = Hathidb::Db.new();
$log = Hathilog::Log.new();
$cic_members = %w[chi ind iowa minn msu nwu osu psu purd uiuc umd unl uom wisc];
# $cic_members = ['purd', 'chi'];

q1 = %Q{
SELECT
    h2.volume_id,
    h3.member_id, 
    SUM(h3.copy_count) AS cc, 
    SUM(h3.access_count) AS ac
FROM
    holdings_htitem AS h2,
    holdings_htitem_htmember_jn AS h3
WHERE
    h2.volume_id = h3.volume_id
    AND
    h3.member_id IN (#{$cic_members.map{ |m| "'#{m}'" }.join(',')})
    AND
    h2.item_type = 'mono'
GROUP BY
    h2.volume_id,
    h3.member_id
};
puts q1;

# Use -q flag if you just want to see the query, not run it.
if ARGV.index('-q') != nil then
  abort;
end

conn = $db.get_conn();
outf = File.open('cic_1_raw.tsv', 'w');
$log.d("Starting query");
conn.query(q1).each_slice(1000) do |slice|
  slice.each do |row|
    outf.puts [row[:volume_id], row[:member_id], row[:cc], row[:ac]].join("\t");
  end
  $log.d("Slice!");
  outf.flush();
end
outf.close();
conn.close();
