require 'hathidata';
require 'hathidb';

# Take a file with oclc numbers. Look each up and report its H.
# Remove 1 from H if it matches the member being estimated.
# So kind of like reestimate, but more "how much more would
# I have to pay if I also submitted these?".

# Call thusly:
# ruby oclc_h.rb <oclc_file> <member_id> <avg_cost_per_vol>

hdin = Hathidata::Data.new(ARGV.shift).open('r');
member_id = ARGV.shift;
cpv = ARGV.shift.to_f; # avg cost per volume, read as a float.

hdout = Hathidata::Data.new("reestimate/oclc_h_#{member_id}.tsv").open('w');
db    = Hathidb::Db.new();
conn  = db.get_conn();

# Query once per line
sql = %W{
  SELECT COUNT(hchj.member_id) AS h
  FROM  holdings_cluster_oclc AS hco
  JOIN  holdings_cluster_htmember_jn AS hchj
  ON    (hco.cluster_id = hchj.cluster_id)
  WHERE hco.oclc = ?
  AND   hchj.member_id != '#{member_id}'
}.join(' ');

q = conn.prepare(sql);

h_hash = {};

# Output each oclc & h pair as a line to one file.
hdin.file.each_line do |line|
  line.strip!;
  if line =~ /^(\d+)$/ then
    oclc = $1;
    q.enumerate(oclc) do |row|
      h = row[:h].to_i;
      hdout.file.puts("#{oclc}\t#{h}");
      h_hash[h] ||= 0;
      h_hash[h] += 1;
    end
  end
end
hdout.close();
hdin.close();

# Output summary of h, count & cost to another file.
hdout_summary = Hathidata::Data.new("reestimate/oclc_h_#{member_id}_summary.tsv").open('w');
hdout_summary.file.puts("H\tCount\t$Cost");
h_hash.keys.sort.each do |h|
  cost = (h_hash[h] / (h + 1).to_f) * cpv;
  hdout_summary.file.puts("#{h}\t#{h_hash[h]}\t#{cost}");
end
hdout_summary.close();
