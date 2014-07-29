require 'hathilog';
require 'hathidb';
require 'hathidata';

# Does the smartest Hathi overlap query I've seen so far. 
# Really using the tables the way they were meant to be used, I guess.
# Cleverly hidden in /htapps/pulintz.babel/Code/phdb/bin/runner.rb.

log = Hathilog::Log.new();
log.d("Started");

member_id = nil;
if ARGV.size > 0 then
  member_id = ARGV.shift;
else
  raise "member_id is required as 1st element in ARGV.";
end

db   = Hathidb::Db.new();
conn = db.get_conn();

q = %w<
SELECT
    hm.local_id,
    hm.oclc,
    hho.volume_id,
    hh.rights,
    hh.access,
    hh.gov_doc
FROM
    holdings_memberitem AS hm
JOIN
    holdings_htitem_oclc AS hho ON (hho.oclc = hm.oclc)
JOIN
    holdings_htitem AS hh ON (hh.volume_id = hho.volume_id)
WHERE
    hm.member_id = ?
>.join(" ");

log.d(q);
log.d(" -- hm.member_id = '#{member_id}'");
pq = conn.prepare(q);

cols = [
    :local_id,
    :oclc,
    :volume_id,
    :rights,
    :access,
    :gov_doc
];

Hathidata.write("overlap_#{member_id}_$ymd.tsv") do |hdout|
  hdout.file.puts cols.map{|c| c.to_s}.join("\t");
  pq.enumerate(member_id) do |row|
    hdout.file.puts cols.map{|c| row[c]}.join("\t");
  end
end

conn.close();
log.d("Finished")
