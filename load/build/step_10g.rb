require 'hathilog';
require 'hathidb';

log  = Hathilog::Log.new();
log.i("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

# So we can measure progress.
def count_unclaimed (conn, log)
  q = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn WHERE member_id = ''";
  log.d(q);
  conn.query(q) do |r|
    log.d(r[:c]);
  end
end

# Check initial count.
log.d("Unclaimed count before update:");
count_unclaimed(conn, log);

# Map of prefix->member_id, reads like:
# volume "uma.ark:/13960/t5hb1q086" goes to "umass".
prefix_member = {
  'keio' => 'ht',
  'osu'  => 'osu',
  'udel' => 'udel',
  'txa'  => 'tamu',
  'uma'  => 'umass',
};

# Do update.
prefix_member.each_pair do |prefix, member_id|
  # Tried this as a prepared statement, but 'tworkn'ted.
  # Set member_id.
  q1 = ["UPDATE holdings_htitem_htmember_jn",
         "SET member_id = '#{member_id}' ",
         "WHERE volume_id LIKE '#{prefix}.%'",
         "AND member_id = ''"
        ].join("\n");
  log.d(q1);
  conn.update(q1);
end

# Check final count.
log.d("Unclaimed count after update:");
count_unclaimed(conn, log);

log.d("Volumes with zero lm count, per member:")

# Now list how many by who.
q2 = %W<
  SELECT
    member_id,
    COUNT(*) AS c
  FROM
    holdings_htitem_htmember_jn
  WHERE
    lm_count IS NULL
  GROUP BY
    member_id
>.join(' ');

log.d(q2);

conn.query(q2) do |row|
  log.d("#{row[:member_id]}\t#{row[:c]}");
end

conn.close();
log.i("Finished");
