require 'hathilog';
require 'hathidb';

log  = Hathilog::Log.new();
log.i("Started");

db   = Hathidb::Db.new();
conn = db.get_conn();

# So we can measure progress.
def count_before_after (conn, log, before_after = :between)
  q = "SELECT COUNT(*) AS c FROM holdings_htitem_htmember_jn WHERE member_id = ''";
  log.d(q);
  conn.query(q) do |r|
    log.d(r[:c]);
    if before_after == :before && r[:c].to_i == 0 then
      log.d("There is no need to do anything further in this step.");
      exit 0;
    end
  end

  # List how many by who.
  count_by_member_before_after = %w<
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

  log.d("Volumes with zero lm count, per member:")
  log.d(count_by_member_before_after);
  conn.query(count_by_member_before_after) do |row|
    log.d("#{row[:member_id]}\t#{row[:c]}");
  end
end

# Check initial count.
log.d("Unclaimed count before update:");
count_before_after(conn, log, :before);

# Map of prefix->member_id, reads like:
# volume "uma.ark:/13960/t5hb1q086" goes to "umass".
# 2016-08-26 #mwarin: There are enough of these now that I think it should be moved to a table.
prefix_member = {
  'aeu'  => 'ualberta',
  'aub'  => 'aub',
  'gri'  => 'getty',
  'hva'  => 'vcu',     # HTS-127466
  'hvd'  => 'harvard', # HTS-121664
  'ia'   => 'wfu',
  'iau'  => 'uiowa',
  'innd' => 'nd',
  'keio' => 'hathitrust',
  'mcg'  => 'mcgill',
  'osu'  => 'osu',
  'penn' => 'upenn',
  'pul1' => 'princeton',
  'txa'  => 'tamu',
  'txu'  => 'utexas',
  'uc1'  => 'ucla', # changed back to ucla
  'ucbk' => 'berkeley',
  'ucmr' => 'ucmerced',
  'ucw'  => 'uconn',
  'udel' => 'udel',
  'uma'  => 'umass',
  'umd'  => 'umd',
  'uml'  => 'miami',
  'umn'  => 'umn',  
  'uq'   => 'uq',
  'uiul' => 'uiuc',
};

# Do update.
sel_sql = %w[
    SELECT
        volume_id
    FROM
        holdings_htitem_htmember_jn
    WHERE
        member_id = '' 
        AND 
        volume_id LIKE CONCAT(?,'.%') 
        AND 
        volume_id NOT IN (
            SELECT 
                volume_id 
            FROM 
                holdings_htitem_htmember_jn 
            WHERE 
                member_id = ? 
                AND 
                volume_id LIKE CONCAT(?,'.%')
        )
].join(' ');
sel_q = conn.prepare(sel_sql);

upd_sql = "UPDATE holdings_htitem_htmember_jn SET member_id = ? WHERE volume_id = ? AND member_id = ''";
upd_q = conn.prepare(upd_sql);

prefix_member.each_pair do |prefix, member_id|
  sel_q.query(prefix, member_id, prefix) do |row|
    volume_id = row[:volume_id];
    log.d("#{volume_id} --> #{member_id}");
    upd_q.execute(member_id, volume_id);
  end
end

# Check final count.
log.d("Unclaimed count after update:");
count_before_after(conn, log, :after);

conn.close();
log.i("Finished");
