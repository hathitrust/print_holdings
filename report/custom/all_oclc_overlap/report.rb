require 'hathidb';
require 'hathidata';

# Take a member_id and get a count on each oclc number how many other members hold it.
# Pass all oclc numbers through oclc_resolution.

member_id = ARGV.shift;
db        = Hathidb::Db.new();
conn      = db.get_conn();
hdout     = Hathidata::Data.new("reports/all_oclc_overlap_count_#{member_id}_$ymd.tsv").open('w');

# First normalize all oclcs of the member.
get_member_oclc_sql = %w[
    SELECT DISTINCT COALESCE(hm.oclc, ocr.oclc_x) AS oclc_resolved
    FROM holdings_memberitem  AS hm
    LEFT JOIN oclc_resolution AS ocr
    ON (hm.oclc = ocr.oclc_y)
    WHERE hm.member_id = ?
    ORDER BY oclc_resolved
].join(' ');
get_member_oclc_q = conn.prepare(get_member_oclc_sql);

# Find all alternate forms of an oclc number.
get_alt_oclcs_sql = %w[
    SELECT oclc_y
    FROM oclc_resolution
    WHERE oclc_x = ?
].join(' ');
get_alt_oclcs_q = conn.prepare(get_alt_oclcs_sql);

# Get all distinct member_ids holding an oclc number.
count_members_sql = %w[
    SELECT DISTINCT member_id
    FROM holdings_memberitem
    WHERE oclc = ?
].join(' ');
count_members_q = conn.prepare(count_members_sql);

hdout.file.puts(['oclc_res', 'cluster', 'h'].join("\t"));

# Loop over the normalized oclc numbers of member_id.
get_member_oclc_q.enumerate(member_id) do |row_o|
  oclc_res     = row_o['oclc_resolved'];
  members      = [];
  h            = 0;
  lookup_oclcs = [oclc_res];

  # Get the alternate forms of that oclc number
  get_alt_oclcs_q.enumerate(oclc_res) do |row_alt|
    lookup_oclcs << row_alt['oclc_y'];
  end
  lookup_oclcs = lookup_oclcs.sort.uniq;

  # Look up all members holding all forms of the oclc number
  lookup_oclcs.each do |lookup_oclc|
    count_members_q.enumerate(lookup_oclc) do |row_member|
      members << row_member['member_id'];
    end
  end

  # Get a unique count of all members holding all forms of the oclc number.
  h = members.uniq.size;
  cluster = lookup_oclcs.join(';')
  hdout.file.puts([oclc_res, cluster, h].join("\t"));
end

hdout.close();
conn.close();
