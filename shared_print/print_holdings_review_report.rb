require 'hathidata';
require 'hathidb';

db         = Hathidb::Db.new();
conn       = db.get_conn();
member_ids = ARGV;
# Give a single member_id, a list of member_ids or the name of a predefined group as input.
ids_str    = member_ids.join('_');
outfn      = "reports/shared_print/print_holdings_review_#{ids_str}_$ymd.tsv";
hdout      = Hathidata::Data.new(outfn).open('w');
group      = false;

# Predefined groups that may be given as input
groups = {
  'foo'     => %w[brynmawr haverford],
  'ivyplus' => %w[brown columbia cornell dartmouth duke harvard jhu mit princeton stanford uchicago upenn yale],
  'big10'   => %w[illinois iu msu northwestern osu psu purdue rutgers uiowa umd umich umn unl wisc],
  'uc'      => %w[berkeley nrlf srlf ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf],
};

# Replace member_ids with a group if ids_str matches one of the groups.
if groups.has_key?(ids_str) then
  member_ids = groups[ids_str];
  group = true;
end

qmarks = (['?'] * member_ids.size).join(',');

# Main query.
report_sql = %W{
  SELECT DISTINCT
  spp.member_id,
  hm.local_id,
  spp.gov_doc,
  spp.item_condition,
  spp.local_oclc,
  spp.oclc,
  hhh.H,
  spp.local_h
  FROM shared_print_pool          AS spp
  JOIN holdings_cluster_oclc      AS hco  ON (spp.oclc = hco.oclc)
  JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
  JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
  JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
  JOIN holdings_memberitem        AS hm   ON (spp.holdings_memberitem_id = hm.id)
  WHERE spp.member_id IN (#{qmarks})
  AND hc.cluster_type = 'spm'
  ORDER BY spp.oclc, spp.member_id
}.join(' ');

# If doing a group report, use this to get group_h
group_h_sql = %W{
  SELECT COUNT(DISTINCT member_id) AS group_h
  FROM shared_print_pool 
  WHERE oclc = ? AND member_id IN (#{qmarks})
}.join(' ');

header = %w{member_id local_id gov_doc item_condition local_oclc oclc h local_h};
legend = [
  "local_oclc : the OCLC number used by #{member_ids.join(',')} in their latest print holdings submission",
  "oclc : resolved OCLC number",
  "h : number of HathiTrust members (including Shared Print members) holding the same OCLC number",
  "local_h : number of Shared Print members (including #{member_ids.join(',')}) holding the same OCLC number",
];

# Tweak header and legend if doing a group report.
if group then
  legend << "group_h : number of members within #{ids_str} holding the same OCLC number";
  header << 'group_h';
end

legend.each do |leg|
  hdout.file.puts("#\t#{leg}");
end

hdout.file.puts(header.join("\t"));

report_q = conn.prepare(report_sql);
group_h_q = conn.prepare(group_h_sql);
memo_oclc = nil;
memo_h    = nil;

report_q.enumerate(*member_ids) do |row|
  vals = row.to_a;
  if group then
    # Poor mans memoization
    if row[:oclc] == memo_oclc then
      vals << memo_h;
    else
      group_h_q.enumerate(row[:oclc], *member_ids) do |h_row|
        vals << h_row[:group_h];
        memo_oclc = row[:oclc];
        memo_h = h_row[:group_h];
      end
    end
  end
  hdout.file.puts(vals.join("\t"));
end

hdout.close();
