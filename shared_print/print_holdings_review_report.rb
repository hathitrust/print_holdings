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

# Predefined groups that may be given as input.
# Note that some groups contain members that aren't (yet?) in SP. They will be ignored.
groups = {
  'foo'     => %w[ucmerced brynmawr],
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
  spp.resolved_oclc,
  MAX(hhh.H) AS overlap_ht,
  spp.local_h AS overlap_sp
  FROM shared_print_pool          AS spp
  JOIN holdings_cluster_oclc      AS hco  ON (spp.resolved_oclc = hco.oclc)
  JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
  JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
  JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
  JOIN holdings_memberitem        AS hm   ON (spp.holdings_memberitem_id = hm.id)
  WHERE spp.member_id IN (#{qmarks})
  AND hc.cluster_type = 'spm'
  GROUP BY
  spp.member_id,
  hm.local_id,
  spp.gov_doc,
  spp.item_condition,
  spp.local_oclc,
  resolved_oclc,
  overlap_sp
  ORDER BY spp.resolved_oclc, spp.member_id
}.join(' ');

# If doing a group report, use this to get overlap_group
overlap_group_sql = %W{
  SELECT COUNT(DISTINCT member_id) AS overlap_group
  FROM shared_print_pool 
  WHERE oclc = ? AND member_id IN (#{qmarks})
}.join(' ');

header = %w{member_id local_id gov_doc item_condition local_oclc resolved_oclc overlap_ht overlap_sp};
legend = [
  "local_oclc : the OCLC number used in the latest print holdings submission",
  "resolved_oclc : resolved OCLC number",
  "overlap_ht : number of HathiTrust members (including Shared Print members) holding the same resolved_oclc number",
  "overlap_sp : number of Shared Print members holding the same resolved_oclc number",
];

# Tweak header and legend if doing a group report.
if group then
  legend << "overlap_group : number of members within #{ids_str} holding the same resolved_oclc number";
  header << 'overlap_group';
end

legend.each do |leg|
  hdout.file.puts("# #{leg}");
end

hdout.file.puts(header.join("\t"));

report_q  = conn.prepare(report_sql);
overlap_group_q = conn.prepare(overlap_group_sql);
memo_oclc = nil;
memo_h    = nil;

report_q.enumerate(*member_ids) do |row|
  vals = row.to_a;
  # Group needs overlap_group
  if group then
    # Poor mans memoization
    if row[:resolved_oclc] == memo_oclc then
      vals << memo_h;
    else
      # Look up overlap_group for oclc if memo failed.
      overlap_group_q.enumerate(row[:resolved_oclc], *member_ids) do |h_row|
        vals << h_row[:overlap_group];
        memo_oclc = row[:resolved_oclc];
        memo_h    = h_row[:overlap_group];
      end
    end
  end
  hdout.file.puts(vals.join("\t"));
end

hdout.close();
