# coding: utf-8
# Report based on https://wush.net/jira/hathitrust/browse/HTP-1082

require 'hathidata';
require 'hathidb';
require 'hathilog';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

get_sp_members_sql = "SELECT DISTINCT member_id FROM shared_print_pool ORDER BY member_id";

member_ids = [];
conn.query(get_sp_members_sql) do |row|
  member_ids << row[:member_id];
end

hdout = Hathidata::Data.new("report_1082_$ymd.tsv").open('w');

queries = {
  "Summary Totals:" =>
  "SELECT SYSDATE() AS date_generated",

  "Total commitments: total number of occurrences of OCN + SP member_id" =>
  "SELECT COUNT(*) AS c FROM shared_print_commitments WHERE member_id = ?",

  "Distinct OCNs: total number of distinct OCNs represented among these committed records (unduplicated count of OCNs )" =>
  "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_commitments WHERE member_id = ?",

  "Unique OCNs in HT: total number of these committed Distinct OCNs that are held by only one HT member_id" =>
  %w{
    SELECT COUNT(*) AS c FROM (
    SELECT spc.resolved_oclc, MAX(hhh.H) AS overlap_ht
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    WHERE spc.member_id = ?
    GROUP BY spc.resolved_oclc
    HAVING overlap_ht = 1
    ) AS x
  }.join(' '),

  "Rare OCNs in SP: number of these Distinct OCNs committed by <=5 SP member_ids" =>
  %w{
    SELECT x.inner_c AS overlap, COUNT(x.inner_c) AS c FROM (
    SELECT resolved_oclc, COUNT(DISTINCT member_id) AS inner_c FROM shared_print_commitments GROUP BY resolved_oclc HAVING inner_c <= 5
    ) AS x GROUP BY overlap
  }.join(' '),

  "Rare OCNs in HT: number of these committed Distinct OCNs held by <= 5 HT member_ids" =>
  %w{
    SELECT x.overlap_ht, COUNT(x.overlap_ht) AS c FROM (
    SELECT spc.resolved_oclc, MAX(hhh.H) AS overlap_ht
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    GROUP BY spc.resolved_oclc
    HAVING overlap_ht <= 5
    ) AS x GROUP BY x.overlap_ht
  }.join(' '),

  "Common OCNs in SP: number of these Distinct OCNs committed by >= 10 SP member_ids" =>
  %w{
    SELECT x.inner_c AS overlap, COUNT(x.inner_c) AS c FROM (SELECT resolved_oclc, COUNT(DISTINCT member_id) AS inner_c
    FROM shared_print_commitments GROUP BY resolved_oclc having inner_c >= 10 order by inner_c asc ) AS x group by inner_c
  }.join(' '),

  "Common OCNs in HT: number of these committed Distinct OCNs held by >= 20 HT member_ids" =>
  %w{
    SELECT overlap_ht AS overlap, COUNT(overlap_ht) AS c FROM (
    SELECT spc.resolved_oclc, MAX(hhh.H) AS overlap_ht
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    GROUP BY spc.resolved_oclc
    HAVING overlap_ht >= 20
    ) AS x GROUP BY overlap ORDER BY overlap
  }.join(' '),

  "HathiTrust Member: member_id" => "",
  "Total SPM" =>
  "SELECT COUNT(*) AS c FROM holdings_memberitem WHERE member_id = ? AND item_type = 'mono'",

  "Matching SPM" =>
  "SELECT COUNT(*) AS c FROM shared_print_pool WHERE member_id = ?",

  "Total Committed" =>
  "SELECT COUNT(*) AS c FROM shared_print_commitments WHERE member_id = ?",

  "Total count PD (allow) / IC (deny) per member" =>
  %w{
    SELECT
    spc.member_id, hh.access, COUNT(hh.access) AS c
    FROM shared_print_commitments   AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hco.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem            AS hh   ON (hchj.volume_id  = hh.volume_id)
    GROUP BY spc.member_id, hh.access
    ORDER BY spc.member_id, hh.access
  }.join(' '),

  "In Storage Facility: local_shelving_type = SFCA or SFCAHM or SFCAASRS" =>
  "SELECT COUNT(*) AS c FROM shared_print_commitments WHERE member_id = ? AND local_shelving_type IN ('sfca','sfcahm','sfcaasrs')",

  "Gov Docs: gov_doc=1" =>
  %w{
    SELECT COUNT(spc.resolved_oclc) AS c
    FROM shared_print_commitments AS spc
    JOIN shared_print_pool AS spp
    ON (spc.resolved_oclc = spp.resolved_oclc AND spc.member_id = spp.member_id)
    WHERE spc.member_id = ? AND spp.gov_doc = '1'
  }.join(' '),

  "Item Condition: item_condition = BRT" =>
  %w{
    SELECT COUNT(spc.resolved_oclc) AS c
    FROM shared_print_commitments AS spc
    JOIN shared_print_pool AS spp
    ON (spc.resolved_oclc = spp.resolved_oclc AND spc.member_id = spp.member_id)
    WHERE spc.member_id = ? AND spp.item_condition = 'BRT'
  }.join(' '),

  "Committed to Other SP: total OCNs committed by this member_id to this other_sp_program" =>
  %w{
    SELECT spc.member_id, spo.sp_program, COUNT(spo.sp_program) AS c
    FROM shared_print_other AS spo
    JOIN shared_print_commitments AS spc
    ON (spo.id = spc.other_commitment_id)
    GROUP BY spc.member_id, spo.sp_program
    ORDER BY spc.member_id, spo.sp_program
  }.join(' '),

  "Unique OCNs SP: total number of this library\'s committed OCNs held by only one SP member_id" =>
  %w{
    SELECT COUNT(*) AS c FROM (
    SELECT t1.resolved_oclc, COUNT(DISTINCT t2.member_id) AS inner_c
    FROM shared_print_commitments AS t1 JOIN shared_print_commitments AS t2 ON (t1.resolved_oclc = t2.resolved_oclc)
    WHERE t1.member_id = ? GROUP BY t1.resolved_oclc HAVING inner_c = 1 ) AS x
  }.join(' '),

  "Rare OCNs SP: total number of this library\'s committed OCNs held by <= 5 SP member_ids" =>
  %w{
    SELECT x.member_id, x.overlap, COUNT(x.overlap) AS c FROM (
      SELECT sp1.member_id, sp1.resolved_oclc, COUNT(DISTINCT sp2.member_id) AS overlap
      FROM shared_print_commitments AS sp1 JOIN shared_print_commitments AS sp2
      ON (sp1.resolved_oclc = sp2.resolved_oclc)
      GROUP BY sp1.member_id, sp1.resolved_oclc HAVING overlap <= 5
    ) AS x
    GROUP BY x.member_id, x.overlap
    ORDER BY x.member_id, x.overlap
  }.join(' '),

  "Common OCNs SP: total number of this library\'s committed OCNs held by >= 15 SP member_ids" =>
  %w{
    SELECT x.member_id, x.overlap, COUNT(x.overlap) AS c FROM (
      SELECT sp1.member_id, sp1.resolved_oclc, COUNT(DISTINCT sp2.member_id) AS overlap
      FROM shared_print_commitments AS sp1 JOIN shared_print_commitments AS sp2
      ON (sp1.resolved_oclc = sp2.resolved_oclc)
      GROUP BY sp1.member_id, sp1.resolved_oclc HAVING overlap >= 15
    ) AS x
    GROUP BY x.member_id, x.overlap
    ORDER BY x.member_id, x.overlap
  }.join(' '),

  # This one has issues for sure. Check oclc 368547, held by 4 members in holdings_memberitem, but only 1 in holdings_htitem_htmember_jn
  "Unique OCNs HT: total number of this library\'s committed OCNs held by only one HT member_id" =>
  %w{
    SELECT spc.member_id, COUNT(spc.resolved_oclc) AS c
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    WHERE hhh.H = 1
    GROUP BY spc.member_id
  }.join(' '),

  # same issues as above query
  "Rare OCNs HT: total number of this library\'s committed OCNs held by <=5 HT member_ids" =>
  %w{
    SELECT x.member_id, x.overlap_ht, COUNT(x.overlap_ht) AS c FROM (
    SELECT spc.member_id, spc.resolved_oclc, MAX(hhh.H) AS overlap_ht
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    GROUP BY spc.member_id, spc.resolved_oclc
    HAVING overlap_ht <= 5
    ) AS x GROUP BY x.member_id, x.overlap_ht
  }.join(' '),

  "Common OCNs HT: total number of this library\'s committed OCNs held > 20 HT member_ids" =>
  %w{
    SELECT spc.member_id, COUNT(DISTINCT spc.resolved_oclc) AS c
    FROM shared_print_commitments AS spc
    JOIN holdings_cluster_oclc      AS hco  ON (spc.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    WHERE hhh.H > 20
    GROUP BY spc.member_id
  }.join(' '),

  "Total Distinct OCNs" => "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_commitments",

};

# Add some more queries.
1.upto(20) do |x|
  label = "Distinct OCNs Committed by exactly #{x} SP Library";
  if x > 1 then
    label.gsub!('Library', "Libraries");
  end

  sql = %W{
    SELECT COUNT(*) AS c FROM (
        SELECT resolved_oclc, COUNT(DISTINCT member_id) AS h FROM shared_print_commitments GROUP BY resolved_oclc HAVING h = #{x}
    ) AS x
  }.join(' ');
  queries[label] = sql;
end

i = 0;
queries.each do |label, sql|
  i += 1;
  log.d (i % 2 == 0 ? "TOCK" : "TICK");
  log.d(sql);
  hdout.file.puts("# #{i})\t#{label}");
  # next if i <= 15;

  if sql != "" then
    if sql.include?('?') then
      # Querying by member_id
      q = conn.prepare(sql);
      total = 0;
      member_ids.each do |member_id|
        q.enumerate(member_id) do |row|
          hdout.file.puts(['','',member_id, row[:c]].join("\t"));
          total += row[:c].to_i;
        end
      end
      hdout.file.puts(['','',"TOTAL", total].join("\t"));
    else
      # Not querying by member_id
      header = false;
      conn.query(sql) do |row|
        if !header then
          hdout.file.puts("\t\t" + (row.to_h.keys.join("\t")));
          header = true;
        end
        hdout.file.puts("\t\t" + (row.to_h.values.join("\t")));
      end
    end
  end
end

hdout.close();
