require 'hathidata';
require 'hathidb';
require 'hathilog';

# This is perhaps a bit more naive than not_in_spp.rb, but that one seems to massively under-report.

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

hdout_path = "reports/shared_print/in_hathi_but_not_in_pool_$ymd.txt";
hdout = Hathidata::Data.new(hdout_path).open('w');

main_sql = %w<
  SELECT DISTINCT hco.oclc AS in_hathi, spp.resolved_oclc AS in_pool
  FROM holdings_cluster       AS hc
  JOIN holdings_cluster_oclc  AS hco ON (hc.cluster_id = hco.cluster_id)
  LEFT JOIN shared_print_pool AS spp ON (hco.oclc = spp.resolved_oclc)
  WHERE hc.cluster_type = 'spm' AND spp.resolved_oclc IS NULL
>.join(' ');

@how_many = 1000;
many_q   = (['?'] * @how_many).join(',');

look_up_many_h_sql = %W<
    SELECT t1.oclc, COUNT(DISTINCT t2.member_id) AS other_h
    FROM holdings_htitem_oclc AS t1
    JOIN holdings_htitem_htmember_jn AS t2 ON (t1.volume_id = t2.volume_id)
    WHERE t1.oclc IN (#{many_q})
    GROUP BY t1.oclc
>.join(' ');

look_up_many_h_q = conn.prepare(look_up_many_h_sql);

def look_up_h (q, oclcs, outf)
  q.enumerate(*oclcs) do |row|
    outf.file.puts([row[:oclc], row[:other_h]].join("\t"));
  end
end

i = 0;
oclc_buffer = [];
conn.query(main_sql) do |row|
  oclc_buffer << row[:in_hathi];
  if oclc_buffer.size == @how_many then
    i += 1;
    log.i(i);
    look_up_h(look_up_many_h_q, oclc_buffer, hdout);
    oclc_buffer = [];
  end
end
# remaining oclcs
if !oclc_buffer.empty? && oclc_buffer.size < @how_many then
  # Pad buffer with nils
  oclc_buffer = oclc_buffer + (['?'] * (@how_many - oclc_buffer.size));
  look_up_h(look_up_many_h_q, oclc_buffer, hdout);
end

hdout.close();
