require 'hathidb';
require 'hathidata';
require 'hathilog';

# Lizanne wants to check if SCS / Rick Lugg et al can do something for us. They need:
# 10K recs NOT from the SP registry, that is, from the 3 million HT monographs that are not in SP.
# For purposes of testing their logic

db    = Hathidb::Db.new();
conn  = db.get_conn();
hdout = Hathidata::Data.new('mono_oclc_sym_$ymd.tsv').open('w')
log   = Hathilog::Log.new();

get_ocn_sql = %w{
  SELECT hm.oclc, MAX(hhh.H)
  FROM holdings_memberitem  AS hm
  JOIN holdings_htitem_oclc AS hho ON (hm.oclc = hho.oclc)
  JOIN holdings_htitem_H    AS hhh ON (hho.volume_id = hhh.volume_id)
  LEFT JOIN shared_print_commitments AS sp ON (hm.member_id = sp.member_id AND hm.oclc = sp.local_oclc)
  WHERE
  sp.local_oclc IS NULL
  AND
  hm.item_type = 'mono'
  GROUP BY hho.oclc
  HAVING MAX(hhh.H) = ?
  LIMIT 0, 50
}.join(' ');
get_ocn_q = conn.prepare(get_ocn_sql);

# 131 current ph members
1.upto(131) do |h|
  log.i(h);
  ocns = [];
  get_ocn_q.enumerate(h) do |row|
    ocns << row[:oclc];
  end
  if !ocns.empty? then
    ocns_str = ocns.map{|x| "'#{x}'"}.join(', ');
    log.i(ocns_str);
    sql = %W{
      SELECT DISTINCT hm.oclc, hh.oclc_sym
      FROM holdings_memberitem  AS hm
      JOIN holdings_htmember    AS hh  ON (hm.member_id = hh.member_id)
      WHERE
      hh.oclc_sym IS NOT NULL
      AND
      hm.oclc IN (#{ocns_str})
    }.join(' ');

    conn.query(sql) do |row|
      hdout.file.puts(row.to_a.join("\t"));
    end
  end
end

hdout.close();
