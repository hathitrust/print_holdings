require 'hathidb';

db   = Hathidb::Db.new();
conn = db.get_conn();

label_qs = {
  "total number of matching records (occurrences of OCN + member_id)" => "SELECT COUNT(*) AS c FROM shared_print_pool",
  "total number of distinct OCNs (unduplicated count of OCNs)"        => "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_pool",
  "total number of unique OCNs (held by only one member_id)"          => "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_pool WHERE local_h = 1",
  "number of distinct OCNs held by <= 5 member_ids"                   => "SELECT COUNT(DISTINCT resolved_oclc) AS c, local_h FROM shared_print_pool GROUP BY local_h HAVING local_h <= 5 ORDER BY local_h",
  "number of distinct OCNs held by >= 45 member_ids"                  => "SELECT COUNT(DISTINCT resolved_oclc) AS c, local_h FROM shared_print_pool GROUP BY local_h HAVING local_h >= 45 ORDER BY local_h",
};

# Ugh what a clunker.
squeequel = %w[
    SELECT COUNT(DISTINCT spp.resolved_oclc) AS c, spp.local_h AS overlap_sp, MAX(hhh.H) AS overlap_ht,
    FROM shared_print_pool AS spp
    JOIN holdings_cluster_oclc      AS hco  ON (spp.resolved_oclc = hco.oclc)
    JOIN holdings_cluster           AS hc   ON (hco.cluster_id = hc.cluster_id)
    JOIN holdings_cluster_htitem_jn AS hchj ON (hc.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    GROUP BY spp.local_h 
    HAVING spp.local_h >= 45 
    ORDER BY spp.local_h
].join(' ');

label_qs.keys.each do |k|
  puts k;
  puts label_qs[k];
  header = false;
  conn.query(label_qs[k]) do |row|
    if !header
      puts row.labels.join("\t");
      header = true;
    end
    puts row.to_a.join("\t");    
  end
  puts "---\n";
end

label_qs_by_member = {
  "total number of matching records (OCN + member_id)"        => "SELECT COUNT(*) AS c FROM shared_print_pool WHERE member_id = ?",
  "total number of unique OCNs (held only by this member_id)" => "SELECT COUNT(DISTINCT resolved_oclc) AS c FROM shared_print_pool WHERE member_id = ? AND local_h = 1",
  "number of OCNs held by <= 5 member_ids"                    => "SELECT COUNT(DISTINCT resolved_oclc) AS c, local_h, member_id FROM shared_print_pool WHERE member_id = ? GROUP BY local_h, member_id HAVING local_h <= 5 ORDER BY local_h",
  "number of distinct OCNs held by >= 45 member_ids"          => "SELECT COUNT(DISTINCT resolved_oclc) AS c, local_h, member_id FROM shared_print_pool WHERE member_id = ? GROUP BY local_h, member_id HAVING local_h >= 45 ORDER BY local_h",
};

member_ids_sql = "SELECT DISTINCT member_id FROM shared_print_pool ORDER BY member_id";
conn.query(member_ids_sql) do |row|
  member_id = row[:member_id];
  puts member_id;
  label_qs_by_member.keys.each do |k|
    note = k;
    sql  = label_qs_by_member[k];
    puts "\t#{note}";
    puts "\t#{sql}";
    q = conn.prepare(sql);
    header = false;
    q.enumerate(member_id) do |r|
      if !header
        puts "\t" + r.labels.join("\t");
        header = true;
      end
      puts "\t" + r.to_a.join("\t");
    end    
    puts "\t---\n";
  end
  puts "---\n";
end
