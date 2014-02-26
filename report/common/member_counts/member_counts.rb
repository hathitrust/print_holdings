require 'hathidb';
require 'hathidata';
require 'hathilog';
require 'hathiquery';

@@members = [];
def get_members (conn)
  if @@members.size == 0 then
    conn.query(Hathiquery.get_all_members) do |row|
      @@members << row[:member_id]
    end
  end
  return @@members;
end

def put_total_member_counts(conn, log)
  q = ["SELECT   member_id, item_type, COUNT(*)",
       "FROM     holdings_memberitem ",
       "WHERE    item_type IN ('mono', 'multi', 'serial')",
       "GROUP BY member_id, item_type"].join(' ');
  log.d(q);
  Hathidata.write("total_member_counts_$ymd.out") do |hdout|
    conn.query(q) do |row|
      hdout.file.puts row.join("\t");
    end
  end
end

def put_matching_item_counts(conn, log)
  q = ["SELECT   item_type, COUNT(DISTINCT hhj.volume_id) AS c",
        "FROM     holdings_htitem_htmember_jn AS hhj, holdings_htitem AS h",
        "WHERE    hhj.volume_id = h.volume_id AND member_id = ?",
        "GROUP BY item_type"].join(' ');
  pq = conn.prepare(q);
  log.d(q);
  Hathidata.write("matching_item_counts_$ymd.out") do |hdout|
    get_members(conn).each do |mem|
      pq.enumerate(mem) do |row|
        out_str = [mem, row[:item_type], row[:c]].join("\t");
        puts out_str;
        hdout.file.puts out_str;
      end
    end
  end
  pq.close();
end

def put_matching_oclc_counts(conn, log)
  q = ["SELECT   item_type, COUNT(DISTINCT oclc) AS c",
       "FROM     holdings_memberitem",
       "WHERE    member_id = ?",
       "GROUP BY item_type"].join(' ');
  pq = conn.prepare(q);
  log.d(q);
  Hathidata.write("matching_oclc_counts_$ymd.out") do |hdout|
    get_members(conn).each do |mem|
      pq.enumerate(mem) do |row|
        out_str = "#{mem}\t#{row[:item_type]}\t#{row[:c]}";
        puts out_str;
        hdout.file.puts out_str;
      end
    end
  end
  pq.close();
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  db   = Hathidb::Db.new();
  conn = db.get_conn();

  put_total_member_counts(conn, log);
  put_matching_item_counts(conn, log);
  put_matching_oclc_counts(conn, log);

  conn.close();
end
