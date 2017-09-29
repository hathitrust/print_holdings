require 'hathidb';
require 'hathidata';
require 'hathilog';

def get_h_and_group_h (db, log, member_ids)
  sql = %W<
    SELECT 
        h.volume_id,
        hhh.H,
        COUNT(hhhj.member_id) AS group_h
    FROM 
        holdings_htitem AS h
        JOIN
        holdings_htitem_htmember_jn AS hhhj
        ON (hhhj.volume_id = h.volume_id)
        JOIN
        holdings_htitem_H AS hhh
        ON (hhhj.volume_id = hhh.volume_id)
    WHERE
        h.access = 'deny'
        AND
        hhhj.member_id IN (#{member_ids.map{|x| "'#{x}'"}.join(',')}) 
    GROUP BY 
        h.volume_id,
        hhh.H
  >.join(' ');

  log.d(sql);

  merge_name = member_ids.sort.join('_')
  hd = Hathidata::Data.new("#{merge_name}_volid_h.tsv");
  if hd.exists? then
    log.d("Already have #{hd.path}");
  else
    hd.open('w');
    conn = db.get_conn();
    i = 0;
    conn.query(sql) do |row|
      i += 1;
      hd.file.puts "#{row['volume_id']}\t#{row['H']}\t#{row['group_h']}";
      if i % 100000 == 0 then
        log.d(i);
      end
    end
    conn.close();
    hd.close();
  end
end

if $0 == __FILE__ then
  db   = Hathidb::Db.new();
  log  = Hathilog::Log.new();
  member_ids = [];
  if ARGV.size > 0 then
    member_ids = ARGV;
  else
    member_ids = %W<unc ncsu nccu duke>;
  end

  get_h_and_group_h(db, log, member_ids);
end
