require 'hathidb';
require 'hathidata';

db = Hathidb::Db.new();
conn = db.get_conn();

dec_q = conn.prepare("SELECT DISTINCT member_id FROM holdings_htitem_htmember_jn_dec_other WHERE volume_id = ?");
cur_q = conn.prepare("SELECT DISTINCT member_id FROM holdings_htitem_htmember_jn_cur_other WHERE volume_id = ?");

loss_counter = {};
i = 0;
Hathidata.read(ARGV.shift) do |line|
  i += 1;
  if i % 5000 == 0 then
    puts i;
  end
  (volume_id, old, new, diff) = line.split("\t");  
  if old.to_i == 2 && new.to_i == 1 then
    old_ids = [];
    new_ids = [];
    dec_q.enumerate(volume_id) do |row|
      old_ids << row[:member_id];
    end
    cur_q.enumerate(volume_id) do |row|
      new_ids << row[:member_id];
    end
    diff = old_ids - new_ids;

    diff.each do |m|
      loss_counter[m] ||= 0;
      loss_counter[m] +=  1;
    end
  end
end

loss_counter.each do |m,c|
  puts "'#{m}' lost #{c}";
end
