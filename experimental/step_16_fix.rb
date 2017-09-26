=begin

There was a hiccup in the prod db on the morning of 9/11 (duh-duh-dUUUUUUH) 2015 which led one of the copy-bits from dev to prod to fail. Luckily we know exactly which ones are the problem:

mysqldump -h mysql-htprep -u ht_repository -pXXX ht_repository holdings_htitem_htmember_jn_dev -w"1 LIMIT 222000000, 1000000" --skip-add-drop-table --skip-disable-keys --skip-add-locks --skip-lock-tables --skip-comments --skip-set-charset --no-create-info | mysql -h mysql-sdr -u ht_repository -pXXX ht_repository

However, cannot just rerun that because SOME of those records made it over. Need to delete the ones that made it over, then run again.

=end

require 'hathidata';
require 'hathidb';
require 'hathilog';

db = Hathidb::Db.new();
dev_conn = db.get_conn();

select_from_dev_sql = "SELECT volume_id, member_id, copy_count, lm_count, wd_count, brt_count, access_count FROM holdings_htitem_htmember_jn_dev WHERE 1 LIMIT 222000000, 1000000";

hdout = Hathidata::Data.new("reinsert_dev_to_prod.tsv").open('w');

prod_conn = db.get_prod_conn();
delete_from_prod_sql = "DELETE FROM holdings_htitem_htmember_jn_dev WHERE volume_id = ? AND member_id = ?";
delete_from_prod_q   = prod_conn.prepare(delete_from_prod_sql);

dev_conn.query(select_from_dev_sql) do |row|
  hdout.file.puts(row.to_a.join("\t"));
  res = delete_from_prod_q.execute(row[:volume_id], row[:member_id]);
  puts [row[:volume_id], row[:member_id], res].join(' ');
end
hdout.close();

load_prod_sql = "LOAD DATA LOCAL INFILE '#{hdout.path}' IGNORE INTO TABLE holdings_htitem_htmember_jn_dev";
puts load_prod_sql;
prod_conn.execute(load_prod_sql);
