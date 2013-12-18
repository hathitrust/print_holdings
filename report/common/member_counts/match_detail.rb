require 'hathidb';

db = Hathidb::Db.new();
conn1 = db.get_conn();

system("echo '' >> unique_counts.tsv");

q1 = "SELECT DISTINCT member_id FROM holdings_memberitem WHERE member_id >= 'harvard'";
conn1.query(q1).each do |row1|
  m = row1[:member_id];
  q2 = "SELECT oclc FROM holdings_memberitem WHERE member_id = '#{m}';";

  outfn = "match_detail_#{m}.txt";
  outf = File.open(outfn, 'w');
  outf.sync = true;

  system("echo '#{m}:' >> unique_counts.tsv");

  conn2 = db.get_conn();
  conn2.query(q2).each do |row2|
    outf.puts row2[:oclc];
  end
  conn2.close();
  outf.close();  

  cmd = "sort #{outfn} | uniq -c | wc -l >> unique_counts.tsv";
  puts cmd;
  system(cmd);

  rm_cmd = "rm -v #{outfn}";
  puts rm_cmd;
  system(rm_cmd);
end

conn1.close();
