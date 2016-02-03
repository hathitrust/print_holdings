=begin

A patron with print disability at University of X tries to access a
volume that University of X say they have, but can't. So Melissa or
Angelina send me an email asking if ux should be able to access
oclc yyy. That's when I run this script. Try to find where, if anywhere
the problem exists. Can only go so far.

Call thusly:

  ruby check_access.rb <member_id> <oclc>

=end

require 'hathidb';
require 'hathidata';

member_id = ARGV.shift;
oclc     = ARGV.shift;

submitted       = false;
memberitem      = false;
volume_id       = nil;
holdings_htitem = false;
hhhj            = false;

if member_id.nil? || oclc.nil? then
  raise "Need member_id and oclc as 1st and 2nd args.";
end

%w{mono multi serial}.each do |item_type|
  data_path = "loadfiles/HT003_#{member_id}.#{item_type}.tsv";
  hdin = Hathidata::Data.new(data_path).open('r');
  hdin.file.each_line do |line|
    line.strip!
    cols = line.split("\t");
    if cols.first == oclc then
      puts line;
      submitted = true;
    end
  end
  hdin.close();
end

if submitted == false then
  puts "#{member_id} did NOT submit OCLC #{oclc}. Stop looking.";
  exit;
end

puts "#{member_id} did in fact submit OCLC #{oclc}. Keep digging.";

db = Hathidb::Db.new();
conn = db.get_conn();

sql_memberitem = %w{
  SELECT oclc, local_id, member_id, status, item_condition, process_date, item_type
  FROM holdings_memberitem
  WHERE member_id = ? AND oclc = ?
}.join(' ');

q_memberitem = conn.prepare(sql_memberitem);
q_memberitem.enumerate(member_id, oclc) do |row|
  puts row.to_a.join(" | ");
  memberitem = true;
end

if memberitem == false then
  puts "Did not make it into holdings_memberitem. Stop looking.";
  exit;
end

puts "Made it into holdings_memberitem, keep digging!";

volume_id_sql = %w{
  SELECT hco.oclc, hchj.volume_id
  FROM holdings_cluster_oclc AS hco
  JOIN holdings_cluster_htitem_jn AS hchj
  ON (hco.cluster_id = hchj.cluster_id)
  WHERE hco.oclc = ?
}.join(' ');

q_volume_id = conn.prepare(volume_id_sql);
q_volume_id.enumerate(oclc) do |row|
  puts row.to_a.join(" | ");
  volume_id = row[:volume_id];
end

if volume_id.nil? then
  puts "Couldn't find a volume_id. Stop looking.";
  exit;
end

puts "Found volume_id #{volume_id}";

holdings_htitem_sql = %w{
  SELECT volume_id, access, rights
  FROM holdings_htitem
  WHERE volume_id = ?
}.join(' ');

q_holdings_htitem = conn.prepare(holdings_htitem_sql);
q_holdings_htitem.enumerate(volume_id) do |row|
  puts row.to_a.join(" | ");
  holdings_htitem = true;
end

if holdings_htitem == false then
  puts "No record of #{volume_id} in holdings_htitem. Stop looking.";
  exit;
end

puts "Found record of #{volume_id} in holdings_htitem. Keep digging.";

hhhj_sql = %w{
  SELECT * 
  FROM holdings_htitem_htmember_jn 
  WHERE volume_id = ?
  AND member_id = ?
}.join(' ');

q_hhhj = conn.prepare(hhhj_sql);
q_hhhj.enumerate(volume_id, member_id) do |row|
  puts row.to_a.join(" | ");
  hhhj = true;
end

if hhhj == false then
  puts "No record of #{volume_id} in holdings_htitem_htmember_jn. Stop looking."
  exit;
end

puts "Found record of #{volume_id} in holdings_htitem_htmember_jn";

