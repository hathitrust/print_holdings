require 'hathidata';
require 'hathidb';

=begin
Some lawyers wanted to know the members that have any LM, WD or BRT
volumes in a list of ids. Gotta run on grog.
=end

db   = Hathidb::Db.new();
p_conn = db.get_prod_conn();
sql = %w[
  SELECT volume_id, member_id, lm_count, wd_count, brt_count 
  FROM holdings_htitem_htmember 
  WHERE volume_id = ? AND (lm_count + wd_count + brt_count) > 0
].join(' ');
sth = p_conn.prepare(sql);
puts [:LM, :WD, :BRT].join("\t");
ids = [];
Hathidata.read(ARGV.shift) do |line|
  id = line.strip;
  ids << id;
  sth.query(id) do |row|
    puts row.to_a.join("\t");
  end
end

# # Generate greps and put in a file.
hathifile_path  = Hathidata::Data.new('builds/current/hathi_full.txt').path;
hathifile_match = Hathidata::Data.new('hathifile_matches.txt').path;
hathifile_bash  = Hathidata::Data.new('hathifile_greps.sh').open('w');
chunk           = [];
chunk_max_size  = 25;

while ids.size > 0 do
  chunk << ids.shift.gsub(/\./, '\\.').gsub(/\$/, '\\$');
  if (chunk.size >= chunk_max_size || ids.size == 0) then
    hathifile_bash.file.puts ['egrep $\'^(', chunk.join('|'), ')\t\'', ' ', hathifile_path, ' >> ', hathifile_match, ';'].join('');
    chunk = [];
  end
end
hathifile_bash.close();

STDERR.puts "now do:\nbash #{hathifile_bash.path}";
