=begin

Generates a 3-col spreadsheet over the memberdata / ht003 files.

=end

require 'hathidb';
require 'hathiquery';

HT_GLOB = '/htapps/pulintz.babel/data/phdb/HT003/HT003_*.tsv';
MD_GLOB = '/htapps/pulintz.babel/data/phdb/MemberData/*/HT003_*.tsv';

ht_files = {};

Dir.glob(HT_GLOB).select do |f| 
  if File.file?(f) then
    fname = f.sub(/^.+\//, '');
    fobj = File.new(f);
    ht_files[fname] = {
      'size'  => fobj.size,
      'mtime' => fobj.mtime.strftime('%Y-%m-%d %H:%M:%S'),
      'path'  => f,
    };
  end
end

md_files = {};

Dir.glob(MD_GLOB).select do |f| 
  if File.file?(f) then

    next if f =~ /estimate/; # Assumption: memberdata is sharp unless marked '.estimate'

    fname = f.sub(/^.+\//, '');
    fobj = File.new(f);
    md_files[fname] = {
      'size'  => fobj.size,
      'mtime' => fobj.mtime.strftime('%Y-%m-%d %H:%M:%S'),
      'path'  => f,
    };
  end
end

allkeys = [ht_files.keys, md_files.keys].flatten.uniq.sort;
members = {};

db   = Hathidb::Db.new();
conn = db.get_conn();
conn.query(Hathiquery.get_all_members) do |row|
  members[row['member_id']] = 1;
end
conn.close();

puts %W{is_member current_f newer_f}.join("\t");

allkeys.each do |k|
  next if k =~ /estimate/;

  current = "''";
  newer   = "''";
  member  = 'non_member';

  md = k.match(/HT003_(.+).(mono|multi|serial)/);
  if md != nil then
    if members.has_key?(md[1]) then
      member = 'member'
    end
  else
    puts "!!!!!!!!!!!!!!!!!!!!!!!!!! #{k} !!!!!!!!!!!!!!!!!!!!!";
  end

  if ht_files.has_key?(k) then
    current = ht_files[k]['path']
  end

  if md_files.has_key?(k) && (!ht_files.has_key?(k) || md_files[k]['mtime'] > ht_files[k]['mtime']) then
    newer = md_files[k]['path'];
  end

  puts [member, current, newer].join("\t")

end
