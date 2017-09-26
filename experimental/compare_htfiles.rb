require 'set';

# Takes a list of member ids from commandline and compares which files
# are in that member's memberdata dir with the ones that are in the 
# HT00x dir. Comparison uses md5sum.

def compare (mem_list)
  memberdata_dir = '/htapps/mwarin.babel/phdb_scripts/data/memberdata';
  ht00x_dir      = '/htapps/mwarin.babel/phdb_scripts/data/loadfiles';
  md_files = {};
  ht_files = {};
  # Compare files.
  mem_list.each do |member_id|
    Dir.entries(memberdata_dir + '/' + member_id).each do |e|
      if e =~ /HT\d+_#{member_id}.(mono|multi|serial).tsv/ then
        md_files[e] = %x(md5sum #{memberdata_dir + '/' + member_id}/#{e}).split()[0];
      end
    end
    Dir.entries(ht00x_dir).each do |e|
      if e =~ /HT\d+_#{member_id}\.(mono|multi|serial)\.tsv$/ then
        ht_files[e] = %x(md5sum #{ht00x_dir}/#{e}).split()[0];
      end
    end
  end

  # Display results.
  allkeys = (Set.new [md_files.keys, ht_files.keys].flatten).to_a;
  puts "same?\tname\t#{memberdata_dir}\t#{ht00x_dir}";
  allkeys.sort.each do |k|
    md   = md_files[k] || 'N/A';
    ht   = ht_files[k] || 'N/A';
    same = false;
    if (md == ht) then
      same = true;
    end
    puts "#{same}\t#{k}\t#{md}\t#{ht}";
  end
end

if $0 == __FILE__ then
  if ARGV.length > 0 then
    compare(ARGV);
  else
    require 'hathidb';
    require 'hathiquery';

    db = Hathidb::Db.new();
    conn = db.get_conn();
    member_ids = [];
    conn.query(Hathiquery.get_active_members) do |row|
      member_ids << row[:member_id];
    end
    compare(member_ids);
  end
end
