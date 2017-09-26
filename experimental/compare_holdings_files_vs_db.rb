require 'hathidata';
require 'hathidb';
require 'hathiquery';

# Check if there are any great discrepancies between what are in the HT003 files and what's loaded into the DB.

ht_path_template = 'loadfiles/HT003_xxx.yyy.tsv'; # e.g. HT003_amherst.multi.tsv
members          = {};
item_types       = %w[mono multi serial];

db   = Hathidb::Db.new();
conn = db.get_conn();

conn.query(Hathiquery.get_active_members) do |row|
  members[row[:member_id]] = 1;
end

# Get counts from holdings_memberitem for each member_id and item_type.
hm_item_type_count = {};
hm_item_type_sql   = "SELECT COUNT(*) AS c FROM holdings_memberitem WHERE member_id = ? AND item_type = ?";
hm_item_type_q     = conn.prepare(hm_item_type_sql);
members.keys.sort.each do |member_id|
  item_types.each do |item_type|
    hm_item_type_q.enumerate(member_id, item_type) do |row|
      hm_item_type_count[member_id] ||= {};
      hm_item_type_count[member_id][item_type] = row[:c].to_i;
    end
  end
end
conn.close();

# Get corresponding counts from HT003 files in data/loadfiles/
line_counts = {};
members.keys.sort.each do |member_id|
  item_types.each do |item_type|
    hd_name = ht_path_template.sub('xxx', member_id).sub('yyy', item_type);
    hd = Hathidata::Data.new(hd_name);
    wcl = %x[wc -l #{hd.path}];
    line_counts[member_id] ||= {};
    line_counts[member_id][item_type] = 0;
    if !wcl.nil? && !wcl.empty? then
      line_count = wcl.split()[0].to_i;
      line_counts[member_id][item_type] = line_count - 1;
    end
  end
end
hdout = Hathidata::Data.new('reports/file_db_diff_$ymd.tsv').open('w');
hdout.file.puts(%w[member_id item_type db_rows file_lines diff].join("\t"));

# Compare counts from db and files.
members.keys.sort.each do |member_id|
  member_total_diff = 0;
  item_types.each do |item_type|
    db_rows    = hm_item_type_count[member_id][item_type];
    file_lines = line_counts[member_id][item_type];    
    diff = db_rows - file_lines;
    member_total_diff += diff;
    hdout.file.puts([member_id, item_type, db_rows, file_lines, diff].join("\t"));
  end
  hdout.file.puts([member_id, 'total', '--', '--', member_total_diff].join("\t"));
end
hdout.close();
