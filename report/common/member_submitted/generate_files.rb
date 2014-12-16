require 'hathidata';
require 'hathidb';
require 'hathilog';

member_id = ARGV.shift;
raise "Need member_id as 1st cmdline arg" if member_id.nil?;

log  = Hathilog::Log.new();
db   = Hathidb::Db.new();
conn = db.get_conn();

sel_sql   = %w[
    SELECT local_id, oclc, status, item_condition, enum_chron, issn 
    FROM holdings_memberitem 
    WHERE member_id = ? AND item_type = ? 
    ORDER BY local_id, oclc, enum_chron, status, item_condition, issn
].join(" ");
sel_query = conn.prepare(sel_sql);

item_type_cols = {
  'mono'   => %w[local_id oclc status item_condition],
  'multi'  => %w[local_id oclc status item_condition  enum_chron],
  'serial' => %w[local_id oclc issn],
};

%w[mono multi serial].each do |item_type|
  cols = item_type_cols[item_type];

  report_path = "reports/member_submitted_inventory/#{member_id}_#{item_type}.tsv";
  hdout       = Hathidata::Data.new(report_path).open('w');
  hdout.file.puts(cols.join("\t"));
  sel_query.enumerate(member_id, item_type) do |row|
    hdout.file.puts(cols.map{|x| row[x]}.join("\t"));
  end
  hdout.close().deflate();
end
