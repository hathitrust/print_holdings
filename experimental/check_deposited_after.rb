require 'hathidata';
require 'hathidb';
require 'hathilog';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

get_member_id_sql = "SELECT member_id FROM holdings_htitem_htmember_jn where volume_id = ?";
get_member_id_q   = conn.prepare(get_member_id_sql);
member_id_counts  = {};
i = 0;

Hathidata.read("before_10e_20170509_rights.tsv") do |line|
  line.strip!
  (volume_id, source, rights, oclc) = line.split("\t");
  rights = rights.sub(/^cc.*/, "cc");

  (namespace, rest) = volume_id.split('.');
  member_id = '';
  get_member_id_q.query(volume_id) do |row|
    member_id = row[:member_id];
  end
  member_id_counts[member_id] ||= {};
  member_id_counts[member_id][:count]         ||= {};
  member_id_counts[member_id][:count][:total] ||= 0;
  member_id_counts[member_id][:count][:total] +=  1;
  member_id_counts[member_id][:namespace] ||= {};
  member_id_counts[member_id][:namespace][namespace] ||= 0
  member_id_counts[member_id][:namespace][namespace] +=  1;
  member_id_counts[member_id][:source] ||= {};
  member_id_counts[member_id][:source][source] ||= 0;
  member_id_counts[member_id][:source][source] +=  1;
  member_id_counts[member_id][:rights]         ||= {};
  member_id_counts[member_id][:rights][rights] ||= 0;
  member_id_counts[member_id][:rights][rights] +=  1;
  i += 1;
  log.d(i) if i % 10000 == 0;
end

member_id_counts.keys.sort.each do |member_id|
  member_id_counts[member_id].keys.sort.each do |label|
    member_id_counts[member_id][label].keys.sort.each do |value|
      count =  member_id_counts[member_id][label][value];
      puts [member_id, label, value, count].join("\t");
    end
  end
end
