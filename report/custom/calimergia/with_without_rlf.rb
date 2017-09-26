# UC wants to know what it would look like if we folded back nrlf->berkeley and srlf->ucla
# Took about 3.5h to run on punch while the build was running.

require 'hathidb';
require 'hathilog';
require 'hathidata';

db           = Hathidb::Db.new();
conn         = db.get_conn();
log          = Hathilog::Log.new();
cost_per_vol = 0.21328463955220106;

# Get all members:
member_ids  = %w[berkeley nrlf srlf ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf];
uc_regexp   = Regexp.new(/^(berkeley|nrlf|srlf|ucdavis|uci|ucla|ucmerced|ucr|ucsb|ucsc|ucsd|ucsf)$/);
rlf_regexp  = Regexp.new(/^[ns]rlf$/);
member_ids_qstring = member_ids.map{|x| "'#{x}'"}.join(', ');

# First we get all the IC volume_ids held by a UC member
get_volume_ids_sql = "SELECT DISTINCT t1.volume_id FROM holdings_htitem_htmember_jn as t1 JOIN holdings_htitem AS t2 ON (t1.volume_id = t2.volume_id) WHERE t1.member_id IN (#{member_ids_qstring}) AND t2.access = 'deny'";
get_holders_sql = "SELECT DISTINCT member_id FROM holdings_htitem_htmember_jn WHERE volume_id = ?";
get_holders_q   = conn.prepare(get_holders_sql);

# Why does Regexp not have something like this?
def bool_match (regexp, str)
  !(regexp =~ str).nil?
end

old_cost = 0.0;
new_cost = 0.0;
i = 0;

cost_per_member = {};
member_ids.map{|m| cost_per_member[m] = 0.0};

hdout = Hathidata::Data.new("uc_with_without_rlf_$ymd.tsv").open('w');
hdout.file.puts(%w[volume_id old_h new_h old_uc_h new_uc_h].join("\t"));
# Loop over volume_ids
conn.query(get_volume_ids_sql) do |v_row|
  volume_id = v_row[:volume_id];
  # puts v_row[:volume_id];

  i += 1;
  if i % 5000 == 0 then
    log.d(i);
  end

  holder_ids = [];
  get_holders_q.enumerate(volume_id) do |m_row|
    member_id = m_row[:member_id];
    # Turn all rlfs to something else
    if member_id == 'nrlf' then
      member_id = 'berkeley';
    elsif member_id == 'srlf' then
      member_id = 'ucla';
    end
    holder_ids << member_id;
  end
  old_h      = holder_ids.size;
  new_h      = holder_ids.uniq.size;
  uc_holders = holder_ids.grep(uc_regexp);
  old_uc_h   = uc_holders.size;
  new_uc_h   = uc_holders.uniq.size;

  # If old_h and old_uc_h was 22 and 12, then UC used to pay 12/22nds of a cost-per-volume per volume.
  # If old UC contained nrlf and srlf, then the new values could be (assuming berkeley and ucla were also in the mix)
  # then new_h = 20 and new_uc_h = 10, so UC pay 10/20ths of a cost-per-volume per volume.
  hdout.file.puts([volume_id, old_h, new_h, old_uc_h, new_uc_h].join("\t"));
  old_cost += (old_uc_h.to_f / old_h) * cost_per_vol;
  new_cost += (new_uc_h.to_f / new_h) * cost_per_vol;

  # assign cost per uc member
  new_cost_per_uc_member = ((new_uc_h.to_f / new_h) * cost_per_vol) / new_uc_h;
  uc_holders.uniq.each do |m|
    cost_per_member[m] += new_cost_per_uc_member;
  end
end

puts "old_cost : #{old_cost}";
puts "new_cost : #{new_cost}";
puts "new cost per uc member:";
member_ids.each do |m|
  puts "#{m}\t:\t#{cost_per_member[m]}";
end

hdout.close();
