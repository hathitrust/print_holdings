# Scenario:
# We already have holdings from member.
# They want to know what the cost would be given new files.

# Call thusly:
# ruby reestimate.rb <member_id> <volid_file> <cost_per_vol>
# ruby reestimate.rb ualberta ualberta_volid.txt 0.149486585794649
# ... where volid_file is a path relative to the hathidata root,
# containing the volume ids of the new holdings.

require 'hathidata';
require 'hathidb';
require 'hathilog';

# Useful vars.
member_id           = ARGV.shift;
new_volid_file      = ARGV.shift;
avg_ic_cost_per_vol = ARGV.shift;
current_volid_h     = {}; # Stores the current volume_id as key, H as value.
new_volid_h         = {};
need_to_check_h     = {};

# Missing input? Inform about usage.
if [member_id, new_volid_file, avg_ic_cost_per_vol].include?(nil) then
  if member_id.nil? then
    puts "First arg <member_id> missing";
  elsif new_volid_file.nil? then
    puts "Second arg <new_volid_file> missing";
  elsif avg_ic_cost_per_vol.nil? then
    puts "Third arg <avg_ic_cost_per_vol> missing";
  end

  puts "Usage:\n\truby #{$0} <member_id> <volid_file> <cost_per_vol>";
  exit(1);
end

# Need to cast string->float.
avg_ic_cost_per_vol = avg_ic_cost_per_vol.to_f;

# DB setup.
db   = Hathidb::Db.new();
conn = db.get_conn();

# Logger setup.
logger = Hathilog::Log.new();
logger.d("Started");

# Get cracking.
# First we want to get the current holdings of the member from db,
# with volume_id and H.
get_current_from_db_sql = %w<
  SELECT hhh.volume_id, hhh.H
  FROM holdings_htitem_H AS hhh
  JOIN holdings_htitem_htmember_jn AS hhhj
  ON (hhh.volume_id = hhhj.volume_id)
  WHERE hhhj.member_id = ?
>.join(' ');
get_current_from_db_q = conn.prepare(get_current_from_db_sql);

logger.d("Running 1st query");
# Store the current volids in hash.
get_current_from_db_q.enumerate(member_id) do |row|
  current_volid_h[row[:volume_id]] = row[:H].to_i;
end

# Read file with new volids.
# Copy H from current holdings.
logger.d("Reading file");
Hathidata.read(new_volid_file) do |line|
  volid = line.strip;
  if current_volid_h.has_key?(volid) then
    new_volid_h[volid] = current_volid_h.delete(volid);
  else
    need_to_check_h[volid] = 1;
  end
end
current_volid_h = {}; # Done with you.

# Check H on volids that are not in the current set. Do 1000 at a time.
logger.d("Checking H of unknown volids");
get_h_from_volid_sql = %W<
  SELECT volume_id, H
  FROM holdings_htitem_H
  WHERE volume_id IN (#{(['?'] * 1000).join(',')})
>.join(' ');
get_h_from_volid_q = conn.prepare(get_h_from_volid_sql);

chunk = [];
need_to_check_h.keys.each_with_index do |volid, i|
  chunk << volid;
  if (chunk.size == 1000 || i == need_to_check_h.keys.size - 1) then
    logger.d(i);
    # Array must always be 1000 long.
    # Pad with nils.
    if chunk.size < 1000 then
      chunk = [chunk, ([nil] * (1000 - chunk.size))].flatten;
    end
    get_h_from_volid_q.enumerate(*chunk) do |row|
      volid = row[:volume_id];
      h = row[:H].to_i;
      need_to_check_h[volid] = h;
    end
    chunk = [];
  end
end

# Add the ones we looked up.
logger.d("Adding checked");
need_to_check_h.keys.each do |volid|
  new_volid_h[volid] = need_to_check_h.delete(volid);
end

# Get count for each H.
h_bag = {};
new_volid_h.values.each do |h|
  h_bag[h] ||= 0;
  h_bag[h] += 1;
end

# Turn H counts into costs.
new_cost = 0;
Hathidata.write("reestimate/#{member_id}_reestimate.tsv") do |hdout|
  h_bag.keys.sort.each do |h|
    h_cost = ((h_bag[h] / h.to_f) * avg_ic_cost_per_vol);
    new_cost += h_cost;
    hdout.file.puts "#{h}\t#{h_bag[h]}\t$#{h_cost}\t$#{new_cost}";
  end
  puts "tl;dr final reestimated IC cost #{new_cost}";
end

logger.d("Done");
