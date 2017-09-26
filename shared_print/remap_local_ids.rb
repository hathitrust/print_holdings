=begin

It happens that a member gives us commitments, and then switch to another ILS,
or something like that, which leads to their local_ids changing.

This script takes a member_id, and a mapping of old local_ids to new local_ids
and updates the commitments.

We assume that the mapping is a file with 2 tab-sep columns: old<TAB>new

=end

require 'hathidata';
require 'hathilog';

map = {};
map_file = Hathidata::Data.new(ARGV.shift).open('r');

map_file.file.each_line do |line|
  line.strip!
  (old, new) = line.split("\t");
  map[old] = new;
end
map_file.close();

commitments_fn    = ARGV.shift;
commitments_file  = Hathidata::Data.new(commitments_fn).open('r');
commitments_remap = Hathidata::Data.new("#{commitments_fn}.remap_$ymd").open('w');

local_id_col = nil;
i = 0;
commitments_file.file.each_line do |line|
  line.strip!
  if i == 0 then
    local_id_col = line.split("\t").index('local_id');
    commitments_remap.file.puts(line);
    i += 1;
  else
    cols = line.split("\t");
    old_local_id       = cols[local_id_col];
    new_local_id       = map[old_local_id];
    if new_local_id.nil? then
      commitments_remap.file.puts(line);
    else
      cols[local_id_col] = new_local_id;
      puts "#{old_local_id} -> #{new_local_id}";
      commitments_remap.file.puts(cols.join("\t"));
    end
  end
end

commitments_file.close();
commitments_remap.close();
