=begin
So, I have 2 files of Florida-holdings.

1) A list of volume_ids obtained from the estimate script.
2) A list of volume_ids, H and group_H for fsu and ufl.

Now to get the real IC cost for this monster, I will overwrite lines
in 1 with corresponding lines in 2, set new H to H - (group_H - 1) 
for those lines.

For all other lines I will set holdings_htitem_H.H++.

Output a textfile of H, which can be turned into frequency list and 
used for calculating cost.
=end

require 'hathilog';
require 'hathidata';
require 'hathidb';

def main (log, conn)
  volids = {};

  # Read all volids for the group, we don't know H yet, so set to 0.
  Hathidata.read('holdings_memberitem_fla_sus-volume_id.20140310.out') do |line|
    volids[line.strip] = 0;
  end

  # Read all the known H and groupH, overwrite.
  Hathidata.read('fsu_ufl_volid_h.tsv') do |line|
    volid, h, grouph = *line.strip.split("\t");
    volids[volid]    = h.to_i - (grouph.to_i - 1);
  end

  no_h = [];
  # Loop through all volids. 
  # If H is 0, look it up, else print H.
  Hathidata.write("merge_out_h_$ymd") do |hdout|
    volids.each_pair do |volid,h|
      if h == 0 then
        no_h << volid;
      else
        hdout.file.puts h;
      end
      volids.delete(volid);

      if no_h.size >= 1000 then
        look_up_h(no_h, hdout, conn);
        no_h = [];
      end
    end

    if no_h.size > 0 then
      look_up_h(no_h, hdout, conn);
    end
  end
end

def look_up_h (list, hdout, conn)
  list_str = list.map{|x| "'#{x}'"}.join(',');
  q = "SELECT H FROM holdings_htitem_H WHERE volume_id IN (#{list_str})";
  conn.query(q) do |row|
    hdout.file.puts row[:H].to_i + 1;
  end
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  db = Hathidb::Db.new();
  conn = db.get_conn();
  main(log, conn);
  log.d("Finished");
end

