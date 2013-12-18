require 'hathidb';
require 'hathilog';

=begin

Based on the CIC holdings in step_02_out.tsv, checks what the
numbers for Hathi as a whole are. Breaks it down by how many 
CIC members hold an item and wether it is open or not.

=end

def process_subset (volume_ids)
  tot_vol = 0; # Sum of copies
  tot_acc = 0; # Sum of access
  cm      = 0; # Count members holding
  cv      = 0; # Count volumes

  data_subset = {}; # Return hash.

  slice_count = 0;
  max_slice_size = 1000;
  conn = $db.get_conn();

  noof_slices = (volume_ids.length / max_slice_size) + 1;

  volume_ids.each_slice(max_slice_size) do |slice|
    slice_count += 1;
    $log.d("Slice #{slice_count} / #{noof_slices} for this segment.");

    q = %Q!
    SELECT 
        SUM(copy_count)           AS tot_vol, 
        SUM(access_count)         AS tot_acc, 
        COUNT(member_id)          AS cm, 
        COUNT(DISTINCT volume_id) AS cv 
    FROM 
        holdings_htitem_htmember_jn 
    WHERE
        volume_id IN (#{slice.map{|v| "'#{v}'"}.join(',')})
    !;

    conn.query(q) do |res|
      tot_vol += res[:tot_vol].to_i;
      tot_acc += res[:tot_acc].to_i;
      cm      += res[:cm].to_i;
      cv      += res[:cv].to_i;
    end

    if (slice_count % 100 == 0) then
      $log.d("So far:");
      puts "tot_vol : #{tot_vol}";
      puts "tot_acc : #{tot_acc}";
      puts "Avg members holding a volume : #{cm.to_f / cv}";
      # break;
      sleep 0.25;
    end
  end
  conn.close();

  data_subset['tot_vol'] = tot_vol;
  data_subset['tot_acc'] = tot_acc;
  data_subset['m_per_v'] = cm.to_f / cv;

  return data_subset;
end

if $0 == __FILE__ then

  $db  = Hathidb::Db.new();
  $log = Hathilog::Log.new();
  data_points = {};

  $log.d("Started");

  # File should look like:
  # cc      ac      volume_id
  # 1       0       10000013
  # 1       0       10000023
  # 1       1       100000272

  inf = File.open('step_02_out.tsv', 'r'); # Assuming the file is sorted.

  ids      = [];
  cc_count = 0;
  ac_count = 0;

  inf.each_line do |line|
    line.strip!;
    next if line =~ /cc.*ac.*volume_id/; # Skip header line.

    cols = line.split(/\t/);

    if cols.length == 3 then
      cc = cols[0].to_i;
      ac = cols[1].to_i;
      id = cols[2];

      if cc_count == 0 then
        cc_count = cc;
      end

      # There are 2 conditions for calling process_subset(),
      # Either the cc_count just went up (new segment) or
      # ac went from 0 to >0. (from closed to open).
      do_it = false;
      o_c   = nil;
      if ac_count == 0 && ac > 0 then
        # puts "COND A";
        do_it    = true;
        ac_count = 1;
        o_c = 'closed';
      elsif cc > cc_count then
        # puts "COND B";
        do_it    = true;
        ac_count = 0; # Starts over every cc++.
        o_c = 'open';
      end

      if do_it then
        $log.d("Process all #{o_c} with cc_count #{cc_count} (#{ids.size} rows)");

        if !data_points.has_key?(cc_count) then
          data_points[cc_count] = {};
        end

        if !data_points[cc_count].has_key?(o_c) then
          data_points[cc_count][o_c] = {};
        end

        data_points[cc_count][o_c] = process_subset(ids);
        cc_count = cc;
        ids.clear;
      end

      ids << id;
    end
  end

  # Do the last segment.
  if ids.length > 0 then
    data_points[cc_count]['open'] = process_subset(ids);
  end

  data_points.each_key do |k1|
    puts data_points[k1];
  end

  inf.close();
  $log.d("Done.");

  outf = File.open('hathi_cic_counts.tsv', 'w');
  outf.puts ['#cic_c', 'oopen', 'oshut', 'sopen', 'sshut', 'avg_ho', 'avg_hs'].join("\t");
  data_points.each_key do |k|
    outf.puts [
               k,
               data_points[k]['open']['tot_acc'],
               data_points[k]['open']['tot_vol'] - data_points[k]['open']['tot_acc'],

               data_points[k]['closed']['tot_acc'],
               data_points[k]['closed']['tot_vol'] - data_points[k]['closed']['tot_acc'],

               data_points[k]['open']['m_per_v'].round(1),
               data_points[k]['closed']['m_per_v'].round(1),
              ].join("\t");
  end

  outf.close();
end
