require 'hathidb';
require 'hathilog';

=begin

Based on the CIC holdings in step_02_out.tsv, 
checks what the numbers are for HT as a whole.

=end

def process_subset (volume_ids)
  tot_c          = 0; 
  slice_count    = 0;
  max_slice_size = 1000;
  conn           = $db.get_conn();
  noof_slices    = (volume_ids.length / max_slice_size) + 1;

  volume_ids.each_slice(max_slice_size) do |slice|
    slice_count += 1;
    $log.d("Slice #{slice_count} / #{noof_slices} for this segment.");

    q = %Q!
    SELECT 
        COUNT(h3.member_id) AS c
    FROM 
        holdings_htitem AS h2 
        JOIN 
        holdings_htitem_htmember_jn AS h3 
        ON 
        (h2.volume_id = h3.volume_id)
    WHERE
        h2.volume_id IN (#{slice.map{|v| "'#{v}'"}.join(',')})
    !;

    conn.query(q) do |res|
      tot_c += res[:c].to_i;
    end

    if (slice_count % 100 == 0) then
      avg = (tot_c / (slice_count * max_slice_size.to_f)).round(2);
      $log.d("So far: Avg members holding a volume : #{avg}");
      #break;
      sleep 0.25;
    end
  end
  conn.close();

  ret_val = {
    'avg_h'   => (tot_c / volume_ids.size.to_f).round(2),
    'tot_vol' => tot_c,
  };

  $log.d("Final word: Avg members holding a volume : #{ret_val['avg_h']}");
  return ret_val;
end

if $0 == __FILE__ then

  $db  = Hathidb::Db.new();
  $log = Hathilog::Log.new();
  data_points = {};

  $log.d("Started");

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
    puts "#{k1} #{data_points[k1]}";
  end

  inf.close();
  $log.d("Done.");

  outf = File.open('hathi_cic_counts.tsv', 'w');
  outf.puts ['cic_h', 'access', 'hathi_vol', 'hathi_avg_h', ].join("\t");
  data_points.each_key do |k|
    ['open','closed'].each do |oc| 
      outf.puts [
                 k,
                 oc,
                 data_points[k][oc]['tot_vol'],
                 data_points[k][oc]['avg_h'],
                ].join("\t");
    end
  end
  outf.close();
end
