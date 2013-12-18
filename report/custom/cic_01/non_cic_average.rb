require 'hathidb';
require 'hathilog';

def process_vol_ids (vol_ids, access)
  counts = [];
  slc = 0;
  conn = $db.get_conn();

  noof_slices = (vol_ids.length / $max_vol_ids) + 1;

  vol_ids.each_slice($max_vol_ids) do |slice|
    slc += 1;
    $log.d("Slice #{slc} / #{noof_slices} for this segment.");

    q = %Q<
    SELECT 
    SUM(h3.copy_count) AS cc
    FROM
    holdings_htitem AS h2,
    holdings_htitem_htmember_jn AS h3
    WHERE
    h2.volume_id = h3.volume_id
    AND
    h3.member_id NOT IN (#{$cic_members.map{ |m| "'#{m}'" }.join(',')})
    AND
    h2.item_type = 'mono'
    AND
    h2.access = '#{access}'
    AND
    h2.volume_id IN (#{slice.map{|v| "'#{v}'"}.join(',')})
    >;

    conn.query(q) do |res|
      counts << res[:cc].to_i;
    end
    if (slc % 25 == 0) then
      $log.d("Avg so far: #{counts.inject(:+) / counts.length}");
    end
  end
  conn.close();
  avg = counts.inject(:+) / counts.length;

  return avg;
end

if $0 == __FILE__ then
  $cic_members = %w[chi ind iowa minn msu nwu osu psu purd uiuc umd unl uom wisc];

  $max_vol_ids = 1000;

  $db   = Hathidb::Db.new();
  $log = Hathilog::Log.new();
  $open_avg   = {};
  $closed_avg = {};

  $log.d("Started");

  inf = File.open('cic_condensed.tsv', 'r'); # Assuming the file is sorted.

  vol_ids     = [];
  cic_count   = 0;

  inf.each_line do |line|
    line.strip!;

    cols = line.split(/\t/);

    if cols.length == 3 then
      cic = cols[0].to_i;
      vid = cols[2];

      if cic_count == 0 then
        cic_count = cic;
      end

      if cic > cic_count then
        $log.d("Process all with cic_count #{cic}");
        $open_avg[cic_count]   = process_vol_ids(vol_ids, 'allow');
        $closed_avg[cic_count] = process_vol_ids(vol_ids, 'deny');
        cic_count = cic;
        vol_ids.clear;
      end

      vol_ids << vid;
    end
  end

  # Do the last segment.
  if vol_ids.length > 0 then
    $open_avg[cic_count]   = process_vol_ids(vol_ids, 'allow');
    $closed_avg[cic_count] = process_vol_ids(vol_ids, 'deny');
  end

  inf.close();
  $log.d("Done.");

  $open_avg.each_key do |k|
    puts "Open\t#{k}\t#{$open_avg[k]}";
  end
  $closed_avg.each_key do |k|
    puts "Closed\t#{k}\t#{$closed_avg[k]}";
  end
end
