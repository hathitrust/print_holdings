require 'hathidb';
require 'hathilog';

def process_oclc (oclc)
  copies = []; # Non-cic members who have 1+ copy
  access = []; # Non-cic members who have 1+ open copy.

  slice_count = 0;
  max_slice_size = 1000;
  conn = $db.get_conn();

  noof_slices = (oclc.length / max_slice_size) + 1;

  oclc.each_slice(max_slice_size) do |slice|
    slice_count += 1;
    $log.d("Slice #{slice_count} / #{noof_slices} for this segment.");

    q = %Q<
    SELECT
        sum(1) AS copy_count,
        SUM(access_count > 0)
    FROM
        holdings_memberitem_counts
    WHERE
        oclc IN (#{slice.map{|o| "'#{o}'"}.join(',')})
        AND
        member_id NOT IN (
            'uom','wisc','chi','uiuc','minn','ind','osu',
            'iowa','msu','nwu','unl','psu','umd','purd'
        )
    >;

    # puts q; # Gets a bit verbose.

    conn.query(q) do |res|
      copies << res[:copy_count].to_i;
      access << res[:access_count].to_i;
    end
    if (slice_count % 50 == 0) then

      # puts copies.join(", ");
      # puts access.join(", ");

      puts "Count avg  : #{copies.inject(:+)} / #{copies.length} = #{copies.inject(:+).to_f / copies.length}";
      puts "Access avg : #{access.inject(:+)} / #{access.length} = #{access.inject(:+).to_f / access.length}";
      sleep 1;
    end
  end
  conn.close();

  avg = {};
  avg['open']   = access.inject(:+).to_f / access.length;
  avg['closed'] = (copies.inject(:+).to_f / copies.length) - avg['open'];
  avg['total']  = access.length;

  return avg;
end

if $0 == __FILE__ then

  $db  = Hathidb::Db.new();
  $log = Hathilog::Log.new();
  averages = {};

  $log.d("Started");

  # File should look like:
  # cc      ac      oclc
  # 1       0       10000013
  # 1       0       10000023
  # 1       0       100000272

  inf = File.open('step_02_out.tsv', 'r'); # Assuming the file is sorted.

  oclc      = [];
  cic_count = 0;

  inf.each_line do |line|
    line.strip!;
    next if line =~ /cc.*ac.*oclc/; # Skip header line.

    cols = line.split(/\t/);

    if cols.length == 3 then
      cic = cols[0].to_i;
      o   = cols[2];

      if cic_count == 0 then
        cic_count = cic;
      end

      # puts "(#{cic_count})(#{cic})(#{o})";

      if cic > cic_count then
        $log.d("Process all with cic_count #{cic_count} (#{oclc.size} rows)");
        averages[cic_count] = process_oclc(oclc);
        cic_count = cic;
        oclc.clear;
      end

      oclc << o;
    end
  end

  # Do the last segment.
  if oclc.length > 0 then
    averages[cic_count] = process_oclc(oclc);
  end

  inf.close();
  $log.d("Done.");

  outf = File.open('non_cic_average_out.tsv', 'w');
  outf.puts ['#cic_c', 'total', 'avg_op', 'avg_clo'].join("\t");
  averages.each_key do |k|
    outf.puts [
               k,
               averages[k]['total'],
               averages[k]['open'].round(3),
               averages[k]['closed'].round(3)
              ].join("\t");
  end

  outf.close();
end
