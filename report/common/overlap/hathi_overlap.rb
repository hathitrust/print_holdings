require 'hathidb';
require 'hathilog';

def main(member_id)
  q = %W<
    SELECT DISTINCT
        h2.oclcs,
        h3.copy_count
    FROM
        holdings_htitem AS h2,
        holdings_htitem_htmember_jn AS h3
    WHERE
        h2.volume_id = h3.volume_id
        AND
        h3.member_id = '#{member_id}'
        AND
        h2.rights IN ('pd', 'pdus')
  >.join(' ');

  o_hash = {};
  o_hash_max = 1000;

  outf = File.open("overlap_#{member_id}_#{Time.new().strftime('%Y%m%d')}.tsv", 'w');
  $log.d("Output will go to #{outf.path}");

  # Store the ones that belong to member and are in hathi_files here,
  # with the count of the member.
  overlap = {};

  $log.d("Running #{q}");

  # Loop through the oclcs and take a thousand (or just over) at a time.
  $conn.query(q).each do |row|
    o  = row[:oclcs];
    cc = row[:copy_count];

    # Split if splittable
    if o =~ /,/ then
      o.split(',').each do |o2|
        o_hash[o2] = cc;
      end
    else
      o_hash[o] = cc;
    end

    # If/when we reach o_hash_max oclcs, run a batch with them.
    if o_hash.size >= o_hash_max then
      $log.d("Process #{o_hash.size} elements.");
      # Merge into the overlap hash the results
      overlap.merge!(process_o(o_hash));
      $log.d("Overlap size is now #{overlap.size}");
      o_hash = {};
    end
  end

  # Make sure to run one more time.
  if o_hash.size > 0 then
    $log.d("Process the last #{o_hash.size} elements.");
    overlap.merge!(process_o(o_hash));
    $log.d("Overlap size is now #{overlap.size}");
  end

  $conn.close();

  # Write results to output file.
  $log.d("Writing output");
  overlap.each_key do |k|
    outf.puts "#{k}\t#{overlap[k]}";
  end
  outf.close();
end

def process_o (hash)
  hits = {};

  # hathi_files contains all digitized books we have.
  # Compare the holdings of the member against these,
  # based on oclc.
  q2 = %Q<
    SELECT DISTINCT oclc
    FROM hathi_files
    WHERE oclc IN (#{hash.keys.map{|o| "'#{o}'"}.join(',')})
  >;

  # For each match, remove it from input hash
  # and put it in the output hash.
  $conn.query(q2).each do |row|
    o = row[:oclc];
    hits[o] = hash.delete(o);
  end

  $log.d("Returning #{hits.size} hits");

  # The output hash will be merged into a bigger hash.
  return hits;
end

if $0 == __FILE__ then
  member_id = nil;

  if ARGV.length > 0 then
    member_id = ARGV.shift;
  else
    puts "Require member_id as 1st arg."
    abort;
  end

  db = Hathidb::Db.new();
  $conn = db.get_conn();
  $log = Hathilog::Log.new();
  main(member_id);
  $log.d("Done");
end
