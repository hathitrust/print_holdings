require 'hathidb';
require 'hathidata';
require 'hathilog';
require 'hathiquery'

# Copied from
# /htapps/pete.babel/Code/phdb/bin/generate_htitem_htmember_jn_data.rb

# Parses the output from 'multipart_cluster_mapper.rb' (typically a file
# called "cluster_htmember_multi.2013xxxx.data" into a file suitable for
# upload into the htitem_htmember_jn table.  The file is highly redundant;
# we'll let the DB deal with that based on the PK

def parse_cluster_htmember_multi_datafile(infile, out_hhj, log)
  db              = Hathidb::Db.new();
  conn            = db.get_conn();
  members         = [];
  get_members_sql = Hathiquery.get_active_members;
  log.d(get_members_sql)
  conn.query(get_members_sql) do |row|
    members << row[:member_id];
  end
  conn.close();
  log.d("#{members.length} members.");

  linecount = 0;
  Hathidata.write(out_hhj) do |hdout|
    Hathidata.read(infile) do |line|
      linecount += 1;
      if linecount % 200000 == 0 then
        log.d(linecount);
      end
      bits = line.chomp.split("\t");
      if not members.include?(bits[2]) then
        log.w("Problem with member_id '#{bits[2]}'... skipping.");
        next;
      end
      oclc = bits[0].to_i;
      if not oclc.is_a? Integer then
        log.w("Problem with oclc '#{bits[0]}'... skipping.");
        next;
      end
      vol_bits = bits[4].split(",");
      vol_bits.each do |vol|
        counts = bits[5,5].join("\t");
        hdout.file.puts("#{vol}\t#{bits[2]}\t#{counts}");
      end
    end
  end
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  if ARGV.length != 2 then
    log.e("Usage: ruby generate_htitem_htmember_jn_data.rb <infile> <htmj-outfile>");
    exit 1;
  end
  parse_cluster_htmember_multi_datafile(ARGV[0], ARGV[1], log);
  log.d("Finished");
end
