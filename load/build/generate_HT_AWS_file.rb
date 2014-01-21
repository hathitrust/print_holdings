require 'hathilog';
require 'hathidb';
require 'hathidata';

# The purpose of this script is to generate the HathiTrust equivalent
# of one of the memberitem flatfiles, so that HT data can be treated
# the same as member data for the purpose of the AWS hadoop calculations.

log = Hathilog::Log.new();
log.d("Started");

count  = 0;
db     = Hathidb::Db.new();
conn   = db.get_conn();
hdout  = Hathidata::Data.new('aws_$ymd.data').open('w');
oclc_h = {};

query = %W<
    SELECT
        ho.oclc,
        ho.volume_id,
        item_type
    FROM
        holdings_htitem_oclc AS ho,
        holdings_htitem      AS h
    WHERE
        h.volume_id = ho.volume_id
>.join(' ');

log.d(query);
conn.fetch_size = 50000;
conn.enumerate(query).each_slice(50000) do |slice|
  slice.each do |row|
    count += 1;
    ocn    = row[0];
    vid    = row[1];
    type   = row[2];
    hdout.file.puts("#{ocn}\t#{vid}\tHATHI\tNull\tNull\tNull\tNull\t#{type}\tNull");
  end
  if count % 500000 == 0 then
    log.d("#{count} ...");
  end
end

conn.close();
hdout.close();

log.d("Finished");
