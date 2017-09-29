require 'hathidb';
require 'hathidata';

db    = Hathidb::Db.new();
conn  = db.get_conn();
oclcs = [];
hathi = {};

hdf = Hathidata::Data.new('ua_brt_damaged.tsv').open('r');
hdf.file.each_line do |line|
  cols  = line.split("\t");
  match = cols[1].match(/\d+/);
  # puts match[0];
  oclcs << match[0];

  if (oclcs.size > 250 || hdf.file.eof?) then
    sql = "SELECT oclc FROM hathi_files WHERE oclc IN (#{oclcs.join(',')})"
    conn.query(sql) do |row|
      hathi[row['oclc']] = 1;
    end
    oclcs = [];
  end
end
hdf.close();

hdf2 = Hathidata::Data.new('ua_all_oclc_typestat');
hdf3 = Hathidata::Data.new('ua_all_oclc_typestat_brt').open('w');
hdf2.open('r').file.each_line do |line|
  cols = line.split("\t");
  if hathi.has_key?(cols[0]) then
    hdf3.file.puts [line.strip, 'BRT'].join("\t");
  else
    hdf3.file.puts line;
  end
end
hdf3.close();
hdf2.close();
