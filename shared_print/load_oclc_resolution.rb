require 'hathidata';
require 'hathidb';

=begin

Lines in infile look like:

1: 6567842 | 9987701 | 53095235 | 433981287<br>
2: 9772597 | 35597370 | 60494959 | 813305061 | 823937796<br>
3: 7124033 | 10654585 | 14218190<br>
...

=end

digits = Regexp.new(/\d+/);
hdin   = Hathidata::Data.new('x2_oclc/x2.all').open('r');
hdout  = Hathidata::Data.new('load_oclc_resolution.tsv').open('w');
i = 0;
hdin.file.each_line do |line|
  ids = line.scan(digits).map{|x| x.to_i}.sort.uniq;                          
  i += 1;
  puts i if i % 500000 == 0;
  oclc_x = ids.first;
  ids.each do |oclc_y|
    # turn infile line "3: 7124033 | 10654585 | 14218190"
    # into outfile lines:
    # "3\t3"
    # "3\t7124033"
    # "3\t10654585"
    # "3\t14218190"    
    hdout.file.puts("#{oclc_x}\t#{oclc_y}");
  end
end
hdout.close();
hdin.close();

db = Hathidb::Db.new();
conn = db.get_conn();

conn.execute("TRUNCATE TABLE oclc_resolution");

load_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE oclc_resolution (oclc_x, oclc_y)";
load_q = conn.prepare(load_sql);
load_q.execute(hdout.path);

=begin

In the end you have a table for which you can ask "what oclc number x does y resolve to"
SELECT oclc_x FROM oclc_resolution WHERE oclc_y = '23323184';
If you get NULL back then there is no alias for that number.

=end
