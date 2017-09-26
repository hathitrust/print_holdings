require 'hathidata';
require 'zlib';

glob_expr = Hathidata::Data.new("builds").path.to_s + "/*/hathi_full.*";

puts glob_expr;

# Read zipped or not
def read_zip_agnostic (path)
  i = 0;
  if path =~ /\.gz$/ then
    Zlib::GzipReader.open(path) do |gzr|
      gzr.each_line do |line|
        yield line;
      end
    end
  else
    Hathidata.read(path) do |line|
      yield line;
    end
  end
end

counts_per_year = {};

# Get all hathifiles from the build directories. They may or may not be gzipped.
Dir.glob(glob_expr).grep(/hathi_full\.data(.gz)?$/).sort.each do |p|
  puts p;
  date_str = p.match(/\d+-\d+-\d+/);
  counts_per_year[date_str] = {:has_oclc => 0, :sans_oclc => 0};
  read_zip_agnostic(p) do |line|
    # Now count number of lines total and number of lines matching criteria.
    cols   = line.split("\t");
    htid   = cols[0];
    access = cols[1];
    source = cols[5];
    oclc   = cols[7];
    if access == 'deny' && source == 'UC' then
      counts_per_year[date_str][oclc =~ /\d/ ? :has_oclc : :sans_oclc] += 1;
    end
  end
  [:sans_oclc, :has_oclc].each do |o|
    puts [date_str, o, counts_per_year[date_str][o]].join("\t");
  end
end
