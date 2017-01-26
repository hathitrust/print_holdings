require 'hathidata';

infile = ARGV.shift;
header = [];
files  = {}; # One per member in the file
ymd    = infile.match(/\d{8}/).to_s; # get ymd from infile

Hathidata.read(infile) do |line|
  if line =~ /^(#|member_id)/ then
    header << line;
  else
    member_id = line.split("\t").first;
    if !files.has_key?(member_id) then
      outfn = "reports/shared_print/print_holdings_review_#{member_id}_#{ymd}.tsv";
      files[member_id] = Hathidata::Data.new(outfn).open('w');
      header.each do |h_line|
        files[member_id].file.puts(h_line);
      end
    end
    files[member_id].file.puts(line);
  end
end

files.keys.sort.each do |k|
  files[k].close();
end
