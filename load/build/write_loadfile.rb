require 'hathidata';

memberdata_dir = Hathidata::Data.new('memberdata/');
Dir.chdir(memberdata_dir.path);

found = [];
# Get all HT003 files in all open dirs in data/memberdata/.
Dir.glob(File.join(%w(** **))).each do |x|
  next if x =~ /\/old\//;
  next if x =~ /\.estimate/;
  next if x =~ /~$/;
  if x =~ /HT003_(.+)\.(mono|multi|serial)\.tsv$/ then
    found << "#{$1}\t#{$2}";  
  end  
end

# Put results in data/builds/current/load.tsv
Hathidata.write('builds/current/load.tsv') do |hdout|
  found.sort.each do |x|
    hdout.file.puts x;
  end
end
