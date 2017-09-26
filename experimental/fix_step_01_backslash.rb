require 'hathidata';

Hathidata.read('hathi_full_backslashes.data') do |line|
  line.strip!
  bits = line.split("\t").map{|x| x.sub(/\\+$/, '')};
  puts bits.map{|x| "[#{x}]"}.join(' ');
end
