require 'phdb/phdb_utils'


# parses the output from 'multipart_cluster_mapper.rb' (typically a file      
#   called "cluster_htmember_multi.2013xxxx.data" into a file suitable for         
#   upload into the htitem_htmember_jn table.  The file is highly redundant;
#   we'll let the DB deal with that based on the PK
def parse_cluster_htmember_multi_datafile(infile, out_hhj) 
  puts "#{Time.new()} START";
  outf2 = File.open(out_hhj, 'w')

  members = PHDBUtils.get_member_list()
  puts "#{members.length} members."
  members.delete('ucm')
  
  linecount = 0
  File.open(infile).each do |line|
    linecount += 1
    if linecount % 200000 == 0 then
      puts "#{Time.new()} #{linecount}";
    end
    bits = line.chomp.split("\t")
    if not members.include?(bits[2])
      puts "Problem with member_id '#{bits[2]}'... skipping."
      next
    end
    oclc = bits[0].to_i
    if not oclc.is_a? Integer
      puts "Problem with oclc '#{bits[0]}'... skipping."
      next
    end
    vol_bits = bits[4].split(",")
    vol_bits.each do |vol|
      counts = bits[5,5].join("\t")
      outstr = "#{vol}\t#{bits[2]}\t#{counts}"
      outf2.puts(outstr)
    end
  end    
  outf2.close
  puts "#{Time.new()} FINISH";
end



if ARGV.length != 2
  puts "Usage: ruby generate_htitem_htmember_jn_data.rb <infile> <htmj-outfile>\n"
  exit
end
parse_cluster_htmember_multi_datafile(ARGV[0], ARGV[1])
