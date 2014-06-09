require 'hathidata';
require 'hathilog';
require 'multipart';

# Part of step 8.
# Exports all lines for a cluster to an outfile (also puts them onscreen)
# useful for debugging

def get_cluster_htmember_multi_data(cluster_id, log)
  log.d("Create cluster dump file for cluster_id #{cluster_id}");
  multi_members_enum = Multipart.get_multipart_members_list();
  data = Multipart.map_multipart_cluster_to_members(cluster_id, multi_members_enum);
  Hathidata.write("cluster_#{cluster_id}") do |hdout|
    data.each do |line|
      puts line;
      hdout.file.puts(line);
    end
  end
end

def generate_cluster_htmember_multi_file(outfilen, log)
  multi_members_enum = Multipart.get_multipart_members_list();
  puts "#{multi_members_enum.length} multipart members.";
  cids = Multipart.get_multipart_cluster_list();
  log.d("#{cids.length} clusters in list.");
  count = 0;
  
  Hathidata.write(outfilen) do |hdout|
    cids.each do |cid|
      result_set = Multipart.map_multipart_cluster_to_members(cid, multi_members_enum);
      # calc unique lines
      result_set.uniq.each do |line|
        # line: oclc, enum, member_id, cluster_id, volume_ids, count
        hdout.file.puts(line);
      end
      count += 1;
      if ((count % 10000) == 0) then
        log.d("#{count}...");
      end
    end
  end
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  if ARGV.length != 1 then
    log.e("Usage: ruby multipart_cluster_mapper.rb <outfile>");
    exit 1;
  end
  # Test:
  # get_cluster_htmember_multi_data(3220320, log);
  generate_cluster_htmember_multi_file(ARGV[0], log);
  log.d("Finished");
end
