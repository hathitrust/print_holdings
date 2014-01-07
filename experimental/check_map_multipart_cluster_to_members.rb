require 'phdb/phdb_multipart_utils';

cids = %w(
5230167
5543629
5543677
5543645
4545579
5367978
);

multi_members_enum = PHDBMultipartUtils.get_multipart_members_list()

cids.each do |cid|
  result_set = PHDBMultipartUtils.map_multipart_cluster_to_members(cid, multi_members_enum)
  result_set.uniq.each do |line|
    puts(line)
  end
end
