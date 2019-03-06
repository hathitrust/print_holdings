in_dir="/htapps/mwarin.babel/phdb_scripts/data/shared_print_commitments/phase2/01_received";
out_dir="/htapps/mwarin.babel/phdb_scripts/data/shared_print_commitments/phase2/02_checked";

# get all the files in in_dir, get the associated member_id, and run
# move to checked_dir if successful

for f in "$in_dir/"*; do
    fn=`echo "$f" | grep -Po '[^\/]+$'`
    member_id=`echo "$fn" | grep -Po '^[^_]+'`;
    echo "$fn ($member_id)";
    jruby ingest_phase2.rb $member_id $f && mv -v $f $out_dir/;
done
