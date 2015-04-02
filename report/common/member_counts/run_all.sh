log=../../../log/builds/current/membercounts.log;

echo "# member_counts.rb #" > $log;
ruby member_counts.rb      >> $log;

echo "# match_detail.rb #" >> $log;
ruby match_detail.rb       >> $log;

echo "# oclc_matchcount_query.rb  #" >> $log;
ruby oclc_matchcount_query.rb        >> $log;