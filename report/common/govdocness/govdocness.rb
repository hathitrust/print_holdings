require 'hathidb';
require 'hathidata';

# List holdings_memberitems where different members give the same OCLC number different gov_doc status.
# Output example:
# 899676599 is 20.0% (1 vs 4) govdoc
# This means 1 member says 899676599 is a govdoc, and 4 members say it hain't.

db   = Hathidb::Db.new();
conn = db.get_conn();

get_oclc_sql     = "SELECT DISTINCT oclc FROM holdings_memberitem WHERE gov_doc = '1'";

get_deets_sql    = "SELECT DISTINCT oclc, member_id, gov_doc FROM holdings_memberitem WHERE oclc = ? AND gov_doc IS NOT NULL";
get_deets_q      = conn.prepare(get_deets_sql);

get_minority_sql = "SELECT DISTINCT member_id, local_id, item_type, gov_doc FROM holdings_memberitem WHERE oclc = ? AND gov_doc = ? ORDER BY member_id";
get_minority_q   = conn.prepare(get_minority_sql);

# Keep track of how many false pos/neg per member.
member_tendency = {};

hdout = Hathidata::Data.new("controversial_govdocs_$ymd.txt").open('w');
conn.query(get_oclc_sql) do |outer_row|
  oclc = outer_row[:oclc].to_s;
  is   = 0;
  isnt = 0;
  get_deets_q.enumerate(oclc) do |inner_row|
    if inner_row[:gov_doc].to_i == 1 then
      is += 1;
    else
      isnt += 1;
    end
  end
  # A percentage of how many think it should have gov_doc=1.
  # If either side has 2/3 majority, list the minority votes.
  percentage = 100 * (is.to_f / (is + isnt));
  has_majority  = false;
  majority_vote = -9;
  minority_vote = -9;
  if percentage < 35  then
    has_majority  = true;
    majority_vote = 0;
    minority_vote = 1;
  elsif percentage > 65 then
    has_majority  = true;
    majority_vote = 1;
    minority_vote = 0;
  end
  hdout.file.puts("#{oclc} is #{percentage.round(2)}% (#{is} is vs #{isnt} isnt) govdoc.");
  if has_majority then
    hdout.file.puts("majority is #{majority_vote}. change #{minority_vote} into #{majority_vote}");
    get_minority_q.enumerate(oclc, minority_vote) do |minority_row|
      member_id = minority_row[:member_id];
      # change 0 to 1 = false neg
      # change 1 to 0 = false pos
      member_tendency[member_id] ||= {};
      neg_pos = minority_vote == 0 ? :false_neg : :false_pos;
      member_tendency[member_id][neg_pos] ||= 0;
      member_tendency[member_id][neg_pos] +=  1;      
      hdout.file.puts("Change govdoc #{minority_vote} to #{majority_vote} in: #{minority_row.to_a.join("\t")}");
    end
  end
end

member_tendency.keys.sort.each do |member_id|
  member_tendency[member_id].keys.sort.each do |neg_pos|
    hdout.file.puts("-- #{member_id}\t#{neg_pos}\t#{member_tendency[member_id][neg_pos]}");
  end
end

hdout.close();
