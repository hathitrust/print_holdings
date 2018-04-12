require 'hathidb';
require 'hathidata';
require 'hathilog';

# https://tools.lib.umich.edu/jira/browse/HT-236
# Given a member_id, list all holdings that have a matching commitment in shared print.

def setup
  @member_id = ARGV.shift; # || raise "Need member_id as arg.";
  db = Hathidb::Db.new();
  @conn  = db.get_conn();
  @hdout = Hathidata::Data.new("reports/shared_print/holdings_matching_sp_#{@member_id}_$ymd.tsv");
  @log   = Hathilog::Log.new();

  @qmarks_magic_number = 50;
  
  get_ph_overlap_sql = %W[
   SELECT COUNT(DISTINCT hm.member_id) AS ph_overlap_count
   FROM holdings_memberitem AS hm
   WHERE hm.oclc IN (
     #{(['?'] * @qmarks_magic_number).join(',')}
   )
 ].join(' ');
 @get_ph_overlap_q = @conn.prepare(get_ph_overlap_sql);

 get_variant_oclcs_sql = "SELECT DISTINCT oclc_y FROM oclc_resolution WHERE oclc_x = ?";
 @get_variant_oclcs_q  = @conn.prepare(get_variant_oclcs_sql);
end

def shutdown
  @hdout.close();
  @conn.close();
end

def main

  # join against hco if you want to make sure we are only talking about things in HT:
  # JOIN holdings_cluster_oclc AS hco ON (hco.oclc = COALESCE(o.oclc_x, hm.oclc))
  sql = %w[
    SELECT hm.local_id, hm.gov_doc, hm.item_condition, hm.oclc AS local_oclc, COALESCE(o.oclc_x, hm.oclc) AS resolved_oclc
    FROM holdings_memberitem AS hm
    LEFT JOIN oclc_resolution AS o ON (hm.oclc = o.oclc_y)
    WHERE hm.item_type = 'mono'
    AND hm.status NOT IN ('LM', 'WD')
    AND hm.member_id = ?
    LIMIT 0, 100
  ].join(' ');

  q = @conn.prepare(sql);
  q.enumerate(@member_id) do |row|

    variant_oclcs = [row[:resolved_oclc]];
    @get_variant_oclcs_q.enumerate(row[:resolved_oclc]) do |o_row|
      variant_oclcs << o_row[:oclc_y]
    end
    variant_oclcs += ([nil] * (@qmarks_magic_number - variant_oclcs.count));
    ph_overlap = get_ph_overlap(variant_oclcs);
    out_row = row.to_a;
    out_row << ph_overlap;
    puts out_row.join("\t");
  end

end

def get_ph_overlap (variant_oclcs)
  @get_ph_overlap_q.enumerate(*variant_oclcs) do |row|
    return row[:ph_overlap_count];
  end
end

if $0 == __FILE__ then
  setup();
  main();
  shutdown();
end


=begin

desc holdings_memberitem;
+----------------+-------------------------------+------+-----+---------+
| Field          | Type                          | Null | Key | Default |
+----------------+-------------------------------+------+-----+---------+
| oclc           | bigint(20)                    | NO   | MUL | NULL    |
| local_id       | varchar(50)                   | NO   |     | NULL    |
| member_id      | varchar(20)                   | NO   | MUL | NULL    |
| status         | enum('','CH','LM','WD')       | YES  |     | NULL    |
| item_condition | enum('','BRT')                | YES  |     | NULL    |
| item_type      | enum('mono','multi','serial') | YES  |     | NULL    |
| gov_doc        | tinyint(1)                    | YES  |     | NULL    |
+----------------+-------------------------------+------+-----+---------+

desc shared_print_commitments;
+-----------------------+---------------+------+-----+---------+
| Field                 | Type          | Null | Key | Default |
+-----------------------+---------------+------+-----+---------+
| member_id             | varchar(20)   | NO   | MUL | NULL    |
| resolved_oclc         | bigint(20)    | NO   | MUL | NULL    |
+-----------------------+---------------+------+-----+---------+

desc oclc_resolution;
+--------+------------+------+-----+---------+
| Field  | Type       | Null | Key | Default |
+--------+------------+------+-----+---------+
| id     | int(11)    | NO   | PRI | NULL    |
| oclc_x | bigint(20) | NO   | MUL | NULL    |
| oclc_y | bigint(20) | NO   | MUL | NULL    |
+--------+------------+------+-----+---------+

=end
