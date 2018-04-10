require 'hathidb';
require 'hathidata';
require 'hathilog';

# https://tools.lib.umich.edu/jira/browse/HT-236
# Given a member_id, list all holdings that have a matching commitment in shared print.

def setup
  @member_id = ARGV.shift || raise "Need member_id as arg.";
  db = Hathidb::Db.new();
  @conn  = db.get_conn();
  @hdout = Hathidata::Data.new("reports/shared_print/holdings_matching_sp_#{@member_id}_$ymd.tsv");
  @log   = Hathilog::Log.new();
end

def shutdown
  @hdout.close();
  @conn.close();
end

def main

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
