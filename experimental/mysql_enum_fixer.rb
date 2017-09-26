require 'hathidb';
require 'hathilog';

=begin

Take a tablename and a column name as input.
Replace the column with an ENUM column based on all the values in
the original column.

Test table:

CREATE TABLE enum_test (
  id  INT NOT NULL AUTO_INCREMENT,
  status VARCHAR(20) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO enum_test (status) VALUES ('ok'), ('bad'), ('good'), ('ok'), ('bad'), ('good');

Issue #1:

Make sure you don't mess with any indexes. If the column you are changing to
enum is part of an index, you need to manually drop the index first, then run this script,
and finally add the index back.

mysql> DROP INDEX `PRIMARY` ON holdings_H_counts;
$ ruby /htapps/mwarin.babel/phdb_scripts/experimental/mysql_enum_fixer.rb holdings_H_counts item_type
$ ruby /htapps/mwarin.babel/phdb_scripts/experimental/mysql_enum_fixer.rb holdings_H_counts access
mysql> ALTER TABLE holdings_H_counts ADD PRIMARY KEY (`H_id`,`member_id`,`access`,`item_type`);

Issue #2:

Check the settings for DEFAULT and NULL before and after. They may have changed.

Issue #3:

WTF? The table grew? Westigate!

SELECT (data_length+index_length)/power(1024,3) AS tablesize_gb FROM information_schema.tables WHERE table_schema = 'ht_repository' AND table_name = 'holdings_memberitem'
Table size 24.592518020421267 GB
ALTER TABLE holdings_memberitem ADD e201510161431580 ENUM('','CH','LM','WD') AFTER status
UPDATE      holdings_memberitem SET e201510161431580 = status
ALTER TABLE holdings_memberitem DROP status
ALTER TABLE holdings_memberitem CHANGE e201510161431580 status ENUM('','CH','LM','WD')
SELECT (data_length+index_length)/power(1024,3) AS tablesize_gb FROM information_schema.tables WHERE table_schema = 'ht_repository' AND table_name = 'holdings_memberitem'
Table size 24.862867034971714 GB

=end

$max_distinct_values = 25;
table = ARGV.shift;
col   = ARGV.shift;

if table.nil? || col.nil? then
  raise "Must provide both table (#{table}) and col (#{col})";
end

db     = Hathidb::Db.new();
iconn  = db.get_interactive();
values = [];
log    = Hathilog::Log.new();

# Get the values in the column, to use as basis for the ENUM(x,y,z).
sql_get_distinct_values = "SELECT DISTINCT #{col} FROM #{table} ORDER BY #{col}";
log.d(sql_get_distinct_values);
iconn.query(sql_get_distinct_values) do |row|
  values << row[col];
end

if values.size > $max_distinct_values then
  log.d(values.map{|x| "'#{x}'"}.join("\n"));
  raise "Too many distinct values (#{values.size} > #{$max_distinct_values})";
end

sql_size = %W<
  SELECT (data_length+index_length)/power(1024,3) AS tablesize_gb 
  FROM   information_schema.tables 
  WHERE  table_schema = 'ht_repository' AND table_name = '#{table}'
>.join(' ');

iconn.query(sql_size) do |row|
  log.d(sql_size);
  log.d("Table size #{row[:tablesize_gb]} GB");
end

# Temporary name for the enum col, rename it later.
enum_col  = 'e' + Time.new().to_s.gsub(/\D/, '')[0..14]; # e.g. e201510161246120
enum_vals = values.map{|x| "'#{x}'"}.uniq.join(',');

sqls = [
        "ALTER TABLE #{table} ADD #{enum_col} ENUM(#{enum_vals}) AFTER #{col}",
        "UPDATE      #{table} SET #{enum_col} = #{col}",
        "ALTER TABLE #{table} DROP #{col}",
        "ALTER TABLE #{table} CHANGE #{enum_col} #{col} ENUM(#{enum_vals})"
       ];

sqls.each do |sql|
  log.d(sql);
  iconn.update(sql);
end

iconn.query(sql_size) do |row|
  log.d(sql_size);
  log.d("Table size #{row[:tablesize_gb]} GB");
end

iconn.close();
