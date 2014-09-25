require 'hathidb';
# You have a table and want to know if the rows are sorted by a certain column.

tbl = ARGV.shift;
col = ARGV.shift;
sql = "SELECT #{col} FROM #{tbl}";
puts sql;

conn = Hathidb::Db.new().get_conn();
prev_val = -99999;
i = 0;

conn.enumerate(sql) do |row|
  val = row[col].to_i;
  i += 1;
  if i % 500000 == 0 then
    puts "#{i} ...";
  end
  if val >= prev_val then
    prev_val = val;
  else
    puts "On row #{i} #{val} is less than #{prev_val}";
    break;
  end
end

puts i;
puts "Table #{tbl} is sorted by #{col}";
