require 'hathidb';

# Assuming that one table with identical columns but different data exist in 2 databases.
# You want to know if they start the same, and if so, where they start diverging.
# Using some heuristics and perhaps dumb assumptions.

def main 
  db = Hathidb::Db.new();
  d_conn = db.get_conn();
  p_conn = db.get_prod_conn();
  
  table = ARGV.shift;
  d_max = get_max(d_conn, table);
  p_max = get_max(p_conn, table);

  get_row_sql_template = "SELECT * FROM #{table} LIMIT xxx, 1";

  dev_rows  = {};
  prod_rows = {};

  dev_rows[0]  = get_row(d_conn, table, get_row_sql_template, 0);
  prod_rows[0] = get_row(p_conn, table, get_row_sql_template, 0);

  dev_rows[d_max]  = get_row(d_conn, table, get_row_sql_template, 0);
  prod_rows[p_max] = get_row(p_conn, table, get_row_sql_template, 0);

  j = [d_max, p_max].min;

  1.upto(10) do 
    i = j / 2;
    dev_rows[i]  = get_row(d_conn, table, get_row_sql_template, i);
    prod_rows[i] = get_row(p_conn, table, get_row_sql_template, i);
    j = i;
  end

  [dev_rows.keys, prod_rows.keys].flatten.uniq.sort.each do |k|
    puts "dev\t[#{k}]\t#{dev_rows[k]}";
    puts "prod\t[#{k}]\t#{prod_rows[k]}";
    puts "---";
  end
end

def get_max(conn, table)
  res = nil;
  conn.query("SELECT COUNT(*) AS c FROM #{table}") do |row|
    res = row[:c];
  end
  return res;
end

def get_row(conn, table, template, i)
  sql = template.sub('xxx', i.to_s);
  puts sql;
  res = nil;
  conn.query(sql) do |row|
    res = row.to_a.join("\t");
  end
  return res;
end

main() if __FILE__ == $0;
