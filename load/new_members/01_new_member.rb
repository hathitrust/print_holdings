require 'hathidb';

$db = Hathidb::Db.new();

def simple_select
  c = $db.get_conn();
  q = "select count(*) as access_count from access_stmts";
  c.query(q) do |r|
    puts r[:access_count];
  end
  c.close();
end

simple_select();
