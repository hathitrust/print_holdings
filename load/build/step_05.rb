require 'hathidb';
require 'hathilog';

# Martin Warin 2014-01-07.
# Replaces the stored procedure in step 5.
# Mostly for logging and ease of automatization purposes.

def empty_table (db, log, start_with_memberid)
  empty_table_sql = "TRUNCATE holdings_cluster_htmember_jn";
  if start_with_memberid != nil then
    empty_table_sql = %W!
      DELETE FROM 
      holdings_cluster_htmember_jn 
      WHERE 
      member_id >= '#{start_with_memberid}'
    !.join(' ');
  end
  conn = db.get_conn();
  log.d(empty_table_sql);
  empty_table_query = conn.prepare(empty_table_sql);
  empty_table_query.execute();
  conn.close();
end

def check_table (db, log)
  conn = db.get_conn();
  sql = "SELECT COUNT(*) AS c FROM holdings_cluster_htmember_jn";
  log.d("Checking count.");
  log.d(sql);
  conn.query(sql) do |row|
    log.d(row[:c]);
  end
  conn.close();
end

def fill_table (db, log, start_with_memberid)
  conn = db.get_conn();

  get_members_sql = %W<
    SELECT DISTINCT
      member_id
    FROM
      holdings_memberitem
    ORDER BY
      member_id
  >.join(' ');

  insert_member_sql = %W<
  INSERT INTO
    holdings_cluster_htmember_jn (cluster_id, member_id)
    SELECT DISTINCT
      cluster_id,
      member_id
    FROM
      holdings_cluster_oclc,
      holdings_memberitem
    WHERE
      holdings_cluster_oclc.oclc = holdings_memberitem.oclc
      AND
      member_id = ?
  >.join(' ');

  insert_member_query = conn.prepare(insert_member_sql);

  log.d(get_members_sql);
  conn.query(get_members_sql) do |row|
    if start_with_memberid != nil then
      if start_with_memberid > row[:member_id] then
        log.d("Skipping inserts for #{row[:member_id]}");
        next;
      end
    end

    sql_copy = insert_member_sql.sub(/\?/, "'#{row[:member_id]}'");
    log.d(sql_copy);
    insert_member_query.execute(row[:member_id]);

    check_table(db, log);
  end
  conn.close();
end

if __FILE__ == $0 then
  db   = Hathidb::Db.new();
  log  = Hathilog::Log.new();

  log.d("Started.");
  start_with_memberid = nil;
  if ARGV.length > 0 then
    command = ARGV.shift;
    if command[/start_with_memberid=(.+)/] then
      start_with_memberid = $1;
    end
  end

  check_table(db, log);
  empty_table(db, log, start_with_memberid);
  check_table(db, log);
  # Calls check_table after each loop.
  fill_table(db, log, start_with_memberid);

  log.d("Finished.");
  log.close();
end
