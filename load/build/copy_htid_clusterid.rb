require 'hathidata';
require 'hathidb';
require 'hathienv';
require 'hathilog';

log = Hathilog::Log.new();
log.i("Started");

db   = Hathidb::Db.new();
conn = db.get_conn(); # Dev conn.

# Step 1
# Copy rows from dev table to file.
# Why? Because this might take a while and we can't assume
# that the dev conn is going to stay open.

hdout = Hathidata::Data.new("builds/current/volume_cluster.tsv").open('w');
count_rows = 0;
get_sql  = %w<
  SELECT t1.cluster_id, t1.volume_id, t3.n_enum
  FROM holdings_cluster_htitem_jn AS t1
  JOIN holdings_cluster AS t2 ON (t1.cluster_id = t2.cluster_id)
  JOIN holdings_htitem AS t3 ON (t1.volume_id = t3.volume_id)
  WHERE t2.cluster_type IN ('spm', 'mpm')
>.join(' ');
get_q = conn.prepare(get_sql);

# Table -> file
get_q.enumerate do |row|
  count_rows += 1;
  hdout.file.puts(row.to_a.join("\t"));
end

hdout.close();
conn.close();

log.i("there are #{count_rows} rows to copy from dev to prod");

# Step 2: clean out the tmp table in prod
# just in case there is any data left from an incomplete run
# create if it does not exist
prod_conn = db.get_prod_interactive(); # Prod conn, prompts for username/password
prod_queries = [
  "CREATE TABLE IF NOT EXISTS holdings_cluster_htitem_jn_tmp LIKE holdings_cluster_htitem_jn",
  "TRUNCATE holdings_cluster_htitem_jn_tmp",
  "ALTER TABLE holdings_cluster_htitem_jn_tmp ENGINE=innodb",
]
prod_queries.each do |sql|
  log.i(sql);
  q = prod_conn.prepare(sql);
  q.execute();
end

# Step 3: load records from file into prod tmp table
# Do a slice at a time until all the rows are exported.
# One million records per slice.
slice_size = 500_000;
slice_seen = 0;
sleeptime  = 0;

load_prod_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE holdings_cluster_htitem_jn_tmp";
load_prod_q = prod_conn.prepare(load_prod_sql)

hdin = Hathidata::Data.new("builds/current/volume_cluster.tsv").open('r');

line_buffer = [];
hdin.file.each_line do |line|
  line.strip!
  line_buffer << line;
  if hdin.file.eof? || line_buffer.size % slice_size == 0 then
    t1 = Time.new;
    slice_seen += slice_size;
    log.i("writing #{line_buffer.size} rows (tot #{slice_seen} of #{count_rows}) to slice file");
    hdslice = Hathidata::Data.new("builds/current/volume_cluster_slice.tsv").open('w');
    line_buffer.each do |buf_line|
      hdslice.file.puts(buf_line);
    end
    hdslice.close();
    line_buffer = [];
    log.i("Loading slice into tmp table");
    load_prod_q.execute(hdslice.path);

    t2 = Time.new;
    # Sleep twice as long as the op took for replication to catch up
    # or at least no less than 2 seconds
    sleeptime = (1 + (t2 - t1).to_i) * 2;
    log.i("Napping #{sleeptime} s");
    sleep(sleeptime);
  end
end

# Step 4: the ol' swapparoo
swap_commands = [
  "DROP TABLE IF EXISTS holdings_cluster_htitem_jn_old",
  "RENAME TABLE holdings_cluster_htitem_jn TO holdings_cluster_htitem_jn_old",
  "RENAME TABLE holdings_cluster_htitem_jn_tmp TO holdings_cluster_htitem_jn",
  "DROP TABLE holdings_cluster_htitem_jn_old",
  "CREATE TABLE holdings_cluster_htitem_jn_tmp LIKE holdings_cluster_htitem_jn",
  "ALTER TABLE holdings_cluster_htitem_jn_tmp ENGINE=innodb",
];

swap_commands.each do |sql|
  log.i("/*PROD:*/ " + sql);
  q = prod_conn.prepare(sql);
  q.execute();
end

log.i("Finished");
