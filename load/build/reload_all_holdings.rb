require 'hathidb';
require 'hathidata';
require 'hathilog';

# For when you migrate systems or have had some sort of DB failure
# such as the id column getting too big.

# Set up.
db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
log.i("Started.");

load_sql = %w[
    LOAD DATA LOCAL INFILE ?
    INTO TABLE holdings_memberitem IGNORE 1 LINES
    (oclc, local_id, member_id, status, item_condition,
    process_date, enum_chron, item_type, issn, n_enum, n_chron, gov_doc)
].join(' ');
load_q = conn.prepare(load_sql);

count_sql = "SELECT COUNT(*) AS c FROM holdings_memberitem";
count_q   = conn.prepare(count_sql);

# Wipe clean.
trunc_sql = "TRUNCATE TABLE holdings_memberitem";
log.i(trunc_sql);
trunc_q   = conn.prepare(trunc_sql);
trunc_q.execute();

# Get the relevant files.
loadfiles_dir = Hathidata::Data.new("loadfiles/");
puts loadfiles_dir.path;
cmd = "ls -w1 #{loadfiles_dir.path} | grep '.tsv'";
puts cmd;

# Load one at a time.
%x(#{cmd}).split("\n").each do |f|
  log.i("Load file: [#{f}]");
  loadfile_path = "#{loadfiles_dir.path}#{f}";
  load_q.execute(loadfile_path);
  count_q.enumerate() do |row|
    log.i("Total count so far: #{row[:c]}");
  end
end

log.i("Done.");
