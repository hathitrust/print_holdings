require 'hathidata';
require 'hathidb';
require 'hathilog';

=begin

This is the table we're talking aboot.

create table govdocness (
       oclc BIGINT(20) NOT NULL,
       yea INT NOT NULL DEFAULT 0,
       nea INT NOT NULL DEFAULT 0,
       PRIMARY KEY (oclc)
);

=end

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();

# Wipe clean.
trunc_govdocness_sql = "TRUNCATE TABLE govdocness";
trunc_govdocness_q   = conn.prepare(trunc_govdocness_sql);
trunc_govdocness_q.execute();

# This one is like a 20min query.
get_govdoc_oclcs_sql = "SELECT DISTINCT oclc FROM holdings_memberitem WHERE gov_doc IS NOT NULL ORDER BY oclc"; # LIMIT 0, 10
get_govdocness_sql   = "SELECT gov_doc, COUNT(DISTINCT member_id) AS c FROM holdings_memberitem WHERE gov_doc IS NOT NULL AND oclc = ? GROUP BY gov_doc";
get_govdocness_q     = conn.prepare(get_govdocness_sql);

# Write to file.
hdout = Hathidata::Data.new("govdocness.dat").open('w');
conn.query(get_govdoc_oclcs_sql) do |row1|
  oclc = row1[:oclc].to_i;
  log.d(oclc) if oclc % 10000 == 0;
  counts = {
    0 => {:name => 'nea', :c => 0},
    1 => {:name => 'yea', :c => 0},
  };  
  get_govdocness_q.enumerate(oclc) do |row2|
    gd = row2[:gov_doc].to_i;
    c  = row2[:c].to_i;
    counts[gd][:c] = c;
  end
  if counts[1][:c] + counts[0][:c] > 1 then
    # Skip the records with just one member backing it.
    hdout.file.puts([oclc, counts[1][:c], counts[0][:c]].join("\t"));  
  end
  
end

hdout.close();

# Load file.
load_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE govdocness (oclc, yea, nea)";
load_q   = conn.prepare(load_sql);
load_q.execute(hdout.path);
