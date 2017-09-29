require 'hathilog';
require 'hathidata';
require 'hathidb';

# We want to find out which of the recs stanford marked as govdocs
# are in hathitrust. Output nice report.

datadir = 'memberdata/stanford';
govdocs = [];
files   = %w[
  stanford_multi-part_20140824.tsv
  stanford_serials_20140824.tsv
  stanford_single-part_20140824.tsv
];

# Get govdocs from files.
files.each do |fn|
  Hathidata.read("#{datadir}/#{fn}") do |line|
    line.strip!;
    cols = line.split("\t");
    if cols.last == '1' then
      govdocs << [cols[0], cols[1]];
    end
  end
end

# Print govdocs to data file.
govdocs_data = Hathidata::Data.new('stanford_govdocs_input.tsv').open('w');
govdocs.each do |g|
  govdocs_data.file.puts(g.join("\t"));
end
govdocs_data.close();

# Load data file into temp table.
db       = Hathidb::Db.new();
iconn    = db.get_interactive();
create   = "CREATE TEMPORARY TABLE t_stanford_govdocs (oclc BIGINT(20), local_id VARCHAR(50))";
load_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE t_stanford_govdocs (oclc, local_id)";
iconn.execute(create);
load_query = iconn.prepare(load_sql);
load_query.execute(govdocs_data.path);

# Find out which oclcs in the tmp table exist in hathi.
select_sql = %w[
  SELECT DISTINCT t.oclc, t.local_id, !ISNULL(hho.oclc) AS in_hathi 
  FROM t_stanford_govdocs AS t 
  LEFT JOIN holdings_htitem_oclc AS hho
  ON (t.oclc = hho.oclc)
].join(" ");

# Output.
Hathidata.write('stanford_govdocs_output.tsv') do |hdout|
  hdout.file.puts(%w[oclc local_id in_hathi].join("\t"));
  iconn.query(select_sql) do |row|
    hdout.file.puts(row.to_a.join("\t"));
  end
end
