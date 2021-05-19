require 'hathidb';
require 'hathidata';

# Provide a report to the member about the records they have deprecated.

member_id = ARGV.shift;
hdout = Hathidata::Data.new("reports/shared_print/deprecated_report_#{member_id}_$ymd.tsv").open('w');

db   = Hathidb::Db.new();
conn = db.get_conn();

# Get counts on deprecated for member.
dep_sql = %w{
  SELECT DATE_FORMAT(deprecation_date, "%Y-%m-%d") AS deprecation_date, deprecation_status, local_id, local_oclc, resolved_oclc, COUNT(*) AS deprecated_count
  FROM shared_print_deprecated
  WHERE member_id = ?
  GROUP BY deprecation_date, deprecation_status, local_id, local_oclc, resolved_oclc
  ORDER BY deprecation_date, deprecation_status, resolved_oclc
}.join(' ');
dep_q = conn.prepare(dep_sql);

# Based on deprecated, get commitments for member.
com_sql =  %w{
  SELECT COUNT(*) AS committed_count
  FROM shared_print_commitments
  WHERE member_id = ? AND resolved_oclc = ?
}.join(' ');
com_q = conn.prepare(com_sql);

# Output.
i = 0;
dep_q.enumerate(member_id) do |d_row|
  if i == 0 then # header
    hdout.file.puts(%w[deprecation_date deprecation_status local_id local_oclc resolved_oclc deprecated_count committed_count].join("\t"));
  end
  committed_count = 0;
  com_q.enumerate(member_id, d_row[:resolved_oclc]) do |c_row|
    committed_count = c_row[:committed_count];
  end
  hdout.file.puts((d_row.to_a + [committed_count]).join("\t"));
  i += 1;
end
hdout.close();
puts "#{i} rows in report.";
