require 'hathidb';
require 'hathidata';
require 'json';

member_id = ARGV.shift;
db        = Hathidb::Db.new();
conn      = db.get_conn();
hdout     = Hathidata::Data.new("reports/shared_print/commitment_report_#{member_id}_$ymd.tsv").open('w');

if ARGV.include?("--brief") then
  # Brief report using --brief
  sql  = %w[
    SELECT local_id, local_oclc, resolved_oclc
    FROM shared_print_commitments
    WHERE member_id = ?
    ORDER BY local_id
  ].join(" ");

  q = conn.prepare(sql);
  hdout.file.puts [:local_id, :local_oclc, :resolved_oclc].join("\t");
  q.enumerate(member_id) do |row|
    hdout.file.puts [row[:local_id], row[:local_oclc], row[:resolved_oclc]].join("\t");
  end
else
  # Default to full report, using member profile to make it
  # look as much as possible as the commitments file
  profile = Hathidata::Data.new("shared_print_profiles/#{member_id}.json");
  profile_json = [];
  profile_data = {};
  if profile.exists? then
    profile.open('r');
    profile.file.each_line do |line|
      profile_json << line.strip;
    end
    profile.close();
    profile_data = JSON.parse(profile_json.join(' '));
  end
  cols = [];
  profile_data.sort_by{|k,v| v}.each do |k,v|
    if k == 'other_commitments' then
      # Values stored in another table, so we have to do some tricks with this one.
      cols << 'other_commitment_id';
    else
      cols << k;
    end

    if !cols.include?('resolved_oclc')
      cols << 'resolved_oclc';
    end
  end

  main_sql = %W<
    SELECT #{cols.join(", ")}
    FROM shared_print_commitments
    WHERE member_id = ?
  >.join(' ');

  oc_sql = %w<
    SELECT sp_program, retention_date
    FROM shared_print_other
    WHERE id = ?
  >.join(' ');

  main_q = conn.prepare(main_sql);
  oc_q   = conn.prepare(oc_sql);

  # Need to know which col, if any, has other_commitments in it.
  oc_index = cols.index('other_commitment_id');

  hdout.file.puts(cols.join("\t").sub('other_commitment_id', 'other_commitments'));
  main_q.enumerate(member_id) do |row|
    out = row.to_a;
    if !oc_index.nil? then
       # Trick to get other commitments.
      oc_id = row[oc_index];
      ocs = [];
      oc_q.enumerate(oc_id) do |row_oc|
        ocs << row_oc.to_a.join(' : ');
      end
      row[oc_index] = ocs.join(' ; ')
    end
  hdout.file.puts(row.to_a.join("\t"));
  end
end
hdout.close();
