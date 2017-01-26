require 'hathidb';
require 'hathidata';
require 'json';

member_id = ARGV.shift;

db   = Hathidb::Db.new();
conn = db.get_conn();
if ARGV.include?("--brief") then
  # Brief report using --brief
  sql  = %w[
    SELECT spc.resolved_oclc, spc.local_id
    FROM shared_print_commitments AS spc
    WHERE spc.member_id = ?
    ORDER BY spc.local_id
  ].join(" ");

  q = conn.prepare(sql);
  puts [:local_id, :resolved_oclc].join("\t");
  q.enumerate(member_id) do |row|
    puts [row[:local_id], row[:resolved_oclc]].join("\t");
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
    elsif k == 'local_oclc' && !cols.include?('resolved_oclc')
      # We don't actually store their local oclc specifically for SP.
      # Make sure we at least give them the resolved oclc.
      cols << 'resolved_oclc';
    else
      cols << k;
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

  oc_index = cols.index('other_commitment_id');
  if !oc_index.nil? then
    cols[oc_index] = 'other_commitment_id';
  end
  puts cols.join("\t");
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
    puts row.to_a.join("\t");
  end
end
