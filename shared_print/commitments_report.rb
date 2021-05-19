require 'hathidb';
require 'hathidata';
require 'json';

member_id = ARGV.shift;
mode      = ARGV.shift;
db        = Hathidb::Db.new();
conn      = db.get_conn();
hdout     = Hathidata::Data.new("reports/shared_print/commitment_report_#{member_id}_$ymd.tsv").open('w');

def put_value_in_col (name, cols, ra, x)
  pos = cols.index(name);
  ra[pos] = x[name];
end

case mode
when '--brief'
  # Brief report using --brief
  sql  = %w[
    SELECT local_id, local_oclc
    FROM shared_print_commitments
    WHERE member_id = ?
    ORDER BY local_id
  ].join(" ");

  q = conn.prepare(sql);
  header = true;
  q.enumerate(member_id) do |row|
    if header then
      hdout.file.puts row.to_h.keys.join("\t")
      header = false
    end
    hdout.file.puts row.to_a.join("\t")
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
  cols = ['resolved_oclc', 'committed_date'];
  profile_data.sort_by{|k,v| v}.each do |k,v|
    if k == 'other_commitments' then
      # Values stored in another table, so we have to do some tricks with this one.
      cols << 'other_commitment_id';
    else
      cols << k;
    end
  end
  cols.uniq!

  main_sql = %W<
    SELECT #{cols.join(", ")}
    FROM shared_print_commitments
    WHERE member_id = ?
  >.join(' ');

  puts main_sql;

  oc_sql = %w<
    SELECT sp_program, retention_date, indefinite
    FROM shared_print_other
    WHERE id = ?
  >.join(' ');

  get_augment_fields_sql = %w<
    SELECT callno, lang AS language, pub_year, pub_place AS pub_country
    FROM shared_print_augment
    WHERE member_id = ? AND local_id = ?
  >.join(' ');

  get_augment_overlap_group_sql = %w<
    SELECT COUNT(DISTINCT member_id) AS overlap_group
    FROM shared_print_commitments WHERE resolved_oclc = ?
    AND member_id IN ('brown', 'columbia', 'cornell', 'duke', 'harvard',
    'jhu', 'mit', 'princeton', 'uchicago',  'upenn', 'yale')
  >.join(' ');

  get_augment_overlap_hathi_sql = %w<
    SELECT MAX(hhh.H) AS overlap_ht
    FROM holdings_cluster_oclc AS hco
    JOIN holdings_cluster_htitem_jn AS hchj ON (hco.cluster_id  = hchj.cluster_id)
    JOIN holdings_htitem_H          AS hhh  ON (hchj.volume_id = hhh.volume_id)
    WHERE hco.oclc = ?
  >.join(' ');

  get_from_pool_sql = %w<
    SELECT gov_doc, local_h AS overlap_sp FROM shared_print_pool WHERE member_id = ? AND resolved_oclc = ?
  >.join(' ');
  
  main_q = conn.prepare(main_sql);
  oc_q   = conn.prepare(oc_sql);
  get_augment_fields_q        = conn.prepare(get_augment_fields_sql);
  get_from_pool_q             = conn.prepare(get_from_pool_sql);
  get_augment_overlap_group_q = conn.prepare(get_augment_overlap_group_sql);
  get_augment_overlap_hathi_q = conn.prepare(get_augment_overlap_hathi_sql);

  # Need to know which col, if any, has other_commitments in it.
  oc_index = cols.index('other_commitment_id');

  # Make "augmented file" for columbia and princeton, https://wush.net/jira/hathitrust/browse/HTP-1085
  if mode == '--augment' then
    %w[overlap_ht overlap_sp overlap_group gov_doc language pub_year pub_country callno].each do |x_col|
      cols << x_col;
    end
  end

  hdout.file.puts(cols.join("\t").sub('other_commitment_id', 'other_commitments'));
  main_q.enumerate(member_id) do |row|
    out = row.to_a;

    if !oc_index.nil? then
      # Trick to get other commitments.
      oc_id = row[oc_index];
      ocs = [];
      oc_q.enumerate(oc_id) do |row_oc|
        if row_oc[:indefinite] == 1 then
          ocs << "#{row_oc[:sp_program]} : indefinite";
        else
          ocs << "#{row_oc[:sp_program]} : #{row_oc[:retention_date]}";
        end
      end
      row[oc_index] = ocs.join(' ; ')
    end
    ra = row.to_a;

    if mode == '--augment' then
      # Look up a bunch of extra stuff and add it to the output array ra.
      local_id      = row[:local_id];
      resolved_oclc = row[:resolved_oclc];
      get_augment_fields_q.enumerate(member_id, local_id) do |x|
        put_value_in_col('language', cols, ra, x);
        put_value_in_col('pub_year', cols, ra, x);
        put_value_in_col('pub_country', cols, ra, x);
        put_value_in_col('callno', cols, ra, x)  ;
      end
      get_augment_overlap_hathi_q.enumerate(resolved_oclc) do |x|
        put_value_in_col('overlap_ht', cols, ra, x);
      end
      get_from_pool_q.enumerate(member_id, resolved_oclc) do |x|
        put_value_in_col('overlap_sp', cols, ra, x);
        put_value_in_col('gov_doc', cols, ra, x);
      end
      get_augment_overlap_group_q.enumerate(resolved_oclc) do |x|
        put_value_in_col('overlap_group', cols, ra, x);
      end

    end
    hdout.file.puts(ra.join("\t"));
  end
end

hdout.close();
