require 'hathidata';
require 'hathidb';
require 'hathilog';

=begin

When a shared print member needs to sort of deprecate a commitment,
but we don't want to just delete the record.

Input files must have a header line like this:

member_id  local_oclc   resolved_oclc  local_id  new_status
trump      994682109    994682109      tr.12345  L
trump      994682109    994682109      tr.12346  L
trump      994682109    994682109      tr.12347  L


... where the only deviation allowed is the optional resolved_oclc, so also OK:

member_id  local_oclc  local_id  new_status
trump      994682109   tr.12345  L
trump      994682109   tr.12346  L
trump      994682109   tr.12347  L

The value for new_status can be: L, D or E.
L=lost, D=damaged, E=committed in error

Look up matching records in shared_print_commitments and move them over
to shared_print_deprecated and add the status.

=end

db   = Hathidb::Db.new();
conn = db.get_conn();
infn = ARGV.shift;
hdin = Hathidata::Data.new(infn).open('r');

# Queries
select_sql = %w<
  SELECT * 
  FROM shared_print_commitments 
  WHERE member_id=? AND local_oclc=? AND resolved_oclc=? AND local_id=?
>.join(" ");

insert_sql = %w<
  INSERT INTO shared_print_deprecated
  (id, member_id, local_oclc, resolved_oclc, local_id, local_bib_id, local_item_id, oclc_symbol, 
  local_item_location, local_shelving_type, ht_retention_date, ht_retention_flag, other_commitment_id,
  lending_policy, scanning_repro_policy, ownership_history, deprecation_status)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
>.join(" ");

delete_sql = "DELETE FROM shared_print_commitments WHERE id = ?";

# Read 1st row and make sure it matches spec.
# resolved_oclc is optional.
header_spec = %w{member_id local_oclc resolved_oclc local_id new_status};
spec_ok     = false;
header      = hdin.file.lines.first.strip;

if header == header_spec.join("\t") then
  spec_ok = true;
else
  header_spec.delete("resolved_oclc");
  if header == header_spec.join("\t") then
    spec_ok = true;
    # Remove resolved_oclc from select
    select_sql.sub!(/AND resolved_oclc=\?/, "");
  end
end
if !spec_ok then
  raise "Infile #{infn} header (#{header}) did not match spec."
end

# Set up logfile.
member_id = header.split("\t").first;
log  = Hathilog::Log.new({:file_name => "#{member_id}_sp_status_update_$ymd.log"});
log.i("Updating shared print status for #{member_id}, using #{infn}");

# Prep queries.
select_q = conn.prepare(select_sql);
insert_q = conn.prepare(insert_sql);
delete_q = conn.prepare(delete_sql);

i = 1;
hdin.file.each_line do |line|
  line.strip!;
  i+=1;
  
  # Select rows in shared_print_commitments
  cols       = line.split("\t");
  new_status = cols.pop();
  match_rows = [];
  select_q.enumerate(*cols) do |row|
    row_h = row.to_h;
    row_h["deprecation_status"] = new_status;
    i.log(row_h);
    match_rows << row_h;
  end

  if match_rows.keys.size == 0 then
    log.i("No match on line #{i}:#{line}");
  else  
    match_rows.each do |h|
      # Copy records over to shared_print_deprecated
      log.i("Inserting #{h} into shared_print_deprecated");
      insert_q.execute(*h.values);
      # Delete them from shared_print_commitments
      log.i("Deleting #{h} from shared_print_commitments");
      delete_q.execute(h["id"]);
    end
  end
end

hdin.close();
