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
L=Lost, D=Damaged, E=committed in Error, C=duplicate Copy

Look up matching records in shared_print_commitments and move them over
to shared_print_deprecated and add the status.

=end

class NoDupError     < StandardError; end
class BadStatusError < StandardError; end

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

check_dup_sql = %w{
  SELECT COUNT(DISTINCT local_id) AS c 
  FROM shared_print_commitments
  WHERE member_id=? AND local_oclc=? AND resolved_oclc=?
}.join(' ');

# Read 1st row and make sure it matches spec.
# resolved_oclc is optional.
header_spec = %w{member_id local_oclc resolved_oclc local_id new_status};
spec_ok     = false;
header      = hdin.file.lines.first.strip;
allowed_status = %w{L D E C};

if header == header_spec.join("\t") then
  spec_ok = true;
else
  header_spec.delete("resolved_oclc");
  if header == header_spec.join("\t") then
    spec_ok = true;
    # Remove resolved_oclc from select
    select_sql.sub!(/AND resolved_oclc=\?/, "");
    check_dup_sql.sub!(/AND resolved_oclc=\?/, "");
  end
end
if !spec_ok then
  raise ArgumentError.new(
          "Infile #{infn} header (#{header}) did not match spec."
        );
end

# Set up logfile.
member_id = header.split("\t").first;
# log  = Hathilog::Log.new({:file_name => "#{member_id}_sp_status_update_$ymd.log"});
log  = Hathilog::Log.new();
log.i("Updating shared print status for #{member_id}, using #{infn}");

# Prep queries.
select_q = conn.prepare(select_sql);
insert_q = conn.prepare(insert_sql);
delete_q = conn.prepare(delete_sql);
dups_q   = conn.prepare(check_dup_sql)

i = 1;

hdin.file.each_line do |line|
  line.strip!;
  i+=1;  
  begin # Select rows in shared_print_commitments
    cols       = line.split("\t");
    new_status = cols.pop();
    # Only allow allowed statuses.
    if !allowed_status.include?(new_status) then
      raise BadStatusError.new(
              "Status #{new_status} is not allowed, line #{i}:#{line}"
            );
    end
    # If C=duplicate Copy, make sure there are duplicates before you deprecate.
    if new_status == 'C' then
      # cols[0,3] = [member_id, local_oclc, (resolved_oclc)]
      dups.enumerate(*cols[0,3]) do |row|
        dup_count = row[:c].to_i;
        if dup_count < 2 then
          raise NoDupError.new(
                  "Status is C=duplicate Copy, but #{dup_count} copy/ies found. On input line #{i}\n#{line}";
                );
        end
      end
    end
    match_rows = [];
    select_q.enumerate(*cols) do |row|
      row_h = row.to_h;
      row_h["deprecation_status"] = new_status;
      i.log(row_h);
      match_rows << row_h;
    end

    if match_rows.size == 0 then
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
  rescue BadStatusError => bse
    log.i(bse.message);
  rescue NoDupError => nde
    log.i(nde.message);
  end
end

hdin.close();
