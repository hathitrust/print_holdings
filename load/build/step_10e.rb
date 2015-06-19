require 'hathilog';
require 'hathidata';
require 'hathidb';
require 'hathiquery';

=begin

Part of step 10.

Edited by mwarin, Dec 17 2013.
Was getting a "Statement nesting level is too deep (likely a bug)"
error when running, using jdbchelper 0.8.

All changed code uses the var subconn.

Jan 27 by The Same, using hathidb and hathiquery, also enumerating the
selects instead of executing them and storing in var.

=end

# This routine accounts for original "source" hathitrust items.  Many items in HT
# do not match anything, even the print holdings of the member who submitted them
# to HT originally.  This routine updates the htitem_htmember_jn table with these
# entries.

def add_source_items_to_htitem_htmember_jn(log)
  db              = Hathidb::Db.new();
  conn            = db.get_conn();
  conn.fetch_size = 10000;
  subconn         = db.get_conn();

  check_sql   = Hathiquery.check_count('holdings_htitem_htmember_jn');
  check_query = conn.prepare(check_sql);
  log.d(check_sql);
  check_query.enumerate do |row|
    log.d("BEFORE: #{row[:c]}");
  end

  hdout = Hathidata::Data.new('builds/current/deposits.txt').open('w');

  # source institution map
  source_h     = Hathiquery.source_map;
  cali_members = Hathiquery.cali_members;

  ### loop through HT items ###
  rowcount   = 0;
  cali_total = 0;
  deposits   = 0;
  cali_hits  = 0;
  other_hits = 0;

  sub_select = subconn.prepare("SELECT member_id FROM holdings_htitem_htmember_jn WHERE volume_id = ?");
  sub_insert = subconn.prepare(
                               %W<
                                INSERT IGNORE INTO 
                                holdings_htitem_htmember_jn 
                                (volume_id, member_id, copy_count) 
                                VALUES 
                                (?, ?, 1)
                                >.join(" ")
                               );

  conn.query("SELECT volume_id, source from holdings_htitem") do |row1|
    rowcount += 1;

    skip = false;
    if row1[:source] == 'UC' then # Cali.
      cali_total += 1;
      sub_select.enumerate(row1[:volume_id]) do |t|
        if cali_members.include?(t[0].strip) then
          skip = true;
          cali_hits += 1;
          break;
        end
      end
    else # Non-Cali.
      member_id = source_h[row1[:source]];
      sub_select.enumerate(row1[:volume_id]) do |t|
        if t[0].strip == member_id then
          skip = true;
          other_hits += 1;
          break;
        end
      end
    end

    unless skip then
      member_id = source_h[row1[:source]];
      deposits += 1;
      sub_insert.execute(row1[:volume_id], member_id)
      outstr = "#{row1[:volume_id]}\t#{member_id}";
      hdout.file.puts(outstr);
    end

    skip = false;
    if ((rowcount % 100000) == 0) then
      log.d("Rowcount:  #{rowcount}...");
      log.d("\tCaliTotal: #{cali_total}");
      log.d("\tCaliHits:  #{cali_hits}");
      log.d("\tOtherHits: #{other_hits}");
      log.d("\tDeposits:  #{deposits}");
    end
  end

  log.d("Final Rowcount:  #{rowcount}...");
  log.d("\tFinal CaliTotal: #{cali_total}");
  log.d("\tFinal CaliHits:  #{cali_hits}");
  log.d("\tFinal OtherHits: #{other_hits}");
  log.d("\tFinal Deposits:  #{deposits}");
  
  log.d(check_sql);
  check_query.enumerate do |row|
    log.d("AFTER: #{row[:c]}");
  end

  hdout.close();
  subconn.close();
  conn.close();
end

if $0 == __FILE__ then
  log   = Hathilog::Log.new();
  log.d("Started");
  add_source_items_to_htitem_htmember_jn(log);
  log.d("Finished");
end
