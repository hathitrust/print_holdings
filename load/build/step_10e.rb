require 'hathilog';
require 'hathidata';
require 'hathidb';
require 'hathiquery';

=begin

Part of step 10.

This routine accounts for original "source" hathitrust items.  Many items in HT
do not match anything, even the print holdings of the member who submitted them
to HT originally.  This routine updates the htitem_htmember_jn table with these
entries.

We look through the Hathifile, record by record:
  "SELECT volume_id, source from holdings_htitem"
and if the volume_id isn't already held (in holdings_htitem_htmember_jn)
then we look up the holdings_htitem.source in a map and assign the volume id to 
that member. If that source map is missing key-value pairs we're going to insert 
null values, and they never make it to prod. Important to keep up to date.

BTW, the sub_insert should probably be rewritten as a LOAD DATA LOCAL INFILE
instead of legion of single inserts.

=end

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

  rowcount   = 0;
  cali_total = 0;
  deposits   = 0;
  cali_hits  = 0;
  other_hits = 0;

  sub_select = subconn.prepare("SELECT member_id FROM holdings_htitem_htmember_jn WHERE volume_id = ?");
  sub_insert = subconn.prepare(
                               %w<
                                INSERT IGNORE INTO 
                                holdings_htitem_htmember_jn 
                                (volume_id, member_id, copy_count) 
                                VALUES 
                                (?, ?, 1)
                                >.join(" ")
                               );

  ### loop through HT items ###
  conn.query("SELECT volume_id, source from holdings_htitem") do |row1|
    rowcount += 1;

    skip = false;
    if row1[:source] == 'UC' then # Cali.
      cali_total += 1;
      # If the source is UC then check if a UC system member is holding it      
      # (according to sub_select)
      sub_select.enumerate(row1[:volume_id]) do |t|
        if cali_members.include?(t[0].strip) then
          skip = true;
          cali_hits += 1;
          break;
        end
      end
    else # Non-Cali.
      # Else look up source-to-memberid and see if that member is holding it
      # (according to sub_select)
      member_id = source_h[row1[:source]];
      sub_select.enumerate(row1[:volume_id]) do |t|
        if t[0].strip == member_id then
          skip = true;
          other_hits += 1;
          break;
        end
      end
    end

    # If we didn't set skip to true, by satisfying one of the conditions above,
    # look up source-to-memberid and insert pair of member_id and volume_id
    # with sub_insert.
    unless skip then
      member_id = source_h[row1[:source]];

      if member_id == '' then
        log.w("Could not map source to member id for source '#{row1[:source]}', volume_id #{row1[:volume_id]}");
      else
        deposits += 1;
        sub_insert.execute(row1[:volume_id], member_id)
        outstr = "#{row1[:volume_id]}\t#{member_id}";
        hdout.file.puts(outstr);
      end
    end

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
