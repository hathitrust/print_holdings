require 'phdb/phdb_utils';

=begin

Edited by mwarin, Dec 17 2013.
Was getting a "Statement nesting level is too deep (likely a bug)"
error when running, using jdbchelper 0.8.

All changed code uses the var subconn.

=end

# This routine accounts for original "source" hathitrust items.  Many items in HT
# do not match anything, even the print holdings of the member who submitted them
# to HT originally.  This routine updates the htitem_htmember_jn table with these
# entries.
def add_source_items_to_htitem_htmember_jn(reportfn)

  conn = PHDBUtils.get_dev_conn();
  conn.fetch_size=10000;
  subconn = PHDBUtils.get_dev_conn();

  # source institution map
  source_h = PHDBUtils.get_source_map();
  cali_members = %w(berkeley ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf);
  #ht_members = PHDBUtils.get_member_list()
  outfile = File.new(reportfn, "w");

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
      test_rows = sub_select.execute(row1[:volume_id]);
      test_rows.each do |t|
        if cali_members.include?(t[0].strip) then
          skip = true;
          cali_hits += 1;
          break;
        end
      end
    else # Non-Cali.
      member_id = source_h[row1[:source]];
      test_rows = sub_select.execute(row1[:volume_id]);
      test_rows.each do |t|
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
      #puts outstr
      outfile.puts(outstr);
    end

    skip = false;
    if ((rowcount % 100000) == 0) then
      puts Time.new();
      puts "\nRowcount:  #{rowcount}...";
      puts "\tCaliTotal: #{cali_total}";
      puts "\tCaliHits:  #{cali_hits}";
      puts "\tOtherHits: #{other_hits}";
      puts "\tDeposits:  #{deposits}";
    end
  end

  puts "\nFinal Rowcount:  #{rowcount}...";
  puts "\tFinal CaliTotal: #{cali_total}";
  puts "\tFinal CaliHits:  #{cali_hits}";
  puts "\tFinal OtherHits: #{other_hits}";
  puts "\tFinal Deposits:  #{deposits}";

  outfile.close();
  subconn.close();
  conn.close();
end

if ARGV.length != 1 then
  abort "Usage: ruby add_source_items_to_htitem_htmember_jn <reportfile>\n";
end

puts "Started #{Time.new()}";
add_source_items_to_htitem_htmember_jn(ARGV[0]);
puts "Finished #{Time.new()}";
