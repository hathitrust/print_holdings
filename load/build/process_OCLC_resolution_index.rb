require 'hathidb';
require 'hathilog';
require 'hathidata';

# Part of step 02.
# Copied from /htapps/pete.babel/Code/phdb/bin/process_OCLC_resolution_index.rb
# ... and adapted to new regime.

def prune_OCLC_resolution_data(pruned_output, log)
  # get a connection
  db   = Hathidb::Db.new();
  conn = db.get_conn();

  # get oclc hash
  oclc_h = {};
  sql    = "SELECT DISTINCT(oclc) FROM holdings_htitem_oclc";
  log.d(sql);
  c = 0;
  conn.query(sql) do |o|
    c += 1;
    oclc_h[o[0]] = 1;
    if c % 250000 == 0 then
      log.d(c);
    end
  end
  log.d("Sanity check: #{oclc_h.length} oclcs.");

  count      = 0;
  out        = 0;
  no_numbers = 0;
  hdout = Hathidata::Data.new(pruned_output).open('w');

  concordance_group_sql = "SELECT resolved, GROUP_CONCAT(variant) AS ocn_group FROM oclc_concordance GROUP BY resolved";
  # only retain lines with HT oclc numbers
  conn.query(concordance_group_sql) do |row|
    count += 1;

    ocns = row[:ocn_group].split(',');
    ocns << row[:resolved];

    if (ocns.length == 1)
      no_numbers += 1;
      next;
    end

    ocns.each do |ocn|
      if oclc_h.has_key?(ocn.to_i);
        hdout.file.puts(ocns.join(','));
	out += 1;
      end
    end

    if ((count % 100000) == 0)
      log.d("#{count}...");
    end
  end

  log.i("#{no_numbers} lines skipped (no oclc number)");
  log.i("#{out} lines written to #{pruned_output}");

  hdout.close();
  conn.close();
end

## Creates an additional htitem_oclc file
def generate_OCLC_data_for_htitem_oclc(pruned_output, final_output, log)

  db    = Hathidb::Db.new();
  conn  = db.get_conn();
  sql   = "SELECT volume_id FROM holdings_htitem_oclc WHERE oclc = ?";
  query = conn.prepare(sql);
  count = 0;
  hdout = Hathidata::Data.new(final_output).open('w');

  Hathidata.read(pruned_output) do |line|
    count += 1;
    ocns = line.chomp.split(',');
    # get all vol_ids associated with these ocns
    vol_ids = Set.new;
    ocns.each do |ocn|
      query.enumerate(ocn) do |vrow|
        vol_ids.add(vrow[0]);
      end
    end
    # insert vol-oclc pairs
    vol_ids.each do |vid|
      ocns.each do |oc|
        hdout.file.puts "#{vid}\t#{oc}\t1";
      end
    end

    if (count % 10000 == 0)
      log.d("#{count}...");
    end
  end

  hdout.close();
  conn.close();
end

def load_holdings_htitem_oclc_tmp (loadfile, log)
  hdin = Hathidata::Data.new(loadfile);

  if !hdin.exists? then
    log.e("Fail");
    raise "Cannot load #{hdin.path} as it does not exist";
  end

  db   = Hathidb::Db.new();
  conn = db.get_conn();

  qs = [
        "TRUNCATE holdings_htitem_oclc_tmp",
        "LOAD DATA LOCAL INFILE '#{hdin.path}' INTO TABLE holdings_htitem_oclc_tmp",
        "INSERT IGNORE INTO holdings_htitem_oclc SELECT * FROM holdings_htitem_oclc_tmp",
        "TRUNCATE holdings_htitem_oclc_tmp",
       ];

  qs.each do |q|
    log.i(q);
    conn.execute(q);
  end
end

# Runnable part.
if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");

  # These paths will be given to Hathidata objects.
  pruned_output = "builds/current/oclc_in_hathi_variants.txt";
  final_output  = "builds/current/volume_ids_with_oclc_variants.tsv";
  
  log.d("Pruning.");
  prune_OCLC_resolution_data(pruned_output, log);
  
  log.d("Generating.");
  generate_OCLC_data_for_htitem_oclc(pruned_output, final_output, log);
  
  log.d("Loading.");
  load_holdings_htitem_oclc_tmp(final_output, log);

  log.d("Finished");
end
