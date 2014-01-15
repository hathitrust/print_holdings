require 'hathilog';
require 'hathienv';
require 'hathidb';

=begin

After 16c, make sure that everything got copied over correctly.
Do this by comparing the counts for each member in dev against
the counts for each member in prod.

=end

exit_code = 1;

log = Hathilog::Log.new();
log.d("Started");

if Hathienv::Env.is_prod? then
  # Same query for both dbs.
  q  = "SELECT member_id, COUNT(*) AS c FROM holdings_htitem_htmember_jn_dev GROUP BY member_id";
  db = Hathidb::Db.new();

  conns = {
    'dev'  => db.get_conn(),
    'prod' => db.get_prod_conn(),
  };

  res = {};

  # Run query once in each db.
  conns.keys.each do |k|
    log.d(k);
    log.d(q);

    conn = conns[k];
    conn.query(q) do |row|
      m = row[:member_id].to_s;
      c = row[:c].to_i;

      next if m.length == 0;

      # Store count as value in hash keyed on [member_id][dev/prod]
      if !res.has_key?(m) then
        res[m] = {
          'dev'  => 0,
          'prod' => 0,
        };
      end

      log.d("#{m} #{k} #{c}");
      res[m][k] = c;
    end
    conn.close();
  end

  # Loop through result hash and compare the stored values for each member.
  good_count = 0;
  bad_count  = 0;

  res.keys.sort.each do |m|
    if res[m]['dev'] == res[m]['prod'] then
      puts "#{m} OK";
      good_count += 1;
    else
      puts "#{m} dev: #{res[m]['dev']}, prod: #{res[m]['prod']}"
      # All is ok if we never end up here.
      bad_count += 1;
    end
  end
  if bad_count == 0 && good_count != 0 then
    exit_code = 0;
    log.d("All is OK: #{good_count} good.");
  else
    log.w("Something is bad: #{good_count} good, #{bad_count} bad.");
  end
else
  log.e("This should only be run in prod.");
end

log.d("Exiting with code #{exit_code}");
log.d("Finished");
exit exit_code;
