# Objective: Figure out if the shift in H2->H1 for utexas has to do with berkeley/nrlf

# First pass, get out all volume_ids held by utexas, put in utexas_ids
# Second pass, for each volume_id in utexas_ids, get all member_ids holding it.

# Needs at least -J-Xmx8000m

require 'hathidata';
require 'hathidb';
require 'hathilog';

log = Hathilog::Log.new();

insert_rx        = Regexp.new(/^INSERT INTO `holdings_htitem_htmember_jn`/);
scan_utexas_rx   = Regexp.new(/\('[^']+','utexas',\d+,\d+,\d+,\d+,\d+\)/);
scan_any_rx      = Regexp.new(/\('[^']+','[^']+',\d+,\d+,\d+,\d+,\d+\)/);
get_volume_id_rx = Regexp.new(/^\('([^']+)/);
get_member_id_rx = Regexp.new(/^\('[^']+','([^']+)/);

runs_month_file = {};
runs_month_file['nov'] = '/htapps/mwarin.babel/phdb_scripts/data/sql/backup_holdings_htitem_htmember_jn_2016-11-30.sql';
runs_month_file['dec'] = '/htapps/mwarin.babel/phdb_scripts/data/sql/backup_holdings_htitem_htmember_jn_2017-01-03.sql';
other_member_syms = [:berkeley, :nrlf];

runs_month_file.each do |month,infile|
  utexas_ids = {}; # Hopefully this actually clears the memory.
  # 1st pass
  i = 0;
  Hathidata.read(infile) do |line|
    if line =~ insert_rx then
      i += 1;
      if i % 10 == 0 then
        log.i("1: #{i}");
      end
      line.scan(scan_utexas_rx).each do |utexas_row|
        volume_id = utexas_row.scan(get_volume_id_rx)[0].first;
        utexas_ids[volume_id] = true;
      end
    end
  end
  first_pass_size = utexas_ids.keys.size;
  puts first_pass_size;

  # 2nd pass
  i = 0;
  Hathidata.read(infile) do |line|
    if line =~ insert_rx then
      i += 1;
      if i % 10 == 0 then
        log.i("2: #{i}");
      end
      line.scan(scan_any_rx).each do |any_row|
        volume_id = any_row.scan(get_volume_id_rx)[0].first;
        member_id_str = any_row.scan(get_member_id_rx)[0].first;
        member_id = member_id_str.to_sym;
        # puts "#{volume_id} #{member_id}"
        if utexas_ids.has_key?(volume_id) then
          if utexas_ids[volume_id].class == TrueClass || utexas_ids[volume_id].class == FalseClass then
            utexas_ids[volume_id] = [];
          end
          utexas_ids[volume_id] << member_id;
          # puts utexas_ids[volume_id].sort.join(', ');
          if utexas_ids[volume_id].size > 2 then
            # puts "delete!";
            utexas_ids.delete(volume_id);
          end
        end
      end
    end
  end
  second_pass_size = utexas_ids.keys.size;

  db   = Hathidb::Db.new();
  conn = db.get_conn();

  puts "#{first_pass_size} ... #{second_pass_size}";
  qs = {};
  # BEST QUERY EVER
  other_member_syms.map{|x| x.to_s}.each do |other_member|
    col = "#{other_member}_#{month}";
    utex_col = "utexas_#{month}";
    sql = %W<
      INSERT INTO investigate_utexas_tmp (volume_id, #{utex_col}, #{col}) VALUES (?, 1, 1)
      ON DUPLICATE KEY UPDATE #{col} = VALUES(#{col})
    >.join(" ");
    puts "Prepping #{sql}";
    qs[col] = conn.prepare(sql);
  end

  i = 0;
  # insurpderting
  utexas_ids.keys.each do |volume_id|
    other_member_syms.each do |o_sym|
      if utexas_ids[volume_id].include?(o_sym) then
        qs["#{o_sym.to_s}_#{month}"].execute(volume_id);
        i += 1;
      end      
    end
  end
  log.i("insurpderped #{i} times");
end
