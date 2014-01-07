require 'hathidb';
require 'hathilog';
require 'hathidata';

db   = Hathidb::Db.new();
conn = db.get_conn();
log  = Hathilog::Log.new();
hdf  = Hathidata::Data.new('missing_duke_volids.txt')

if hdf.exists? then
  log.d("#{hdf.path} already exists, skip this.");
else
  hdf.open('w');

  q1 = %W<
  SELECT 
    d.volume_id 
  FROM 
    mwarin_ht.holdings_htitem_htmember_jn_dec AS d 
  LEFT JOIN 
    ht_repository.holdings_htitem_htmember_jn AS p 
  ON 
    (d.volume_id = p.volume_id) 
  WHERE 
    d.member_id  = 'duke' 
    AND 
    p.member_id  IS NULL
  >.join(' ');

  conn.query(q1) do |res|
    hdf.file.puts res['volume_id']
  end
  hdf.close();
end

counters = {
  :holdings_htitem_htmember_jn => 0,
  :hathi_files                 => 0,
  :holdings_cluster_oclc       => 0,
};

Hathidata::Data.new('missing_dukes/q2').touch();
Hathidata::Data.new('missing_dukes/q3').touch();

volids = [];
j = 0;
hdf.open('r').file.each_line do |line|
  line.strip!;
  volids << line
  if volids.size >= 100 || hdf.file.eof? then
    j += 1;
    log.d(j);

    volids_joined = volids.map{|x| "'#{x}'"}.join(', ');

    q2log = Hathidata::Data.new('missing_dukes/q2').open('a');
    q2 = %W<
        SELECT volume_id, member_id, copy_count FROM ht_repository.holdings_htitem_htmember_jn
        WHERE volume_id IN (#{volids_joined})
    >.join(' ');
    log.d(q2);
    conn.query(q2) do |res|
      q2log.file.puts "volume_id:#{res[:volume_id]} | member_id:#{res[:member_id]} | copy_count:#{res[:copy_count]}";
      counters[:holdings_htitem_htmember_jn] += 1;
    end
    q2log.close();

    q3log = Hathidata::Data.new('missing_dukes/q3').open('a');
    q3 = %W<
    SELECT htid, oclc, issn, bib_format FROM hathi_files
    WHERE htid IN (#{volids_joined})
    >.join(' ');
    log.d(q3);
    conn.query(q3) do |res|
      q3log.file.puts "volume_id:#{res[:htid]} | oclc:#{res[:oclc]} | issn:#{res[:issn]} | bib_format:#{res[:bib_format]}";
      counters[:hathi_files] += 1;
    end
    q3log.close();

    volids = [];
  end
end

oclcs = [];
q3log = Hathidata::Data.new('missing_dukes/q3')
q4log = Hathidata::Data.new('missing_dukes/q4');

if q4log.exists? then
  log.d("#{q4log.path} already exists, skipping step.")
else
  q3log.open('r');
  q4log.open('w');
  q3log.file.each_line do |q3line|  
    oclcs << q3line[/oclc:(\d+)/, 1];
    if oclcs.size >= 100 || q3log.file.eof? then
      

      oclcs_joined = oclcs.map{|x| "'#{x}'"}.join(', ');
      q4 = %W<
      SELECT cluster_id, oclc 
      FROM holdings_cluster_oclc
      WHERE oclc IN (#{oclcs_joined})
    >.join(' ');

      log.d(q4);
      conn.query(q4) do |res|
        q4log.file.puts "#{res[:oclc]} | #{res[:cluster_id]}"
        counters[:holdings_cluster_oclc] += 1;
      end
      oclcs = [];
    end
  end
  q4log.close();
  q3log.close()
end
puts counters;

