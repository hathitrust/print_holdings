require 'hathidb';
require 'hathilog';
require 'hathidata';

$db  = Hathidb::Db.new();
$log = Hathilog::Log.new();

def generate_report(member_id, stoppers)
  conn = $db.get_conn();
  hdfn = "#{member_id}_all_oclc";

  get_oclcs(member_id, hdfn, conn);
  if stoppers.has_key?('stop_after_get_oclcs') then
    $log.i("Stopped by stopper.");
    return;
  end

  get_overlap(hdfn, conn);
  if stoppers.has_key?('stop_after_get_overlap') then
    $log.i("Stopped by stopper.");
    return;
  end

  get_type_and_status(member_id, hdfn, conn);
  conn.close();
end

# Get all unique OCLC numbers by this member.
# Output to file.
def get_oclcs(member_id, hdfn, conn)
  hdf = Hathidata::Data.new(hdfn);

  if hdf.exists? then
    $log.i("#{hdf.path} already exists, skipping method #{__method__}.");
    return hdf.path;
  elsif hdf.backup? then
    $log.i("Backup of #{overlapf.path} already exists. Skipping method #{__method__}.");
    return hdf.path;
  end

  hdf.open('w');
  sql = "SELECT DISTINCT oclc FROM holdings_memberitem WHERE member_id = ?";
  sth = conn.prepare(sql);

  $log.d(sql.sub('?', member_id));

  count_lines = 0;
  sth.query(member_id) do |row|
    hdf.file.puts row['oclc'];
    count_lines += 1;
    if count_lines % 100000 then 
      $log.d("#{count_lines} oclcs output");
    end
  end
  hdf.close();

  return hdf.path;
end

# Based on the OCLC numbers in file, check which ones are in hathi_files.
# Output to another file.
def get_overlap(hdfn, conn)
  slice_size = 1000;
  oclcs      = [];
  overlapf   = Hathidata::Data.new(hdfn + '_overlap');

  if overlapf.exists? then
    $log.i("#{overlapf.path} already exists, skipping method #{__method__}.");
    return overlapf.path;
  elsif overlapf.backup? then
    $log.i("Backup of #{overlapf.path} already exists, skipping method #{__method__}.");
    return overlapf.path;
  end

  overlapf.open('w');
  hdf = Hathidata::Data.new(hdfn).get_backup_or_open('r');

  slices = 0;
  hdf.file.each_line do |line|
    oclcs << line.strip;
    if (oclcs.size >= slice_size || hdf.file.eof?) then

      slices += 1;
      if slices % 10 == 0 then
        $log.d("get_overlap has done #{slices} slices");
      end

      # Get the rights and oclc of records overlapping between member holdings and hathi_files.
      sql = "SELECT DISTINCT oclc, rights FROM hathi_files WHERE oclc IN (#{oclcs.join(',')})";

      conn.query(sql) do |row|
        # Because some have commas in them, argh.
        row['oclc'].split(',').each do |o|
          overlapf.file.puts "#{o}\t#{row['rights']}";
        end
      end

      oclcs  = [];
    end
  end

  $log.d("get_overlap finished after #{slices} slices");
  overlapf.close();
  hdf.close();

  return overlapf.path;
end

def get_type_and_status(member_id, hdfn, conn)
  typestatf  = Hathidata::Data.new(hdfn + '_typestat');
  if typestatf.exists? then
    $log.i("#{typestatf.path} already exists, skipping method #{__method__}.");
    return typestatf.path;
  elsif typestatf.backup? then
    $log.i("#{typestatf.backup_path()} backup already exists, skipping method #{__method__}.");
    return typestatf.path;
  end

  oclcs     = {};
  max_slice = 1000;
  typestatf.open('w');
  typestatf.file.puts %W(oclc rights item_type status item_condition).join("\t");
  overlapf  = Hathidata::Data.new(hdfn + '_overlap').get_backup_or_open();
  slices    = 0;

  overlapf.file.each_line do |oline|
    oclc, rights = oline.split("\t").map{|x| (x || '').to_s.strip} ;
    oclcs[oclc] ||= [];
    oclcs[oclc]  << rights.strip;

    if oclcs.keys.length >= max_slice || overlapf.file.eof? then
      slices += 1;
      if slices % 10 == 0 then
        $log.d("get_type_and_status done #{slices} slices");
      end

      # puts oclcs;
      sql = %W<
             SELECT DISTINCT oclc, item_type, status, item_condition
             FROM holdings_memberitem
             WHERE member_id = '#{member_id}'
             AND oclc IN (#{oclcs.keys.map{|x| "'#{x}'"}.join(',')})
            >.join(" ");

      # $log.d(sql);
      conn.query(sql) do |row|
        begin
          oclcs[row['oclc'].to_s].each do |right|
            typestatf.file.puts [
                                 row['oclc'],
                                 right,
                                 row['item_type'],
                                 row['status'],
                                 row['item_condition'],
                                ].join("\t");
          end
        rescue StandardError => e
          $log.w("Strange OCLC #{row['oclc']}");
          $log.w(e.to_s);
        end
      end
      oclcs = {};
    end
  end
  $log.d("get_type_and_status finished after #{slices} slices");

  typestatf.close();
end

if $0 == __FILE__ then
  member_id = 'ua';
  if ARGV.size >= 1 then
    member_id = ARGV.shift;
  end

  # You may not want the whole shialabouf, so you can tell it to stop 
  # after a certain method.
  stoppers = {
    'stop_after_get_oclcs'           => false,
    'stop_after_get_overlap'         => false,
    'stop_after_get_type_and_status' => false,
  };

  ARGV.each do |arg|
    if stoppers.has_key?(arg) then
      stoppers[arg] = true;
    end
  end

  stoppers.delete_if {|k,v| !v};
  puts stoppers;
  generate_report(member_id, stoppers);
end
