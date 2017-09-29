# Given a file with OCLCs, check which of them are in the rights db as OP.

require 'hathidb';
require 'hathidata';
require 'hathilog';

db    = Hathidb::Db.new();
conn  = db.get_conn();
log   = Hathilog::Log.new();
log.set_level(1);

oclcs = [];
q2    = conn.prepare("SELECT id FROM ht_rights.rights_current WHERE attr = 3 AND CONCAT(namespace, '.', id) = ?");
hdf   = Hathidata::Data.new('ua_all_oclc_typestat').open('r');
hit   = Hathidata::Data.new('ua_op_hits').open('w');
hdf.file.each_line do |line|
  next if line.match(/BRT|LM/) == nil;
  cols  = line.split("\t");
  match = cols[0].match(/\d+/);
  oclc  = match[0].gsub(/^0+/, '');
  oclcs << oclc;

  if oclcs.size > 50 || hdf.file.eof? then
    q1 = %W<
            SELECT 
            hchj.volume_id,
            hco.oclc
            FROM 
            ht_repository.holdings_cluster_oclc AS hco 
            JOIN
            ht_repository.holdings_cluster_htitem_jn AS hchj
            ON (hco.cluster_id = hchj.cluster_id)
            WHERE 
            hco.oclc IN (#{oclcs.join(',')})
           >.join(' ');

    log.i(q1);
    conn.query(q1) do |row|
      v_id = row['volume_id'];
      oc   = row['oclc'];
      log.d("\tvolume_id #{v_id} oclc #{oc}");
      q2.execute(v_id) do |row_row| # row_yerboat
        hit.file.puts ("#{oc} -> #{v_id} -> Access!")
      end
    end
    oclcs = [];
  end
end

hit.close();
hdf.close();
