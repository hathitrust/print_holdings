require 'hathidb';
require 'hathilog';
require 'set';

=begin

Part of step 06.

Copied by mwarin from /htapps/pete.babel/Code/phdb/bin/calc_cluster_rights.rb.

Original docstring:
This module implements a rights calculation for clusters.  It just
utilizes the 'access' column of htitem and assigns either '2' or '1'
('in-copyright' and public-domain', respectively) based on the
'allow' or 'deny' indicators for a cluster.  All items must have the
same access indication, otherwise it'll be marked as '0' ('undetermined').

=end

log = Hathilog::Log.new();
log.d("Started");
db   = Hathidb::Db.new();
conn = db.get_conn();

sql1 = "SELECT cluster_id FROM holdings_cluster";

sql2 = %w[
    SELECT hh.access 
    FROM holdings_htitem            AS hh 
    JOIN holdings_cluster_htitem_jn AS hchj 
    ON (hchj.volume_id = hh.volume_id)
    WHERE hchj.cluster_id = ?
  ].join(' ');
query2 = conn.prepare(sql2); 

sql3   = "UPDATE holdings_cluster SET cost_rights_id = ?, last_mod = SYSDATE() WHERE cluster_id = ?";
query3 = conn.prepare(sql3);

citer = 0;
log.d("iterating...");
conn.query(sql1) do |row|
  cid = row[:cluster_id];
  citer += 1;
  # get the access designation of all volumes in a cluster
  # calculate access right designation from results
  accs = Set.new;
  query2.enumerate(cid) do |row|
    accs.add(row[:access]);
  end

  # get appropriate update query
  rid = -1;
  if (accs.length == 0) then
    # shouldn't happen
    log.w("problem: #{cid}, cost rights len = 0");
    next;
  elsif accs.length > 1 then
    # rights discrepancy, assign 0
    rid = 0;
  else
    # consistent rights designations
    if accs.include?('deny') then
      rid = 2;
    elsif accs.include?('allow') then
      rid = 1;
    else
      log.w("problem rights string = #{rstring} (#{cid})");
    end

  end
  if rid < 0 then
    log.f("'-1' rights id, shouldn't happen.");
    exit;
  end

  query3.execute(rid, cid);

  log.d(citer) if (citer % 500000) == 0;
end

conn.close();
log.d("finished updating cluster rights.");
