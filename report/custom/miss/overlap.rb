require 'hathidata';
require 'hathidb';
require 'hathilog';

def main(member_id, log)
  db   = Hathidb::Db.new();
  conn = db.get_conn();
  q    = %W<
    SELECT
        h1.rights,
        h2.oclcs,
        h2.enum_chron,
        h3.copy_count
    FROM
        hathi_files                 AS h1,
        holdings_htitem             AS h2,
        holdings_htitem_htmember_jn AS h3,
        mwarin_ht.oclc_miss         AS om
    WHERE
        h1.htid      = h2.volume_id
        AND 
        h2.volume_id = h3.volume_id
        AND
        h3.member_id = '#{member_id}'
        AND
        om.oclc = h2.oclcs
  >.join(' ');

  rights = {};
  [1, 7, (9 .. 15).to_a, 17].flatten.map{|x| rights[x] = 'allow'};
  [2, 5, 8].map{|x| rights[x] = 'deny'};
  rights[3] = 'op';

  # Dictionary for rights attributes, 'ic' => 'deny' etc.
  rights_name_to_access = {};
  conn.query("SELECT name, id FROM ht_rights.attributes") do |row|
    rights_name_to_access[row[:name]] = rights[row[:id]];
    puts "#{row[:name]} => #{row[:id]}";
  end


  log.d("Running #{q}");
  i = 0;
  cols = [:oclcs, :enum_chron, :copy_count];
  # Write results to this output file.
  Hathidata.write("#{member_id}_overlap.tsv") do |hdout|
    conn.query(q).each do |row|
      i += 1;
      if i % 100000 == 0 then
        log.d(i);
      end

      hdout.file.puts [
                       rights_name_to_access[row[:rights]], 
                       cols.map{|c| row[c]}
                      ].flatten.join("\t");
    end
  end
  conn.close();
end

if $0 == __FILE__ then
  log = Hathilog::Log.new();
  log.d("Started");
  main('missouri', log);
  log.d("Finished");
end
