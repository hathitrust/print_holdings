require 'hathilog';
require 'hathidb';
require 'hathidata';

# Based on wustl/report.rb

def get_it (member_id)
  log = Hathilog::Log.new();
  log.d("Started");

  db   = Hathidb::Db.new();
  conn = db.get_conn();

  q = %w<
    SELECT DISTINCT
      hm.oclc,
      hm.enum_chron,
      hh.access,
      hh.rights,
      hmc.copy_count,
      hhh.h
    FROM
      holdings_memberitem AS hm
    JOIN
      holdings_htitem_oclc AS hho ON (hho.oclc = hm.oclc)
    JOIN
      holdings_htitem AS hh ON (hh.volume_id = hho.volume_id)
    JOIN
      holdings_htitem_H hhh ON (hh.volume_id = hhh.volume_id)
    JOIN
      holdings_memberitem_counts hmc ON (hmc.oclc = hm.oclc)
    WHERE
      hm.member_id = ?
      AND
      hmc.member_id = hm.member_id
  >.join(" ");

  log.d(q);

  cols = [
          :oclc,
          :enum_chron,
          :access,
          :rights,
          :copy_count,
          :h
         ];

  Hathidata.write("overlap_#{member_id}_$ymd.tsv") do |hdout|
    hdout.file.puts cols.map{|c| c.to_s}.join("\t");
    pq = conn.prepare(q);
    pq.enumerate(member_id) do |row|
      hdout.file.puts cols.map{|c| row[c]}.join("\t");
    end
  end

  conn.close();
  log.d("Finished")
end

if __FILE__ == $0 then
  get_it(ARGV.shift);
end
