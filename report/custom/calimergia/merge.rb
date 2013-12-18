require 'hathidb';
require 'hathidata';
require 'hathilog';

$db   = Hathidb::Db.new();
$log  = Hathilog::Log.new();
$cali_libs = %W<berkeley ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf>;

=begin

Exp 1 Start.

So, this only tells us what it would look like if we smush together
the already aggregated counts. holdings_H_counts tells us how many
of each H each member has. We need to go earlier than this.

=end

def exp1()
  sql = %W<
    SELECT
        h_id,
        member_id,
        sum(h_count) AS sum_h
    FROM
        holdings_H_counts
    WHERE
        access = 'deny'
        AND
        member_id IN (#{$cali_libs.map{|x| "'#{x}'"}.join(',')})
    GROUP BY
        h_id, member_id
    ORDER BY
        member_id, H_id;
  >.join(' ');

  $log.d(sql);
  h_sums = {};

  conn = $db.get_conn();
  conn.query(sql) do |row|
    h_sums[row['h_id']] ||= 0;
    h_sums[row['h_id']] +=  row['sum_h'].to_i;
  end

  h_sums.each_key do |x|
    $log.d("#{x}\t#{h_sums[x]}");
  end
  conn.close();
end
#exp1();


=begin

This is pretty much what generates the data for exp1. 
Next question, where does holdings_htitem_H come from?

Query borrowed from:
/htapps/pulintz.babel/Code/phdb/lib/sql/create_calcHCounts_stored_procedure_all.sql

=end

def exp2()

  sql = %W<
    SELECT
        hh.H,
        COUNT(DISTINCT hh.volume_id)
    FROM
        holdings_htitem             AS h,
        holdings_htitem_H           AS hh,
        holdings_htitem_htmember_jn AS hhj
    WHERE
        hh.volume_id = hhj.volume_id
        AND
        hh.volume_id = h.volume_id
        AND
        hhj.member_id IN (#{$cali_libs.map{|x| "'#{x}'"}.join(',')})
        AND
        h.access = 'deny'
    GROUP BY 
        hh.H
  >.join(' ');

  $log.d(sql);

  conn = $db.get_conn();
  conn.query(sql) do |row|
    $log.d("#{row[0]}\t#{row[1]}");
  end
  conn.close();
end
#exp2();

def exp3()
  sql = %W<
    SELECT 
        h.volume_id,
        hhh.H,
        COUNT(hhhj.member_id) AS group_h
    FROM 
        holdings_htitem AS h
        JOIN
        holdings_htitem_htmember_jn AS hhhj
        ON (hhhj.volume_id = h.volume_id)
        JOIN
        holdings_htitem_H AS hhh
        ON (hhhj.volume_id = hhh.volume_id)
    WHERE
        h.access = 'deny'
        AND
        hhhj.member_id IN (#{$cali_libs.map{|x| "'#{x}'"}.join(',')}) 
    GROUP BY 
        h.volume_id,
        hhh.H
  >.join(' ');

  $log.d(sql);

  hd = Hathidata::Data.new('cali_volid_h.tsv');
  if hd.exists? then
    $log.d("Already have #{hd.path}");
  else
    hd.open('w');
    conn = $db.get_conn();
    i = 0;
    conn.query(sql) do |row|
      i += 1;
      hd.file.puts "#{row['volume_id']}\t#{row['H']}\t#{row['group_h']}";
      if i % 100000 == 0 then
        $log.d(i);
      end
    end
    conn.close();
    hd.close();
  end
end
exp3()
