
require 'phdb/phdb_utils'
require 'date'


def put_total_member_counts(filen)
  conn = PHDBUtils.get_dev_conn()
  outf = File.open(filen, "w")
  
  rows = conn.query("select member_id, item_type, count(*) 
                     from holdings_memberitem group by member_id, item_type")
  rows.each do |row|
    outstr = row.join("\t")
    outf.puts outstr
  end      
  outf.close     
  conn.close
end


def put_matching_item_counts(outfn)
  conn = PHDBUtils.get_dev_conn()
  outf = File.open(outfn, 'w')
                                                                                     
  members = []
  rows = conn.query("select member_id, member_name from holdings_htmember")
  rows.each do |row|
    members << row[:member_id]
  end
  puts "#{members.length} members."

  smembers = members.sort
  smembers.each do |mem|
    q1 = "select item_type, count(distinct hhj.volume_id)
          from holdings_htitem_htmember_jn as hhj, holdings_htitem as h where hhj.volume_id = h.volume_id
          and member_id = '#{mem}' group by item_type"
    conn.query(q1) do |row|
      out_str = "#{mem}\t#{row[0]}\t#{row[1]}"
      puts out_str
      outf.puts(out_str)
    end
  end
  conn.close
  outf.close
end


def put_matching_oclc_counts(outfn)
  conn = PHDBUtils.get_dev_conn()
  outf = File.open(outfn, 'w')

  members = []
  rows = conn.query("select member_id, member_name from holdings_htmember")
  rows.each do |row|
    members << row[:member_id]
  end
  puts "#{members.length} members."

  smembers = members.sort
  smembers.each do |mem|
    q1 = "select item_type, count(distinct oclc)      
          from holdings_memberitem where member_id = '#{mem}' group by item_type"
    conn.query(q1) do |row|
      out_str = "#{mem}\t#{row[0]}\t#{row[1]}"
      puts out_str
      outf.puts(out_str)
    end
  end
  conn.close
  outf.close
end


def main()
  date = DateTime.now
  datestr = date.strftime("%Y%m%d")

  ### count report queries ### 
  name1 = "total_member_counts.#{datestr}.out"
  puts "Generating #{name1}..."
  put_total_member_counts(name1)

  name2 = "matching_item_counts.#{datestr}.out"
  puts "Generating #{name2}..."
  put_matching_item_counts(name2)

  name3 = "matching_oclc_counts.#{datestr}.out"
  puts "Generating #{name3}..."
  put_matching_oclc_counts(name3)
end



main()
