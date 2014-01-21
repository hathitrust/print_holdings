require 'hathidb';

def prune_OCLC_resolution_data(datafn, outfn)
  # only retain lines with HT oclc numbers
  file = File.new(datafn, "r")
  outf = File.open(outfn, 'w')

  # get a connection
  db = Hathidb::Db.new();
  conn = db.get_conn()

  # get oclc hash
  oclc_h = {}
  o_rows = conn.query("select distinct(oclc) from holdings_htitem_oclc;")
  o_rows.each do |o|
    oclc_h[o[0]] = 1
  end

  puts "Sanity check: #{oclc_h.length} oclcs."

  count = 0
  out = 0
  no_numbers = 0
  while (line = file.gets)
    count += 1
    #1: 6567842 | 9987701 | 53095235 | 433981287
    #2: 9772597 | 35597370 | 60494959 | 813305061 | 823937796
    #3: 7124033 | 10654585 | 14218190
    #4: 518119215
    # latest file (11/11/2013) has br tag at end (tlp):
    #94: 3696127 | 67412172 | 220820012 | 221206437 | 316195569<br>
    line = line.gsub("<br>", "")
    ocns = line.chomp.split(/[|: ]+/)
    if (ocns.length == 1)
	no_numbers += 1
	next
    end

    ocns.each do |ocn|
      if oclc_h.has_key?(ocn.to_i)
        outf.puts line
	out += 1
      end
    end

    if ((count % 100000) == 0)
      puts "#{count}..."
    end
  end
  puts "#{count} lines read from #{datafn}"
  puts "#{no_numbers} lines skipped (no oclc number)"
  puts "#{out} lines written to #{outfn}"

  file.close
  outf.close
  conn.close
end

## Creates an additional htitem_oclc file
def generate_OCLC_data_for_htitem_oclc(datafn, outfn)
  file = File.new(datafn, "r")
  outf = File.open(outfn, 'w')
  db = Hathidb::Db.new();
  conn = db.get_conn()
  count = 0
  while (line = file.gets)
    count += 1
    ocns = line.chomp.split(/[|: ]+/)
    # get all vol_ids associated with these ocns
    vol_ids = Set.new
    ocns.each do |ocn|
      vid_rows = conn.query("select volume_id from holdings_htitem_oclc where oclc = #{ocn};")
      vid_rows.each do |vrow|
        vol_ids.add(vrow[0])
      end
    end
    # insert vol-oclc pairs
    vol_ids.each do |vid|
      ocns.each do |oc|
        outf.puts "#{vid}\t#{oc}\t1"
      end
    end

    if ((count % 10000) == 0)
      puts "#{count}..."
    end
  end
  file.close
  outf.close
  conn.close
end

root_dir = "/htapps/pulintz.babel/data/phdb/TABLE_DATA/OCLC"
#file_roots = %w{x2.0-1 x2.1-3 x2.3-5 x2.5-7 x2.7-9}
#file_roots = %w{x2.1-3 x2.3-5 x2.5-7 x2.7-9}
file_roots = %w{x2.all}

file_roots.each do |fr|
  fullfr = "#{root_dir}/#{fr}"
  outfn  = "#{fullfr}.out"
  datafn = "#{fullfr}.data"

  puts "processing #{fullfr}..."
  puts "Pruning."
  prune_OCLC_resolution_data(fullfr, outfn)
  puts "Generating."
  generate_OCLC_data_for_htitem_oclc(outfn, datafn)
  puts "Done."
end
