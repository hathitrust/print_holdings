require 'hathidb';

module Multipart

  ALPHANUM_RX = /[A-Z0-9]/i.freeze
  HYPHEN_COMMA_RX = /[\-,]/.freeze

  @@db   = Hathidb::Db.new();
  @@conn = @@db.get_conn();

  Q1 = @@conn.prepare("select distinct oclc from holdings_cluster_oclc where cluster_id = ?")

  Q2 = @@conn.prepare(
    "SELECT member_id, oclc, n_enum, status, item_condition FROM holdings_memberitem WHERE oclc = ?"
  )

  Q3 =  @@conn.prepare(
    "SELECT DISTINCT oclcs, n_enum, chtij.volume_id FROM holdings_htitem AS h,
       holdings_cluster_htitem_jn AS chtij WHERE h.volume_id = chtij.volume_id
       AND chtij.cluster_id = ?"
  )

  Q4 = @@conn.prepare(
    "SELECT DISTINCT ho.oclc, h.n_enum
      FROM holdings_htitem  AS h,
      holdings_htitem_oclc  AS ho,
      holdings_cluster_oclc AS co,
      holdings_memberitem   AS mm
      WHERE h.volume_id = ho.volume_id
      AND ho.oclc       = co.oclc
      AND ho.oclc       = mm.oclc
      AND h.n_enum      = mm.n_enum
      AND co.cluster_id = ?
      AND mm.member_id  = ?"
  )
  
  class MultiItem
    attr_accessor :id, :oclc, :member_id, :status, :item_condition,
    :process_date, :enum_chron, :item_type, :issn, :n_enum, :n_chron

    def initialize(instring)
      bits = instring.chomp.split("\t")

      unless bits.length == 11
        puts "Wrong number of fields: '#{bits.length}'"
        return nil
      end

      @id = bits[0]
      @oclc = bits[1]
      @local_id = bits[2]
      @member_id = bits[3]
      @status = bits[4]
      @item_condition = bits[5]
      @proces_date = bits[6]
      @enum_chron = bits[7]
      @item_type = bits[8]
      @issn = bits[9]
      @n_enum = bits[10]
      @n_chron = bits[11]
    end

    def to_s()
      i_vars = self.instance_variables
      first_var = i_vars.shift
      outstr = self.instance_variable_get(first_var)
      i_vars.each do |var|
        outstr << "\t#{self.instance_variable_get(var)}"
      end
      outstr << "\n"
      return outstr
    end
  end


  # returns a list of members who have contributed enum_chron data
  def Multipart.get_multipart_members_list()
    rows1 = @@conn.query("select member_id from holdings_memberitem
                        where length(n_enum)>0 group by member_id")
    mem_ids = []
    rows1.each do |row|
      mid = row[:member_id]
      mem_ids << mid
    end

    return mem_ids
  end


  def Multipart.get_multipart_cluster_list()
    rows1 = @@conn.query("select distinct(cluster_id) from holdings_cluster where cluster_type = 'mpm'")
    cids = []
    rows1.each do |row|
      cids << row[0]
    end

    return cids
  end


  def Multipart.choose_oclc(cluster_id, member_id)
    res = @@conn.query("select co.oclc, count(*) from holdings_memberitem as mm,
                          holdings_cluster_oclc as co where co.oclc = mm.oclc and
                          cluster_id = #{cluster_id} and member_id = '#{member_id}' group by oclc;")
    max = 0
    winner = ''
    res.each do |row|
      if row[1].to_i > max
        winner = row[0]
      end
    end

    return winner
  end


  ### This method is the main implementation of the mapping between multipart records
  #   provided by members and hathitrust items.  Given a cluster id, this method will
  #   return a set of lines of the format:
  #        oclc n_enum member_id volume_id [counts]
  #   where [counts] is a list of [copy_count, lm_count, wd_count, brt_count, access_count].
  ###
  def Multipart.map_multipart_cluster_to_members(cluster_id, enum_members_l)
    # this subroutine populates the "cluster_htmember_multi" data file

    # collect data rows
    results = []

    ### get all oclcs ###
    query_str = "select distinct oclc from holdings_cluster_oclc where cluster_id = #{cluster_id};"
    rows1 = @@conn.query(query_str)
    ocns = []
    rows1.each do |row1|
      ocns << row1[:oclc]
    end

    ### get all memberdata and construct the memberdata stucture            ###
    ##  The structure is a bit involved to accommodate all the counts, but   ##
    ##  basically its member_id[oclc-nenum] -> [copy_count, lm_count, ...]   ##
    member_data = {}    # this will be a hash of hashes of lists (refactor this)
    ocns.each do |ocn|
      rows2 = @@conn.query("select member_id, oclc, n_enum, status, item_condition from holdings_memberitem
                          where oclc = #{ocn};")
      rows2.each do |row2|
        data_key = "#{row2[:oclc]}-#{row2[:n_enum]}"
        if member_data.has_key?(row2[:member_id])
          if member_data[row2[:member_id]].has_key?(data_key)
            member_data[row2[:member_id]][data_key][0] += 1
            member_data[row2[:member_id]][data_key][1] += 1 if row2[:status] == 'LM'
            member_data[row2[:member_id]][data_key][2] += 1 if row2[:status] == 'WD'
            member_data[row2[:member_id]][data_key][3] += 1 if row2[:item_condition] == 'BRT'
            member_data[row2[:member_id]][data_key][4] += 1 if (row2[:status] == 'LM' or row2[:item_condition] == 'BRT')
          else
            row2[:status] == 'LM' ? lm_count = 1 : lm_count = 0
            row2[:status] == 'WD' ? wd_count = 1 : wd_count = 0
            row2[:item_condition] == 'BRT' ? brt_count = 1 : brt_count = 0
            (lm_count + brt_count) > 0 ? access_count = 1 : access_count = 0
            member_data[row2[:member_id]][data_key] = [1, lm_count, wd_count,
                                                       brt_count, access_count]
          end
        else
          count_l = [1, 0, 0, 0, 0]
          count_l[1] += 1 if row2[:status] =='LM'
          count_l[2] += 1 if row2[:status] == 'WD'
          count_l[3] += 1 if row2[:item_condition] == 'BRT'
          count_l[4] += 1 if (row2[:status] == 'LM' or row2[:item_condition] == 'BRT')
          member_data[row2[:member_id]] = {data_key => count_l}
        end
      end
    end


    ### get all HT data ###
    ht_data = []  # this will be a list of lists
    rows3 = @@conn.query("select distinct oclcs, n_enum, chtij.volume_id from holdings_htitem as h,
                        holdings_cluster_htitem_jn as chtij where h.volume_id = chtij.volume_id
                        and chtij.cluster_id = #{cluster_id};")
    rows3.each do |row3|
      ht_data << [row3[0], row3[1], row3[2]]
    end

    # if HT data has 'blanks', bailout assign all members from both lists
    bailout = 0

    ### construct volume_id maps for cluster enums
    ht_dict = {}
    ht_data.each do |htd|
      if ht_dict.has_key?(htd[1])
        ht_dict[htd[1]] << htd[2]
      else
        ht_dict[htd[1]] = [htd[2]]
      end
    end

    ### categorize members ###
    enum_match_mems = []
    all_match_mems = []   # these members will be assigned to all items
    if (bailout == 1)
      all_match_mems = member_data.keys
    else
      unique_members = member_data.keys
      unique_members.each do |mem|
        if not enum_members_l.include?(mem)
          all_match_mems << mem
          next
        end
        data = member_data[mem]
        fail = 0
        data.each_pair do |k,v|
          k_bits = k.split('-')
          if not (k_bits[1] =~ /[A-Z0-9]/i)
            fail = 1
            next
          end
          if (k_bits[1] =~ /[\-,]/)
            fail = 1
            next
          end
        end
        fail == 0 ? enum_match_mems << mem : all_match_mems << mem
      end
    end

    ### add 'enum-match' members (should be <= all match) ###
    enum_match_mems.each do |emm|
      rows4 = @@conn.query("select distinct ho.oclc, h.n_enum from holdings_htitem as h,
                          holdings_htitem_oclc as ho, holdings_cluster_oclc as co, holdings_memberitem as mm
                          where h.volume_id = ho.volume_id and ho.oclc = co.oclc and
                          ho.oclc = mm.oclc and h.n_enum = mm.n_enum and co.cluster_id = #{cluster_id}
                          and mm.member_id = '#{emm}';")
      rows4.each do |row4|
        pkey = "#{row4[1]}"
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{emm}"
        end
        count_key = "#{row4[0]}-#{row4[1]}"
        count_str = "1\t0\t0\t0\t0"
        if member_data[emm].has_key?(count_key)
          copy_counts = member_data[emm][count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{row4[0]}\t#{row4[1]}\t#{emm}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
      end
      # if no matches (but with member data for oclc), add member to all-match members
      if rows4.count() == 0
        all_match_mems << emm
      end

    end

    ### add 'all-match' members ###
    all_match_mems.each do |amm|
      ht_data.each do |ht_item|
        ocn = ht_item[0]
        if ht_item[0] =~ /[,]/          #  this can be optimized, its redundant
          ocn = Multipart.choose_oclc(cluster_id, amm)
        end
        pkey = "#{ht_item[1]}"
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{amm}"
        end
        count_key = "#{ocn}-#{ht_item[1]}"
        count_str = "1\t0\t0\t0\t0"
        if member_data[amm].has_key?(count_key)
          copy_counts = member_data[amm][count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{ocn}\t#{ht_item[1]}\t#{amm}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
      end
    end

    return results
  end

  def Multipart.map_multipart_cluster_to_members_debug(cluster_id, enum_members_l)
    # this subroutine populates the "cluster_htmember_multi" data file

    # collect data rows
    results = []
    ocns    = []

    Q1.enumerate(cluster_id) do |row1|
      ocns << row1[:oclc]
    end

    ### get all memberdata and construct the memberdata stucture            ###
    ##  The structure is a bit involved to accommodate all the counts, but   ##
    ##  basically its member_id[oclc-nenum] -> [copy_count, lm_count, ...]   ##
    member_data = {}    # this will be a hash of hashes of lists (refactor this)
    ocns.each do |ocn|
      Q2.enumerate(ocn) do |row2|
        data_key  = "#{row2[:oclc]}-#{row2[:n_enum]}"
        member_id = row2[:member_id]
        status    = row2[:status]
        item_cond = row2[:item_condition]
        # First time we see member_id-data_key, set up array of ints:
        member_data[member_id]           ||= {}
        member_data[member_id][data_key] ||= [0, 0, 0, 0, 0]
        # Every time we see member_id-data_key: incr ints as seen fit.
        member_data[member_id][data_key][0] += 1
        member_data[member_id][data_key][1] += 1 if status    == 'LM'
        member_data[member_id][data_key][2] += 1 if status    == 'WD'
        member_data[member_id][data_key][3] += 1 if item_cond == 'BRT'
        member_data[member_id][data_key][4] += 1 if (status == 'LM' or item_cond == 'BRT')
      end
    end

    ### get all HT data ###
    ht_data = []  # this will be a list of lists    
    Q3.enumerate(cluster_id) do |row3|
      ht_data << [row3[0], row3[1], row3[2]] # oclcs, n_enum, volume_id
    end

    ### construct volume_id maps for cluster enums
    ht_dict = {}
    ht_data.each do |htd|
      ht_dict[htd[1]] ||= []
      ht_dict[htd[1]] << htd[2]
    end

    ### categorize members ###
    enum_match_mems = [] # these members will be assigned to items where enum match
    all_match_mems  = [] # these members will be assigned to all items

    unique_members = member_data.keys
    unique_members.each do |mem|
      if not enum_members_l.include?(mem)
        all_match_mems << mem
        next
      end
      data = member_data[mem]
      fail = 0
      data.each_pair do |k,v|
        k_bits = k.split('-')
        if not (k_bits[1] =~ ALPHANUM_RX)
          fail = 1
          next
        end
        if (k_bits[1] =~ HYPHEN_COMMA_RX)
          fail = 1
          next
        end
      end
      fail == 0 ? enum_match_mems << mem : all_match_mems << mem
    end

    ### add 'enum-match' members (should be <= all match) ###
    enum_match_mems.each do |emm|
      emm_match_count = 0;
      Q4.enumerate(cluster_id, emm) do |row4|
        pkey = "#{row4[1]}"
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{emm}"
        end
        count_key = "#{row4[0]}-#{row4[1]}"
        count_str = "1\t0\t0\t0\t0"
        if member_data[emm].has_key?(count_key)
          copy_counts = member_data[emm][count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{row4[0]}\t#{row4[1]}\t#{emm}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
        emm_match_count += 1
      end

      # if no matches (but with member data for oclc), add member to all-match members
      if emm_match_count == 0 # <--- this used to be broken, used rows4.count()==0, mw 2020-02-17
        all_match_mems << emm
      end
    end

    ### add 'all-match' members ###
    all_match_mems.each do |amm|
      ht_data.each do |ht_item|
        ocn = ht_item[0]
        if ocn =~ /[,]/          #  this can be optimized, its redundant
          ocn = Multipart.choose_oclc(cluster_id, amm)
        end
        pkey = "#{ht_item[1]}"
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{amm}"
        end
        count_key = "#{ocn}-#{ht_item[1]}"
        count_str = "1\t0\t0\t0\t0"
        if member_data[amm].has_key?(count_key)
          copy_counts = member_data[amm][count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{ocn}\t#{ht_item[1]}\t#{amm}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
      end
    end

    return results
  end


  ### Modification of the previous routine to only generate a file for an individual member  ###
  def Multipart.map_multipart_cluster_to_individual_member(cluster_id, member_id, member_type)
    # this subroutine populates the "cluster_htmember_multi" data file

    # collect data rows
    results = []

    ### get all oclcs ###
    query_str = "select distinct oclc from holdings_cluster_oclc where cluster_id = #{cluster_id};"
    rows1 = @@conn.query(query_str)
    ocns = []
    rows1.each do |row1|
      ocns << row1[:oclc]
    end

    ### get all data for these OCLCs and construct the memberdata stucture   ###
    ##  The structure is a list of counts, basically its oclc-nenum -> [copy_count, lm_count, ...]
    member_data = {}
    ocns.each do |ocn|
      rows2 = @@conn.query("select oclc, n_enum, status, item_condition from holdings_memberitem
                          where oclc = #{ocn} and member_id = '#{member_id}';")
      rows2.each do |row2|
        n_enum = row2[:n_enum]
        next if n_enum.nil?
        data_key = "#{ocn}-#{n_enum}"
        if member_data.has_key?(data_key)
          member_data[data_key][0] += 1
          member_data[data_key][1] += 1 if row2[:status] == 'LM'
          member_data[data_key][2] += 1 if row2[:status] == 'WD'
          member_data[data_key][3] += 1 if row2[:item_condition] == 'BRT'
          member_data[data_key][4] += 1 if (row2[:status] == 'LM' or row2[:item_condition] == 'BRT')
        else
          count_l = [1, 0, 0, 0, 0]
          count_l[1] += 1 if row2[:status] =='LM'
          count_l[2] += 1 if row2[:status] == 'WD'
          count_l[3] += 1 if row2[:item_condition] == 'BRT'
          count_l[4] += 1 if (row2[:status] == 'LM' or row2[:item_condition] == 'BRT')
          member_data[data_key] = count_l
        end
      end
    end

    return nil if member_data.empty?

    ### get all HT data ###
    ht_data = []  # this will be a list of lists
    rows3 = @@conn.query("select distinct oclcs, n_enum, chtij.volume_id from holdings_htitem as h,
                        holdings_cluster_htitem_jn as chtij where h.volume_id = chtij.volume_id
                        and chtij.cluster_id = #{cluster_id};")
    rows3.each do |row3|
      ht_data << [row3[0], row3[1], row3[2]]
    end

    # if HT data has 'blanks', bailout assign all members from both lists
    punt = 0

    ### construct volume_id maps for cluster enums
    ht_dict = {}
    ht_data.each do |htd|
      if ht_dict.has_key?(htd[1])   # 1 = n_enum
        ht_dict[htd[1]] << htd[2]   # 2 = volume_id
      else
        ht_dict[htd[1]] = [htd[2]]
      end
    end

    ### Handle detailed matching VS all matching (punt) case ###
    punt_count = 0
    non_punt_count = 0
    if member_type
      rows4 = @@conn.query("select distinct ho.oclc, h.n_enum from holdings_htitem as h,
                          holdings_htitem_oclc as ho, holdings_cluster_oclc as co, holdings_memberitem as mm
                          where h.volume_id = ho.volume_id and ho.oclc = co.oclc and
                          ho.oclc = mm.oclc and h.n_enum = mm.n_enum and co.cluster_id = #{cluster_id}
                          and mm.member_id = '#{member_id}';")
      # if no matches (but with member data for oclc), add member to all-match members
      if rows4.count() == 0
        punt = 1
        punt_count += 1
      else
        punt = 0
        non_punt_count += 1
      end
      rows4.each do |row4|
        pkey = "#{row4[1]}"      # n_enum
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{member_id}"
        end
        count_key = "#{row4[0]}-#{row4[1]}"   # oclc-n_enum
        count_str = "1\t0\t0\t0\t0"
        if member_data.has_key?(count_key)
          copy_counts = member_data[count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{row4[0]}\t#{row4[1]}\t#{member_id}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
      end
    end

    if (punt == 1)   # match all-all
      ht_data.each do |ht_item|
        ocn = ht_item[0]
        if ht_item[0] =~ /[,]/          #  this can be optimized, its redundant
          ocn = Multipart.choose_oclc(cluster_id, member_id)
        end
        pkey = "#{ht_item[1]}"
        vol_str = ''
        if ht_dict[pkey]
          vol_str = ht_dict[pkey].join(',')
        else
          puts "Problem: zero length vol_str:\n\t#{cluster_id}\t#{row4[0]}\t#{row4[1]}\t#{member_id}"
        end
        count_key = "#{ocn}-#{ht_item[1]}"
        count_str = "1\t0\t0\t0\t0"
        if member_data.has_key?(count_key)
          copy_counts = member_data[count_key]
          count_str = copy_counts.join("\t")
        end
        outstr = "#{ocn}\t#{ht_item[1]}\t#{member_id}\t#{cluster_id}\t#{vol_str}\t#{count_str}"
        results << outstr
      end
    end

    return results
  end

  def calc_multireport_stats(mfilen)
    sum_memberitems = 0
    sum_matches = 0
    sum_memblanks = 0
    sum_htblanks = 0
    clusters_with_blanks = 0
    File.readlines(mfilen).each do |line|
      bits = line.split("\t")
      sum_memberitems += bits[3].to_i
      sum_matches += bits[4].to_i
      sum_memblanks += bits[7].to_i
      sum_htblanks += bits[8].to_i
      if bits[8].to_i > 0
        clusters_with_blanks += 1
      end
    end
    puts "\nTotal memberitems: #{sum_memberitems}"
    puts "Total matches: #{sum_matches}"
    puts "Total memblanks: #{sum_memblanks}"
    puts "Total htblanks: #{sum_htblanks}"
    percent_match = sum_matches.to_f/sum_memberitems.to_f
    percent_blank = sum_memblanks.to_f/sum_memberitems.to_f
    puts "Percentage matched: #{percent_match}"
    puts "Percentage blanks: #{percent_blank}"
    puts "Clusters with blanks: #{clusters_with_blanks}"
  end

end
