module CostCalc

  def CostCalc.calc_ave_cost_per_vol(projected_operating_costs, conn)
    total_HT_items = 0
    conn.query("select count(*) as c from holdings_htitem") do |count1|
      total_HT_items = count1['c']
      break
    end
    return projected_operating_costs.to_f/total_HT_items.to_f
  end

  def CostCalc.get_monograph_list(conn)
    members = []
    conn.query("select distinct (member_id) from holdings_memberitem") do |mr|
      members << mr['member_id']
    end
    return members
  end

  def CostCalc.get_serial_list(conn)
    serial_member_sql  = "SELECT DISTINCT member_id FROM holdings_memberitem WHERE item_type='serial' ORDER BY member_id"
    serial_members = []
    conn.query(serial_member_sql) do |row|
      serial_members << row[:member_id]
    end

    return serial_members
  end


  def CostCalc.calc_pd_costs(ave_cost_per_vol, conn)
    public_counts = []
    # get the singlepart monograph public domain cost
    conn.query("select count(*) as c from holdings_htitem where item_type = 'mono' and access = 'allow'") do |count1|
      public_counts << count1['c']
      break
    end

    # get the serial public domain cost
    conn.query("select count(*) as c from holdings_htitem where item_type = 'serial' and access = 'allow'") do |count2|
      public_counts << count2['c']
      break
    end

    # get the multipart monograph public domain cost
    conn.query("select count(*) as c from holdings_htitem where item_type = 'multi' and access = 'allow'") do |count3|
      public_counts << count3['c']
      break
    end

    costs = public_counts.map {|x| x.to_i*ave_cost_per_vol}

    return costs
  end

  def CostCalc.calc_total_ic_singlepart_monograph_cost(ave_cost_per_vol, conn)
    ic_spm = 0
    conn.query("select count(*) as c from holdings_htitem where item_type = 'mono' and access = 'deny'") do |count_row|
      ic_spm = count_row['c'].to_i
      break
    end
    total_spm_cost = ic_spm * ave_cost_per_vol

    return total_spm_cost
  end

  def CostCalc.calc_adjusted_ic_spm_ave_cost_per_vol(ave_cost_per_vol, conn)
    new_total = 0.0
    conn.query("select distinct (member_id) from holdings_memberitem") do |mr|
      result = CostCalc.calc_ic_singlepart_monograph_cost_for_member(mr['member_id'], ave_cost_per_vol)
      cost_f = "%.2f" % result.to_f
      new_total += result.to_f
    end

    old_total = CostCalc.calc_total_ic_singlepart_monograph_cost(ave_cost_per_vol)
    diff = old_total - new_total
    puts "\tSPM Old Total: #{old_total} New Total: #{new_total} \n\tDiff: #{diff}"

    # need the number of *matching volumes* among which to distribute new cost
    ic_spms = 0
    conn.query("select count(*) as c from holdings_htitem_H as hhH, holdings_htitem as hh
                            where hhH.volume_id = hh.volume_id and hh.item_type = 'mono'
                            and hh.access = 'deny' ") do |count_row|
      ic_spms = count_row['c'].to_i
      break
    end

    new_ave_cost_per_vol = ave_cost_per_vol + diff/ic_spms

    return new_ave_cost_per_vol
  end

  def CostCalc.calc_ic_singlepart_monograph_cost_for_member(mem_id, ave_cost_per_vol, conn)
    total_cost = 0.0

    # get the in-copyright lookup table values for member
    conn.query("select H_id, H_count from holdings_H_counts where member_id = '#{mem_id}'
                       and access = 'deny' and item_type = 'mono'") do |row|
      h = row[:H_id].to_i
      count = row[:H_count].to_i
      cost = (count*ave_cost_per_vol)/h
      total_cost += cost
    end
    return total_cost
  end

  def CostCalc.calc_total_ic_multipart_monograph_cost(ave_cost_per_vol, conn)
    ic_multis = 0
    conn.query("select count(*) as c from holdings_htitem where item_type = 'multi' and access = 'deny'") do |count_row|
      ic_multis = count_row['c']
    end
    total_multi_cost = ic_multis.to_i * ave_cost_per_vol

    return total_multi_cost
  end

  def CostCalc.calc_adjusted_ic_multipart_cost_per_vol(old_ave_cost_per_vol, total_amount_reduced, conn)
    ### DEPRECATED ###

    raise "DEPRECATED"

    # given a value to be deducted from the total, calculate a new ave_cost_per_vol for serials
    old_total_cost = CostCalc.calc_total_ic_multipart_monograph_cost(old_ave_cost_per_vol)

    ic_multis = 0
    # need the number of *matching volumes* among which to distribute new cost
    conn.query("select count(distinct oclc, n_enum) as c from cluster_htmember_multi as chm,
                            cluster as c where chm.cluster_id = c.cluster_id and
                            not c.cost_rights_id = 1") do |count_row|

      ic_multis = count_row['c'].to_i
      break
    end

    new_ave_cost_per_vol = (old_total_cost - total_amount_reduced) / ic_multis

    return new_ave_cost_per_vol
  end

  def CostCalc.calc_adjusted_ic_mpm_ave_cost_per_vol(ave_cost_per_vol, conn)
    new_total = 0.0
    conn.query("select distinct (member_id) from holdings_memberitem") do |mr|
      result = CostCalc.calc_ic_multipart_monograph_cost_for_member(mr['member_id'], ave_cost_per_vol)
      cost_f = "%.2f" % result.to_f
      new_total += result.to_f
    end

    old_total = CostCalc.calc_total_ic_multipart_monograph_cost(ave_cost_per_vol)
    diff = old_total - new_total
    puts "\tMPM Old Total: #{old_total} New Total: #{new_total} \n\tDiff: #{diff}"

    # need the number of *matching volumes* among which to distribute new cost
    ic_mpms = 0
    conn.query("select count(*) as c from holdings_htitem_H as hhH, holdings_htitem as hh
                            where hhH.volume_id = hh.volume_id and hh.item_type = 'multi'
                            and hh.access = 'deny' ") do |count_row|
      ic_mpms = count_row['c'].to_i
      break
    end

    new_ave_cost_per_vol = ave_cost_per_vol + diff / ic_mpms

    return new_ave_cost_per_vol
  end

  def CostCalc.calc_ic_multipart_monograph_cost_for_member(mem_id, ave_cost_per_vol, conn)
    total_cost = 0.0

    # get the in-copyright lookup table values for member
    conn.query("select H_id, H_count from holdings_H_counts where member_id = '#{mem_id}'
    and access = 'deny' and item_type = 'multi'") do |row|
      h = row[:H_id].to_i
      count = row[:H_count].to_i
      cost = (count*ave_cost_per_vol)/h
      total_cost += cost
    end
    return total_cost
  end

  def CostCalc.calc_total_ic_serial_cost(ave_cost_per_vol, conn)
    ic_serials = 0
    conn.query("select count(*) as c from holdings_htitem where item_type = 'serial' and access = 'deny'") do |count_row|
      ic_serials = count_row['c'].to_i
      break
    end
    total_serial_cost = ic_serials * ave_cost_per_vol

    return total_serial_cost
  end

  def CostCalc.calc_adjusted_ic_serial_cost_per_vol(old_ave_cost_per_vol, total_amount_reduced, conn)
    # given a value to be deducted from the total, calculate a new ave_cost_per_vol for serials
    old_total_cost = CostCalc.calc_total_ic_serial_cost(old_ave_cost_per_vol)

    # need the number of *matching* volumes among which to distribute new cost
    ic_serials = 0
    conn.query("select count(*) as c from holdings_htitem_H as hhH, holdings_htitem as hh
                            where hhH.volume_id = hh.volume_id and hh.item_type = 'serial'
                            and hh.access = 'deny' ") do |count_row|
      ic_serials = count_row['c'].to_i
      break
    end

    new_ave_cost_per_vol = (old_total_cost - total_amount_reduced) / ic_serials
    return new_ave_cost_per_vol
  end

  def CostCalc.calc_ic_serial_cost_for_member_by_size(mem_id, conn)
    xfactor = 1.50
    total_storage_size_bytes = 480722804968888.625
    total_storage_cost_usd = 1878347

    rowcount = 0
    total_cost = 0.0
    # get the in-copyright lookup table values for member
    conn.enumerate("SELECT holdings_cluster.cluster_id, cost_rights_id, total_serial_size, H
                    FROM holdings_cluster, holdigns_cluster_htmember_jn
                    WHERE holdings_cluster.cluster_id = holdings_cluster_htmember_jn.cluster_id
                    AND total_serial_size > 0 AND NOT cost_rights_id = 1 AND member_id = '#{mem_id}'").each_slice(1000) do |slice|

      slice.each do |row|
        rowcount += 1
        cluster_id = row['cluster_id']
        total_serial_size = row[:total_serial_size].to_f
        volume_H = row[:H].to_i
        # calc cost for serial based on size
        serial_storage_cost = (total_serial_size/total_storage_size_bytes)*total_storage_cost_usd
        serial_cost_to_member = serial_storage_cost/volume_H
        total_cost += serial_cost_to_member
      end
    end
    return total_cost
  end

  def CostCalc.calc_member_monograph_percent_contribution(cost_hash, total_mono_cost)
    # expects a data structure as a hash, keyed by member_id, with a list [spm, mpm] costs as a value
    percent_hash = {}
    cost_hash.each_key {|k| percent_hash[k] = nil}

    cost_hash.each { |key, value|
      sum = value[0] + value[1]
      percent_hash[key] = (value[0]+value[1])/total_mono_cost
    }
    return percent_hash
  end

  def CostCalc.calc_ic_serial_cost_for_member_by_count(mem_id, ave_cost_per_vol, conn)
    total_cost = 0.0
    # get the in-copyright lookup table values for member
    conn.enumerate("SELECT holdings_cluster.cluster_id, cost_rights_id, H, num_of_items
                    FROM holdings_cluster, holdings_cluster_htmember_jn
                    WHERE holdings_cluster.cluster_id = holdings_cluster_htmember_jn.cluster_id
                    AND cluster_type = 'ser'
                    AND member_id = '#{mem_id}'
                    AND NOT cost_rights_id = 1
                    GROUP BY cluster_id").each_slice(10000) do |slice|
      slice.each do |row|
        cluster_id = row['cluster_id']
        total_volume_count= row[3].to_i
        volume_H = row[:H].to_i
        # calc cost for serial based on size
        total_serial_cost = (total_volume_count)*ave_cost_per_vol
        serial_cost_to_member = total_serial_cost/volume_H
        total_cost += serial_cost_to_member
      end
    end

    return total_cost
  end

  def CostCalc.calc_ic_serial_cost_for_member_by_count2(mem_id, ave_cost_per_vol, conn)
    total_cost = 0.0

    # get the in-copyright lookup table values for member
    conn.query("SELECT H_id, H_count FROM holdings_H_counts WHERE member_id = '#{mem_id}'
                    AND item_type = 'serial' AND access = 'deny'") do |row|
      h = row[:H_id].to_i
      h_count = row[:H_count].to_i
      total_cost += h_count*(ave_cost_per_vol/h)
    end

    return total_cost
  end

end
