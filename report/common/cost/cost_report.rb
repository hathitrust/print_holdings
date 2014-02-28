require 'phdb/phdb_utils'
require 'phdb/cost_calculator'

### calc_costs.rb ###
# This script implements the cost calculation for hathitrust members.  
# It currently calculates serials using the 'remove non-serial-members' approach,
# although there's only one partner who hasn't submitted serial data yet (USU).


if ARGV.length != 2
  puts "Usage: ruby calc_cost.rb <infrastructure_cost> <num_of_members>\n"
  exit
end

calc_full_serial = true
xfactor = 1.0


#serial_member_list = %w{arizona asu baylor bc brandeis berkeley bu calgary chi cmu 
#			columbia cornell dart 
#                        duke emory fsu getty harvard iastate ind iowa jhu ksu ku 
#			lafayette loc mcgill 
#                        miami minn missouri mit msu nccu ncsu nd nwu nypl nyu osu ou pitt 
#                        prnc psu purd stanford syr tamu ucdavis uci ucla ucmerced uconn ucr 
#                        ucsb ucsc ucsd ucsf udel ufl uh uic uiuc umd unc unl 
#                        uom upenn utah uva uvm vt washington wfu wisc wustl yale}
puts "Getting serial member list..."
serial_member_list = PHDBUtils.get_serial_member_list()

#monograph_member_list = %w{arizona asu baylor bc berkeley bu chi columbia cornell dart duke emory 
#                           getty harvard ind iowa jhu lafayette loc mcgill minn missouri mit msu 
#                           nccu ncsu nd nwu nypl nyu osu pitt prnc psu purd stanford tamu 
#                           ucdavis uci ucla ucm ucmerced uconn ucr ucsb ucsc ucsd ucsf ufl uic uiuc 
#                           umd unc unl uom upenn usu utah uva washington wisc wustl yale}
                           
no_serials_list = %w{usu}
        
                           
#multi_members_enum = %w{bc chi columbia cornell dart harvard ind lafayette minn missouri nd nwu
#                        nyu osu pitt prnc psu purd stanford tamu ufl uic unl uom upenn washington wisc}
#                        
#multi_members_no_enum = %w{asu baylor berkeley bu duke emory getty iowa jhu loc mit msu nccu ncsu 
#                           ucsdavis uci ucla ucmerced uconn ucr ucsb ucsc ucsd ucsf uiuc umd unc usu 
#                           utah uva yale}

total_operating_costs = Integer(ARGV[0])
total_members = Integer(ARGV[1])

# Average Cost Per Volume Calculations
ave_cost_per_vol = PHDBUtils.calc_ave_cost_per_vol(total_operating_costs)
puts "Ave Cost Per Vol: #{ave_cost_per_vol}"

ave_cost_per_vol_ic_spm = PHDBUtils.calc_adjusted_ic_spm_ave_cost_per_vol(ave_cost_per_vol)
puts "Adjusted Ave Cost Per IC SPM Vol: #{ave_cost_per_vol_ic_spm}"

ave_cost_per_vol_ic_mpm = PHDBUtils.calc_adjusted_ic_mpm_ave_cost_per_vol(ave_cost_per_vol)
puts "Adjusted Ave Cost Per IC MPM Vol: #{ave_cost_per_vol_ic_mpm}"


monograph_member_list = PHDBUtils.get_monograph_list

### public domain costs ###
public_costs = PHDBUtils.calc_pd_costs(ave_cost_per_vol)
sum = 0
public_costs.each { |a| sum+=a }
per_member = sum/total_members

puts "Public domain costs:"
puts "Calculating PD cost based on #{total_members} members."
puts "\tSinglepart Monos: $#{public_costs[0]}"
puts "\tSerials: $#{public_costs[1]}"
puts "\tMultipart Monos: $#{public_costs[2]}"
puts "\tTotal public domain costs: $#{sum}"
puts "\tPublic domain cost per member: $#{per_member}"
puts "Remaining total cost: $#{total_operating_costs - sum}"


### monograph cost structure ###
cost_struct = {}
monograph_member_list.each{|x| cost_struct[x] = [nil, nil, nil, nil]}

### singlepart monograph costs ###
spm_total = 0.0
unadj_spm_total = 0.0
puts "\nIn-copyright Singlepart Monograph costs:"
target_single_cost = PHDBUtils.calc_total_ic_singlepart_monograph_cost(ave_cost_per_vol)
# calc unadjusted amount
monograph_member_list.each do |mmember|
  result = PHDBUtils::calc_ic_singlepart_monograph_cost_for_member(mmember, ave_cost_per_vol)
  unadj_spm_total += result.to_f
end
# calc adjusted amount
monograph_member_list.each do |mmember|
  result = PHDBUtils::calc_ic_singlepart_monograph_cost_for_member(mmember, ave_cost_per_vol_ic_spm)
  xresult = result*xfactor
  cost_f = "%.2f" % result.to_f
  cost_f2 = "%.2f" % xresult.to_f
  spm_total += result.to_f
  #puts "#{mmember}\t#{cost_f}\t#{cost_f2}"
  cost_struct[mmember][0] = result.to_f
end
total_spm = "%.2f" % spm_total
puts "Target IC spm cost: $#{target_single_cost}"
puts "Unadjusted IC SPM cost: $#{unadj_spm_total}"
puts "Adjusted IC spm cost: $#{total_spm}"
puts "Remaining total cost $#{total_operating_costs - sum - spm_total}"


### multipart monograph costs ###
mpm_total = 0.0
unadj_mpm_total = 0.0
puts "\nIn-copyright Multipart Monograph costs:"
target_multi_cost = PHDBUtils.calc_total_ic_multipart_monograph_cost(ave_cost_per_vol)
monograph_member_list.each do |mmember|
  result = PHDBUtils::calc_ic_multipart_monograph_cost_for_member(mmember, ave_cost_per_vol)
  unadj_mpm_total += result.to_f
end
monograph_member_list.each do |mmember|
  result = PHDBUtils::calc_ic_multipart_monograph_cost_for_member(mmember, ave_cost_per_vol_ic_mpm)
  #xresult = result*xfactor
  cost_f = "%.2f" % result.to_f
  #cost_f2 = "%.2f" % xresult.to_f
  mpm_total += result.to_f
  puts "#{mmember}\t#{cost_f}"
  cost_struct[mmember][1] = result.to_f
end
total_mpm = "%.2f" % mpm_total
puts "Target IC multi cost: $#{target_multi_cost}"
puts "Unadjusted Actual IC MPM cost: $#{unadj_mpm_total}"
puts "Adjusted Actual IC mpm cost: $#{total_mpm}"
puts "Remaining total cost $#{total_operating_costs - sum - spm_total - mpm_total}"


### serial costs ###
# cost of all serial volumes
puts "\nIn-copyright Serial costs:"
ser1_total = PHDBUtils.calc_total_ic_serial_cost(ave_cost_per_vol)
total_ser1 = "%.2f" % ser1_total
puts "Target IC serial cost: $#{total_ser1}"
#puts "Remaining total cost $#{total_operating_costs - sum - spm_total - mpm_total - ser1_total}"

# monograph percents by member (to calculate serial percents)
total_mono_cost = spm_total + mpm_total
#total_mono_cost = spm_total
percents = PHDBUtils.calc_member_monograph_percent_contribution(cost_struct, total_mono_cost)

# calc serial costs for members w/o serial data
no_serial_sum = 0.0
no_serials_list.each do |mem|
  mem_percent = percents[mem]
  mem_cost = (mem_percent * ser1_total)
  cost_struct[mem][2] = mem_cost
  cost_struct[mem][3] = mem_cost
  no_serial_sum += mem_cost
end
no_serial_sum_str = "%.2f" % no_serial_sum
amount_serial_remaining = ser1_total - no_serial_sum
puts "Calculating new ave cost per vol for ic serials..."
new_ave_cost_per_vol_serial = PHDBUtils.calc_adjusted_ic_serial_cost_per_vol(ave_cost_per_vol, no_serial_sum)
puts "Total sum of non-serial member contributions using monograph percentage: $#{no_serial_sum_str}"
puts "Remaining serial cost to be distributed amongst other members: $#{amount_serial_remaining}"
puts "New average cost per volume for serials: $#{new_ave_cost_per_vol_serial}"

### serial costs, calculated by serial cluster - full amount ###

if calc_full_serial
  ser2_total = no_serial_sum
  puts "\nIn-copyright Serial costs, using full serial amount:"
  serial_member_list.each do |smember|
    result = PHDBUtils.calc_ic_serial_cost_for_member_by_count2(smember, ave_cost_per_vol)
    if not result
      puts "No data for #{smember}"
      next
    end
    xresult = result*xfactor
    cost_f = "%.2f" % result.to_f
    cost_f2 = "%.2f" % xresult.to_f
    ser2_total += result.to_f
    puts "\t#{smember}\t#{cost_f}"
    cost_struct[smember][2] = result.to_f
  end
  total_ser2 = "%.2f" % ser2_total
  puts "Total IC ser cost (full): $#{total_ser2}"
  puts "Remaining total cost (full) $#{total_operating_costs - sum - spm_total - mpm_total - ser2_total}"
end

### serial costs, calculated by serial cluster - reduced amount ###
ser3_total = no_serial_sum
puts "\nIn-copyright Serial costs, using reduced serial amount:"
serial_member_list.each do |smember|
  result = PHDBUtils.calc_ic_serial_cost_for_member_by_count2(smember, new_ave_cost_per_vol_serial)
  if not result
    puts "No data for #{smember}"
    next
  end
  xresult = result*xfactor
  cost_f = "%.2f" % result.to_f
  cost_f2 = "%.2f" % xresult.to_f
  ser3_total += result.to_f
  puts "\t#{smember}\t#{cost_f}"
  cost_struct[smember][3] = result.to_f
end
total_ser3 = "%.2f" % ser3_total
puts "Total IC ser cost (reduced): $#{total_ser3}"
puts "Remaining total cost (reduced): $#{total_operating_costs - sum - spm_total - mpm_total - ser3_total}"


### produce member report ###
puts "FINAL REPORT\n"
puts "\nmember_id\tpd_cost\tspm_cost\tmpm_cost\t% ser\tserial-full\tserial-reduced"
cost_struct.each { |key, value|
  mem_ratio = percents[key]
  puts "#{key}\t#{per_member}\t#{value[0]}\t#{value[1]}\t#{mem_ratio}\t#{value[2]}\t#{value[3]}"
}


puts "\ndone."
