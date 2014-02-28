require 'hathidb';
require 'hathiquery';
require 'hathilog';
require 'cost_calculator';

### cost_report.rb ###
# This script implements the cost calculation for hathitrust members.
# It currently calculates serials using the 'remove non-serial-members' approach,
# although there's only one partner who hasn't submitted serial data yet (USU).
log = Hathilog::Log.new();
log.d("Started");

if ARGV.length != 2
  log.e("Usage: ruby calc_cost.rb <infrastructure_cost> <num_of_members>\n")
  exit;
end

total_operating_costs = Integer(ARGV[0])
total_members         = Integer(ARGV[1])

db   = Hathidb::Db.new();
conn = db.get_conn();

calc_full_serial = true
xfactor = 1.0

monograph_members = CostCalc.get_monograph_list(conn)
serial_members    = CostCalc.get_serial_list(conn);
no_serials_list   = monograph_members - serial_members;

# Average Cost Per Volume Calculations
ave_cost_per_vol = CostCalc.calc_ave_cost_per_vol(total_operating_costs, conn)
puts "Ave Cost Per Vol: #{ave_cost_per_vol}"

ave_cost_per_vol_ic_spm = CostCalc.calc_adjusted_ic_spm_ave_cost_per_vol(ave_cost_per_vol, conn)
puts "Adjusted Ave Cost Per IC SPM Vol: #{ave_cost_per_vol_ic_spm}"

ave_cost_per_vol_ic_mpm = CostCalc.calc_adjusted_ic_mpm_ave_cost_per_vol(ave_cost_per_vol, conn)
puts "Adjusted Ave Cost Per IC MPM Vol: #{ave_cost_per_vol_ic_mpm}"

### public domain costs ###
public_costs = CostCalc.calc_pd_costs(ave_cost_per_vol, conn)
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
monograph_members.each{|x| cost_struct[x] = [nil, nil, nil, nil]}

### singlepart monograph costs ###
spm_total = 0.0
unadj_spm_total = 0.0
puts "\nIn-copyright Singlepart Monograph costs:"
target_single_cost = CostCalc.calc_total_ic_singlepart_monograph_cost(ave_cost_per_vol, conn)
# calc unadjusted amount
monograph_members.each do |mmember|
  result = PHDBUtils::calc_ic_singlepart_monograph_cost_for_member(mmember, ave_cost_per_vol)
  unadj_spm_total += result.to_f
end
# calc adjusted amount
monograph_members.each do |mmember|
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
target_multi_cost = CostCalc.calc_total_ic_multipart_monograph_cost(ave_cost_per_vol, conn)
monograph_members.each do |mmember|
  result = PHDBUtils::calc_ic_multipart_monograph_cost_for_member(mmember, ave_cost_per_vol)
  unadj_mpm_total += result.to_f
end
monograph_members.each do |mmember|
  result = PHDBUtils::calc_ic_multipart_monograph_cost_for_member(mmember, ave_cost_per_vol_ic_mpm)
  cost_f = "%.2f" % result.to_f
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
puts "\nIn-copyright Serial costs:"
ser1_total = CostCalc.calc_total_ic_serial_cost(ave_cost_per_vol, conn)
total_ser1 = "%.2f" % ser1_total
puts "Target IC serial cost: $#{total_ser1}"
#puts "Remaining total cost $#{total_operating_costs - sum - spm_total - mpm_total - ser1_total}"

# monograph percents by member (to calculate serial percents)
total_mono_cost = spm_total + mpm_total
percents = CostCalc.calc_member_monograph_percent_contribution(cost_struct, total_mono_cost, conn)

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
new_ave_cost_per_vol_serial = CostCalc.calc_adjusted_ic_serial_cost_per_vol(ave_cost_per_vol, no_serial_sum, conn)
puts "Total sum of non-serial member contributions using monograph percentage: $#{no_serial_sum_str}"
puts "Remaining serial cost to be distributed amongst other members: $#{amount_serial_remaining}"
puts "New average cost per volume for serials: $#{new_ave_cost_per_vol_serial}"

### serial costs, calculated by serial cluster - full amount ###
if calc_full_serial
  ser2_total = no_serial_sum
  puts "\nIn-copyright Serial costs, using full serial amount:"
  serial_members.each do |smember|
    result = CostCalc.calc_ic_serial_cost_for_member_by_count2(smember, ave_cost_per_vol, conn)
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
serial_members.each do |smember|
  result = CostCalc.calc_ic_serial_cost_for_member_by_count2(smember, new_ave_cost_per_vol_serial, conn)
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

log.d("Finished.");
