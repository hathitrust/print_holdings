require 'cost_calculator';
require 'hathidata';
require 'hathidb';
require 'hathilog';

### cost_report.rb ###
# This script implements the cost calculation for hathitrust members.
# It currently calculates serials using the 'remove non-serial-members' approach.

log = Hathilog::Log.new();
log.d("Started");

if ARGV.length != 1
  log.e("Usage: ruby calc_cost.rb <total_operating_cost>");
  exit;
end

db    = Hathidb::Db.new();
conn  = db.get_conn();
hdout = Hathidata::Data.new("costreport/costreport_$ymd.txt").open('w');

calc_full_serial      = true;
xfactor               = 1.0;
total_operating_costs = Integer(ARGV[0]);
monograph_members     = CostCalc.get_monograph_list(conn);
total_members         = monograph_members.size;
if monograph_members.include?('ht') then
  # HT are not used in PD costs.
  total_members -= 1;
end

serial_members  = CostCalc.get_serial_list(conn);
no_serials_list = monograph_members - serial_members;

# Average Cost Per Volume Calculations
ave_cost_per_vol        = CostCalc.calc_ave_cost_per_vol(total_operating_costs, conn);
ave_cost_per_vol_ic_spm = CostCalc.calc_adjusted_ic_spm_ave_cost_per_vol(ave_cost_per_vol, conn);
ave_cost_per_vol_ic_mpm = CostCalc.calc_adjusted_ic_mpm_ave_cost_per_vol(ave_cost_per_vol, conn);

hdout.file.puts "Total Operating Costs\t#{total_operating_costs}";
hdout.file.puts "Ave Cost Per Vol\t#{ave_cost_per_vol}";
hdout.file.puts "Adjusted Ave Cost Per IC SPM Vol\t#{ave_cost_per_vol_ic_spm}";
hdout.file.puts "Adjusted Ave Cost Per IC MPM Vol\t#{ave_cost_per_vol_ic_mpm}";

### public domain costs ###
public_costs = CostCalc.calc_pd_costs(ave_cost_per_vol, conn);
sum = 0;
public_costs.each do |a|
  sum += a;
end
per_member = sum / total_members;

hdout.file.puts "Public domain costs:";
hdout.file.puts "Calculating PD cost based on #{total_members} members.";
hdout.file.puts "Singlepart Monos\t$#{public_costs[0]}";
hdout.file.puts "Serials\t$#{public_costs[1]}";
hdout.file.puts "Multipart Monos\t$#{public_costs[2]}";
hdout.file.puts "Total public domain costs\t$#{sum}";
hdout.file.puts "Public domain cost per member\t$#{per_member}";
hdout.file.puts "Remaining total cost\t$#{total_operating_costs - sum}";

### monograph cost structure ###
cost_struct = {};
monograph_members.each{|x| 
  cost_struct[x] = [nil, nil, nil, nil]
};

### singlepart monograph costs ###
spm_total = 0.0;
unadj_spm_total = 0.0;
hdout.file.puts "\nIn-copyright Singlepart Monograph costs:";
target_single_cost = CostCalc.calc_total_ic_singlepart_monograph_cost(ave_cost_per_vol, conn);

# calc unadjusted amount
monograph_members.each do |m|
  result = CostCalc.calc_ic_singlepart_monograph_cost_for_member(m, ave_cost_per_vol, conn);
  unadj_spm_total += result.to_f;
end

# calc adjusted amount
monograph_members.each do |m|
  result  = CostCalc.calc_ic_singlepart_monograph_cost_for_member(m, ave_cost_per_vol_ic_spm, conn);
  xresult = result * xfactor;
  cost_f  = "%.2f" % result.to_f;
  cost_f2 = "%.2f" % xresult.to_f;
  spm_total += result.to_f;
  cost_struct[m][0] = result.to_f;
end

total_spm = "%.2f" % spm_total;
hdout.file.puts "Target IC spm cost\t$#{target_single_cost}";
hdout.file.puts "Unadjusted IC SPM cost\t$#{unadj_spm_total}";
hdout.file.puts "Adjusted IC spm cost\t$#{total_spm}";
hdout.file.puts "Remaining total cost\t$#{total_operating_costs - sum - spm_total}";

### multipart monograph costs ###
mpm_total = 0.0;
unadj_mpm_total = 0.0;
hdout.file.puts "\nIn-copyright Multipart Monograph costs:";
target_multi_cost = CostCalc.calc_total_ic_multipart_monograph_cost(ave_cost_per_vol, conn);

monograph_members.each do |m|
  result = CostCalc.calc_ic_multipart_monograph_cost_for_member(m, ave_cost_per_vol, conn);
  unadj_mpm_total += result.to_f;
end

monograph_members.each do |m|
  result            = CostCalc.calc_ic_multipart_monograph_cost_for_member(m, ave_cost_per_vol_ic_mpm, conn);
  cost_f            = "%.2f" % result.to_f;
  mpm_total        += result.to_f;
  cost_struct[m][1] = result.to_f;
  hdout.file.puts "#{m}\t#{cost_f}";
end

total_mpm = "%.2f" % mpm_total;
hdout.file.puts "Target IC multi cost\t$#{target_multi_cost}";
hdout.file.puts "Unadjusted Actual IC MPM cost\t$#{unadj_mpm_total}";
hdout.file.puts "Adjusted Actual IC mpm cost\t$#{total_mpm}";
hdout.file.puts "Remaining total cost\t$#{total_operating_costs - sum - spm_total - mpm_total}";

### serial costs ###
ser1_total = CostCalc.calc_total_ic_serial_cost(ave_cost_per_vol, conn);
total_ser1 = "%.2f" % ser1_total;
hdout.file.puts "\nIn-copyright Serial costs:";
hdout.file.puts "Target IC serial cost\t$#{total_ser1}";

# monograph percents by member (to calculate serial percents)
total_mono_cost = spm_total + mpm_total;
percents = CostCalc.calc_member_monograph_percent_contribution(cost_struct, total_mono_cost);

# calc serial costs for members w/o serial data
no_serial_sum = 0.0;
no_serials_list.each do |mem|
  mem_percent         = percents[mem];
  mem_cost            = (mem_percent * ser1_total);
  cost_struct[mem][2] = mem_cost;
  cost_struct[mem][3] = mem_cost;
  no_serial_sum      += mem_cost;
end

no_serial_sum_str           = "%.2f" % no_serial_sum;
amount_serial_remaining     = ser1_total - no_serial_sum;
new_ave_cost_per_vol_serial = CostCalc.calc_adjusted_ic_serial_cost_per_vol(ave_cost_per_vol, no_serial_sum, conn);

hdout.file.puts "Calculating new ave cost per vol for ic serials...";
hdout.file.puts "Total sum of non-serial member contributions using monograph percentage\t$#{no_serial_sum_str}";
hdout.file.puts "Remaining serial cost to be distributed amongst other members\t$#{amount_serial_remaining}";
hdout.file.puts "New average cost per volume for serials\t$#{new_ave_cost_per_vol_serial}";

### serial costs, calculated by serial cluster - full amount ###
if calc_full_serial
  ser2_total = no_serial_sum;
  hdout.file.puts "\nIn-copyright Serial costs, using full serial amount:";
  serial_members.each do |smember|
    result = CostCalc.calc_ic_serial_cost_for_member_by_count2(smember, ave_cost_per_vol, conn);
    if not result
      hdout.file.puts "No data for #{smember}";
      next;
    end
    xresult     = result * xfactor;
    cost_f      = "%.2f" % result.to_f;
    cost_f2     = "%.2f" % xresult.to_f;
    ser2_total += result.to_f;
    hdout.file.puts "\t#{smember}\t#{cost_f}";
    cost_struct[smember][2] = result.to_f;
  end
  total_ser2 = "%.2f" % ser2_total;
  hdout.file.puts "Total IC ser cost (full)\t$#{total_ser2}";
  hdout.file.puts "Remaining total cost (full) $#{total_operating_costs - sum - spm_total - mpm_total - ser2_total}";
end

### serial costs, calculated by serial cluster - reduced amount ###
ser3_total = no_serial_sum;
hdout.file.puts "\nIn-copyright Serial costs, using reduced serial amount:";
serial_members.each do |smember|
  result = CostCalc.calc_ic_serial_cost_for_member_by_count2(smember, new_ave_cost_per_vol_serial, conn);
  if not result
    hdout.file.puts "No data for #{smember}";
    next;
  end
  xresult     = result * xfactor;
  cost_f      = "%.2f" % result.to_f;
  cost_f2     = "%.2f" % xresult.to_f;
  ser3_total += result.to_f;
  hdout.file.puts "\t#{smember}\t#{cost_f}";
  cost_struct[smember][3] = result.to_f;
end

total_ser3 = "%.2f" % ser3_total;
hdout.file.puts "Total IC ser cost (reduced)\t$#{total_ser3}";
hdout.file.puts "Remaining total cost (reduced)\t$#{total_operating_costs - sum - spm_total - mpm_total - ser3_total}";
hdout.close();

### produce member report ###
Hathidata.write("costreport/costreport_$ymd.tsv") do |hdout|
  hdout.file.puts "member_id\tpd_cost\tspm_cost\tmpm_cost\tserial-full\tserial-reduced";
  # k = member id, v = array of values.
  cost_struct.each do |k, v|
    # Not using in report anymore, so dropped it from output.
    #mem_ratio = percents[k];
    pd = per_member;
    if k == 'ht' then
      pd = 0;
    end
    hdout.file.puts [k, pd, v[0], v[1], v[2], v[3]].join("\t");
  end
end

conn.close();
log.d("Finished.");
