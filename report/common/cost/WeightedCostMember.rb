require 'hathidata';
require 'hathidb';
require 'hathiquery';

=begin 

This report script takes a "total operating cost" as only input.
It then calculates how much each member owes in membership fee,
in order to fully cover the total operating cost.

Call thusly:

$ ruby WeightedCostMember.rb <TOTAL_OPERATING_COST>

It generates 2 datestamped .tsv files in the data/ directory:

* costreport_$ymd.tsv contains the actual cost report.
* costreport_$ymd_details.tsv contains some detailed background figures.

If you don't want those files, or want to redirect output in som other
way, add --stdout to the invocation thusly:

$ ruby WeightedCostMember.rb <TOTAL_OPERATING_COST> --stdout

=end

module Cost
  # This module has 2 classes: Report and Member.
  class Report
    # A Report tells a bunch of Members to give their counts.
    # Generates a cost per volume, and assigns costs to the Members
    # accordingly. Writes results to file.
    @@db   = Hathidb::Db.new();
    @@conn = @@db.get_conn();

    def initialize (cost)
      @total_op_cost     = cost;
      @members           = [];
      @avg_cost_per_vol  = 0;
      @cost_per_ic_spm   = 0;
      @cost_per_ic_mpm   = 0;
      @cost_per_ic_ser   = 0;
      @pd_costs          = 0;
      @participate_in_ic = 0;
      @participate_in_pd = 0;
      @sum_weights       = 0.0;
      @coverage          = 0;

      # Add --stdout to invocation if you don't want to write to files
      @to_stdout = ARGV.include?("--stdout");
      
      # Output goes here.
      unless @to_stdout
        @data = Hathidata::Data.new("costreport/costreport_$ymd.tsv").open('w');
        @deet = Hathidata::Data.new("costreport/costreport_$ymd_details.tsv").open('w');
      end
      
      return self;
    end

    # Creates members, runs them, sums it all up.
    def run
      # Create and run members. This gives counts.
      get_members_sql = "SELECT member_id, weight FROM holdings_htmember ORDER BY member_id";
      @@conn.query(get_members_sql) do |m|
        member    = Cost::Member.new(m);
        @members << member.run();
      end

      # Count how many members do ic and/or pd.
      @members.each do |m|
        if m.participates_in_pd then
          @participate_in_pd += 1;
          @sum_weights += m.weight;
        else
          if m.num_pd != 0 then
            raise "#{m.member_id} does not participate in pd, but still has pd holdings.";
          end
        end
        if m.participates_in_ic then
          @participate_in_ic += 1;
        else
          if [m.ic_spm.keys, m.ic_mpm.keys, m.ic_ser.keys].flatten.size > 0 then
            raise "#{m.member_id} does not participate in ic, but still has ic holdings.";
          end
        end
      end

      deet("@total_op_cost\t#{@total_op_cost}");
      # Get costs.
      @avg_cost_per_vol = calc_avg_cost_per_vol();
      @pd_costs         = calc_pd_costs();
      pd_cost_per_w     = (@pd_costs.to_f / @sum_weights);
      @cost_per_ic_spm  = calc_ic('mono');
      @cost_per_ic_mpm  = calc_ic('multi');
      @cost_per_ic_ser  = calc_ic('serial');

      deet("Number of members\t#{@members.size}");
      deet(%w[member_id participates_in_pd participates_in_ic ic_spm_count ic_mpm_count ic_ser_count].join("\t"));

      # Print who does what and what their ic counts are.
      @members.each do |m|
        deet(
          [
            m.member_id,
            m.participates_in_pd,
            m.participates_in_ic,
            m.ic_spm.values.inject(:+),
            m.ic_mpm.values.inject(:+),
            m.ic_ser.values.inject(:+),
          ].join("\t")
        );
      end

      # Moar print.
      deet("@participate_in_ic\t#{@participate_in_ic}");
      deet("@participate_in_pd\t#{@participate_in_pd}");
      deet("@avg_cost_per_vol\t#{@avg_cost_per_vol}");
      deet("sum_weights\t#{@sum_weights}");
      deet("pd_cost_per_w\t#{pd_cost_per_w}");

      # Assign cost to members.
      @members.each do |m|
        if m.participates_in_ic then
          m.set_costs(@cost_per_ic_spm, @cost_per_ic_mpm, @cost_per_ic_ser);
        end
        if m.participates_in_pd then
          m.costs[:pd] = pd_cost_per_w * m.weight;
        end
      end

      deet("Raw h sums");
      deet(['member_id', :spm, :mpm, :ser].join("\t"));
      @members.each do |m|
        row = [m.member_id];
        [:spm, :mpm, :ser].each do |item_type|
          row << m.count_h_sums[item_type];
        end
        deet(row.join("\t"));
      end

      ht_special_rule(); # Covert trickery.
    end

    # member_id:ht is special. Distribute its costs among the others as an additional pd cost.
    def ht_special_rule
      ht = @members.select { |m| m.member_id == 'hathitrust' }[0];
      if !ht.nil? then
        @members.delete_if { |m| m.member_id == 'hathitrust' };
        ht_cost = ht.costs[:spm] + ht.costs[:mpm] + ht.costs[:ser] + ht.costs[:pd];
        ht_cost_per_pd_member = ht_cost.to_f / @participate_in_pd;
        deet("ht_cost\t#{ht_cost}");
        deet("ht_cost_per_pd_member\t#{ht_cost_per_pd_member}");
        @members.each do |m|
          if m.participates_in_pd then
            m.costs[:extra] += ht_cost_per_pd_member;
          end
        end
      end
    end

    def calc_avg_cost_per_vol
      q = "SELECT COUNT(*) AS c FROM holdings_htitem";
      hathi_holdings = 0;
      @@conn.query(q) do |row|
        hathi_holdings = row[:c].to_i;
      end
      avg_cost_per_vol = @total_op_cost.to_f / hathi_holdings;
      deet(
        [
          "#{hathi_holdings.to_i} volumes from holdings_htitem form basis of avg_cost_per_vol:",
          "avg_cost_per_vol ==",
          "(@total_op_cost / hathi_holdings) ==",
          "#{@total_op_cost}\t/\t#{hathi_holdings}\t==\t#{avg_cost_per_vol}"
        ].join("\n\t")
      );
      return avg_cost_per_vol;
    end

    def calc_pd_costs
      q = "SELECT COUNT(*) AS c FROM holdings_htitem WHERE item_type IN ('mono', 'multi', 'serial') AND access = 'allow'";
      hathi_pd_holdings = 0;
      @@conn.query(q) do |row|
        hathi_pd_holdings = row[:c].to_i;
      end
      pd_costs = hathi_pd_holdings * @avg_cost_per_vol;
      deet(
        [
          "#{hathi_pd_holdings}\tvolumes form basis of pd_costs",
          "pd_costs ==",
          "hathi_pd_holdings * @avg_cost_per_vol ==",
          "#{hathi_pd_holdings}\t*\t#{@avg_cost_per_vol}\t==\tpd_costs"
        ].join("\n\t")
      );
      return pd_costs;
    end

    def calc_ic (item_type)
      # Get total ic cost for item_type as if they cost @avg_cost_per_vol each.
      puts "Calculating #{item_type} per-volume cost...";
      new_total = 0;
      @members.each do |m|
        ic_hash = m.get_ic_count('deny', item_type);
        ic_hash.keys.each do |k|
          new_total += (ic_hash[k] / k.to_f) * @avg_cost_per_vol;
        end
      end

      # Then get a slightly different total ic_cost, this time not using H.
      old_total_count = 0;
      old_total_sql = "SELECT COUNT(*) AS c FROM holdings_htitem WHERE item_type = ? AND access = 'deny'";
      old_sql = @@conn.prepare(old_total_sql);
      old_sql.enumerate(item_type) do |row|
        old_total_count += row[:c].to_i;
        break; # <-- not strictly necessary, but visually appealing.
      end
      old_total_cost = old_total_count * @avg_cost_per_vol;

      # Get a diff between those costs
      diff = old_total_cost - new_total;
      puts "Old total #{old_total_cost}";
      puts "New total #{new_total}";
      puts "Diff #{diff}";

      # Use that diff in setting the ic_cost_per_vol.
      ic_cost_per_vol = @avg_cost_per_vol + diff / old_total_count;
      deet("ic_#{item_type}_cost_per_vol\t#{ic_cost_per_vol}");

      return ic_cost_per_vol;
    end


    # Make sure that the cost is covered by the members.
    # Check out any diff between target cost and covered cost.
    def coverage
      @coverage = 0;

      puts %w(member_id spm mpm ser pd extra total).join("\t");
      @members.each do |m|
        m.costs[:total] = m.costs[:spm] + m.costs[:mpm] + m.costs[:ser] + m.costs[:pd] + m.costs[:extra];
        print_cost      = [:spm, :mpm, :ser, :pd, :extra, :total].map{|x| m.costs[x]}.join("\t");
        puts [m.member_id, print_cost].join("\t") unless @to_stdout;
        @coverage += m.costs[:total];
      end

      puts "Total operating cost = #{@total_op_cost}, coverage = #{@coverage}";
      diff = @total_op_cost - @coverage;
      if @coverage > @total_op_cost then
        puts "We are charging %.10f too much" % diff.abs;
      elsif @coverage < @total_op_cost then
        puts "We are charging %.10f too little" % diff.abs;
      end

      # If we are off more than one dollar per member, adjust.
      if diff.abs > (@members.size / 100.0) then
        diff_per_member = diff.to_f / @members.size;
        deet("diff_per_member\t#{diff_per_member}");
        @members.each do |m|
          m.costs[:extra] += diff_per_member;
        end
        puts "Adding #{diff_per_member} as extra cost to each member.";
        puts "Running the numbers again...";
        return coverage();
      else
        puts "... but the diff is so small that we don't care.";
      end

      data(%w(member_id spm mpm ser pd weight extra total).join("\t"));
      @members.each do |m|
        data(m.to_s);
      end

      unless @to_stdout
        @data.close();
        @deet.close();
      end
    end

    # Puts a string in @deet or stdout depending on @to_stdout
    def deet(str)
      if @to_stdout then
        puts "deet\t#{str}"
      else
        @deet.file.puts(str)
      end
    end

    # Puts a string in @data or stdout depending on @to_stdout
    def data(str)
      if @to_stdout then
        puts "data\t#{str}"
      else
        @data.file.puts(str)
      end
    end
    
  end # class Report

  class Member
    # A Member has some counts and some costs. It can only figure out the counts itself.
    @@db   = Hathidb::Db.new();
    @@conn = @@db.get_conn();

    # These do not participate in pd cost sharing.
    @@no_pd = %w[hathitrust];

    # These do not pay for their own ic volumes.
    @@no_ic = %w[ucm];
    
    attr_reader :member_id, :ic_spm, :ic_mpm, :ic_ser, :num_pd, :weight, :participates_in_ic, :participates_in_pd, :count_h_sums;
    attr_accessor :costs;

    def initialize (member_data = {})
      @member_id          = member_data[:member_id];
      @weight             = member_data[:weight].to_f;
      @participates_in_pd = true;
      @participates_in_ic = true;
      
      if @@no_pd.include?(@member_id) then
        @participates_in_pd = false;
      end
      if @@no_ic.include?(@member_id) then
        @participates_in_ic = false;
      end

      @ic_spm = {};
      @ic_mpm = {};
      @ic_ser = {};
      @num_pd = 0;
      @count_h_sums = {:spm => 0, :mpm => 0, :ser => 0};
      @costs        = {:spm => 0, :mpm => 0, :ser => 0, :pd => 0, :extra => 0, :total => 0};

      return self;
    end

    # Get all the relevant counts for this member.
    # The Report object will use them to set costs.
    def run
      if @participates_in_ic then
        puts "running #{@member_id} ic counts";
        @ic_spm = get_ic_count('deny', 'mono');
        @ic_mpm = get_ic_count('deny', 'multi');
        @ic_ser = get_ic_count('deny', 'serial');
      end
      if @participates_in_pd then
        puts "running #{@member_id} pd counts";
        @num_pd = get_pd_count();
      end

      return self;
    end

    # Name says it all.
    def get_ic_count (access, item_type)
      q  = "SELECT H_id, H_count FROM holdings_H_counts WHERE member_id = ? AND access = ? AND item_type = ?";
      pq = @@conn.prepare(q);
      h_counts = {1 => 0};
      pq.enumerate(@member_id, access, item_type) do |row|
        h_counts[row[:H_id].to_i] = row[:H_count].to_i;
      end

      return h_counts;
    end

    # Name says it all.
    def get_pd_count
      q  = "SELECT SUM(H_count) AS c FROM holdings_H_counts WHERE member_id = ? AND access = ?";
      pq = @@conn.prepare(q);
      pd_count = 0;
      pq.enumerate(@member_id, 'allow') do |row|
        pd_count = row[:c].to_i;
      end

      return pd_count;
    end

    # Given a avg_cost_per_volume, sets the ic costs.
    def set_costs (ic_spm_cost_per_vol, ic_mpm_cost_per_vol, ic_ser_cost_per_vol)
      set_cost(:spm, @ic_spm, ic_spm_cost_per_vol);
      set_cost(:mpm, @ic_mpm, ic_mpm_cost_per_vol);
      set_cost(:ser, @ic_ser, ic_ser_cost_per_vol);
    end

    # Sets a cost of a type based on counts and cost-per-vol.
    def set_cost (cost_sym, counts_hash, avg_cost_per_vol)
      count_h_sum             = counts_hash.map{ |k,v| v.to_f / k }.inject(:+);
      @count_h_sums[cost_sym] = count_h_sum;
      @costs[cost_sym]        = count_h_sum * avg_cost_per_vol;
    end

    # For the report, one line with all the costs.
    def to_s
      [
       @member_id,
       self.costs[:spm],
       self.costs[:mpm],
       self.costs[:ser],
       self.costs[:pd],
       self.weight,
       self.costs[:extra],
       self.costs[:total]
      ].join("\t");
    end

  end # class Member
end # module Cost

# Read input from STDIN and start the engine.
if __FILE__ == $0 then
  if ARGV[0].nil? then
    puts "Need an operating cost as 1st arg.";
    exit(1);
  end
  r = Cost::Report.new(ARGV.shift.to_i);
  r.run();
  r.coverage();
end
