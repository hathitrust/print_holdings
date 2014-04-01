require 'phdb/phdb_utils'

module PHDBUtils
  class CostEstimator
    # removed hardcoded constant MAX_H, mwarin 2013-12-13

    def initialize member_table
      @member_table = member_table || 'holdings_memberitem'
    end

    def test_item_type(itype)
      type_list = ["mono", "multi", "serial", "", nil]
      if type_list.include?(itype)
        return true
      else
        return false
      end
    end

    def select_matching_ic_volume_ids()
      conn = PHDBUtils.get_dev_conn()
      rows = conn.query("SELECT DISTINCT ho.volume_id FROM holdings_htitem_oclc as ho,
                         holdings_htitem as h, timothy_ht.#{@member_table} as mt
                         WHERE ho.oclc = mt.oclc
                         AND h.volume_id = ho.volume_id
                         AND h.access = 'deny'")
      volumes = []
      count = 0
      rows.each do |row|
        count += 1
        volumes << row[0]
      end
      conn.close()
      puts "#{volumes.length} volumes..."
      return volumes
    end

    def construct_H_counts_array(bump, volid_fn)

      conn = PHDBUtils.get_dev_conn()

      max_h = 0;
      conn.query("SELECT MAX(H_id) AS h FROM holdings_H_counts") do |row|
        max_h = row['h'];
      end
      h_array = Array.new(max_h + 2, 0)

      f = File.open(volid_fn, "r")
      count = 0
      
      # Original code that doesnt work now.
      # f.each_line { |line|
      #   v = line.strip
      #   h_row = conn.query("select H from holdings_htitem_H where volume_id = '#{v}'")
      #   puts "\tunique: #{v}" if h_row[0] == nil
      #   h_row[0] == nil ? h = 1 : h = (h_row[0][:H].to_i)+bump
      #   begin
      #     h_array[h] += 1
      #   rescue
      #     puts "Problem with line: '#{v}', Count = #{h_row[0][:H]}"
      #   end
      #   count += 1
      #   puts count if count % 100000 == 0
      # }

      # MW 2014-01-07 edit start
      f.each_line do |line|
        v = line.strip
        h_row = [];

        # Have to write to array, can't treat result set as an array.
        conn.query("select H from holdings_htitem_H where volume_id = '#{v}'") do |row|
          h_row << row[:H];
        end 

        puts "\tunique: #{v}" if h_row[0] == nil
        h_row[0] == nil ? h = 1 : h = (h_row[0].to_i) + bump
        begin
          h_array[h] += 1
        rescue
          puts "Problem with line: '#{v}', Count = #{h_row[0]}"
        end
        count += 1
        puts count if count % 100000 == 0
      end
      # MW 2014-01-07 edit end

      conn.close()
      f.close()
      return h_array
    end

    def estimate_cost(ave_cost_per_vol, bump, vidfn)
      # 'bump' is an int, indicating whether to bump the H counts by one
      # for a potential member or leave at 0 for a regular member
      if not (bump==1 or bump==0)
        puts 'Bad bump.'
        exit
      end
      h_nums = construct_H_counts_array(bump, vidfn)
      total_cost = 0.0
      h_nums.each_with_index do |h, i|
        ind = i.to_i
        next if ind == 0
        next if not h.integer?
        h_cost = (h * (ave_cost_per_vol/ind))
        total_cost += h_cost
        puts "#{ind}\t#{h}\t#{h_cost}\t#{total_cost}"
      end
      return total_cost
    end
  end
end
