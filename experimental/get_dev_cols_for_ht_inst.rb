require 'hathibase'

# Generate queries to update ht_institutions

class Report < Hathibase::BaseReport
  def initialize
    super

    puts "-- Alterations to ht_institutions (prod) to include new cols and data from holdings_htmember (dev)."
    
    newcols = [
      "oclc_sym VARCHAR(10) NULL",
      "weight DECIMAL(4,2) NOT NULL DEFAULT 1.00",
      "country_code CHAR(2) NOT NULL DEFAULT 'us'"
    ]
    newcols.each do |col|
      puts "ALTER TABLE ht_institutions ADD #{col};"
    end

    sql = "SELECT member_id, country_code, FORMAT(weight, 2) AS weight, oclc_sym FROM holdings_htmember"
    rows(sql) do |row|
      puts "UPDATE ht_institutions 
      SET country_code='#{row[:country_code]}', weight='#{row[:weight]}', oclc_sym='#{row[:oclc_sym]}' 
      WHERE IF(mapto_inst_id='universityofcalifornia', inst_id='#{row[:member_id]}', mapto_inst_id='#{row[:member_id]}');
      ".gsub(/\s+|\n+/, " ")
    end    
  end
end

Report.new
