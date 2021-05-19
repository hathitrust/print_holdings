require 'hathidb'
require 'hathidata'
require 'hathienv'

# Generates 1 or more .csv files used for registering commitments with OCLC.
#
# $ ruby oclc_registration_csv.rb member_a member_b ... member_z
#
# See (some of the) spec in https://tools.lib.umich.edu/jira/browse/HT-2735
# and also in https://tools.lib.umich.edu/jira/browse/HT-2746 .
# If the result is more than @@max_lines, it will be split into .1 .2 .3 etc.
# Also if the results have more than one OCLC symbol it will be split.

unless Hathienv::Env.require_minimum_ram(2000)
  raise "Put a little more mustard on it."
end

class Report
  @@max_lines = 300000 - 1 # leave room for header
  @@db        = Hathidb::Db.new()
  @@conn      = @@db.get_conn()

  # A member_id may have 1+ oclc_symbols
  get_symbol_sql = %w<
    SELECT DISTINCT UPPER(oclc_symbol) AS sym
    FROM shared_print_commitments 
    WHERE member_id = ?
    ORDER BY sym
  >.join(' ')
  @@get_symbol_q = @@conn.prepare(get_symbol_sql)

  @@sql = %w<
    SELECT
      local_oclc              AS 'OCLC_Number',
      MAX(local_id)           AS 'LSN',
      ''                      AS 'Barcode',
      UPPER(oclc_symbol)      AS 'InstitutionSymbol_852$a',
      local_item_location     AS 'HoldingLibrary_852$b',
      ''                      AS 'CollectionID',
      'Committed to Retain'   AS 'ActionNote_583$a',
      DATE_FORMAT(MAX(committed_date), "%Y%m%d") AS 'ActionDate_583$c',
      '20421231'              AS 'ExpirationDate_583$d',
      ''                      AS 'MethodofAction_583$i',
      ''                      AS 'Status_583$l',
      ''                      AS 'PublicNote_583$z',
      ''                      AS 'ProgramName_583$f',
      ''                      AS 'MaterialsSpecified_583$3'
    FROM shared_print_commitments
    WHERE member_id = ? AND UPPER(oclc_symbol) = ?
    GROUP BY local_oclc, UPPER(oclc_symbol), local_item_location
  >.join(' ')
  @@query = @@conn.prepare(@@sql)

  def initialize(member_id)
    @member_id = member_id
    @symbol = ""
    
    read_collection()
    
    @@get_symbol_q.enumerate(@member_id) do |row|
      run(row[:sym])
    end
  end

  # Get OCLC collection id from file
  def read_collection    
    @collection_id_map = {}
    map_fn = 'shared_print_commitments/member_collection_id.tsv'
    Hathidata.read(map_fn) do |line|
      line.strip!
      (id, name, sym) = line.split("\t")
      @collection_id_map[sym] = id
    end
  end
  
  def run(symbol)    
    collection_id = @collection_id_map[symbol]
    puts "Running member:#{@member_id}, symbol:#{symbol}, collection:#{collection_id}"

    f_cnt  = 0 # number of files, given the @@max_lines constraint
    dir    = "reports/shared_print/oclc_registration"
    hdout  = nil
    @@query.enumerate(@member_id, symbol).each_slice(@@max_lines) do |slice|
      f_cnt += 1
      fn     = "#{dir}/1036726.HATHI.sharedprint_#{symbol}_$ymd.#{f_cnt}.csv"
      hdout  = Hathidata::Data.new(fn).open('w')
      header = true
      slice.each do |row|
        # Turn row into hash, for easier manipulation
        rowh = row.to_h

        # Might remove this condition at some point,
        # but right now only add real values to the col if it's an RLF.
        unless ['nrlf','srlf'].include?(@member_id) then
          rowh['HoldingLibrary_852$b'] = ''
        end

        rowh['CollectionID'] = collection_id
        if header then
          hdout.file.puts(rowh.keys.join(','))
          header = false
        end
        hdout.file.puts(rowh.values.join(','))
      end
      hdout.close()
    end

    # OCLC only wants file numbering if there is more than one file to number.
    # So if there is only one, un-number it after the fact.
    if f_cnt == 1 then
      old_name = hdout.file.path.to_str
      new_name = old_name.gsub('.1.', '.')
      `mv #{old_name} #{new_name}`
      puts "renamed to #{new_name}"
    end    
  end
end

if $0 == __FILE__ then
  ARGV.each do |member_id|
    Report.new(member_id)
  end
end
