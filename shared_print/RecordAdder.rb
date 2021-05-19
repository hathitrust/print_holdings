require 'hathibase'

# Generic-ish loader for spc
# Takes one argument, a tsv file.
# And after that possibly some flags.

class Script < Hathibase::BaseReport
  def initialize
    super
    @resolve_q = @conn.prepare(
      "SELECT resolved FROM oclc_concordance WHERE variant = ?"
    )
    duper
  end

  def duper
    fn   = ARGV.shift
    hdin = Hathidata::Data.new(fn).open('r')

    # Pass -D if committed_date is in the file
    # if not we assume ph2 date
    date_in_file = ARGV.include?("-D")

    # Pass -d=yyyy-mm-dd to specify which date to use
    use_date = false
    use_date_flag = ARGV.select{|x| x =~ /^-d=\d\d\d\d-\d\d-\d\d/}
    unless use_date_flag.empty?
      use_date = use_date_flag.first.match(/\d\d\d\d-\d\d-\d\d/)[0]
    end
    
    # No-op
    noop = ARGV.include?("-n")
      
    committed_date = use_date || "2019-02-28"
    header         = []
    sql_cols       = nil
    need_resolve   = false
    
    hdin.file.each_line do |line|
      cols = line.strip.split("\t")
      if header.empty? then
        # Get cols from first line
        header = cols

        # -D
        unless date_in_file
          header << "committed_date"
        end
        
        if !header.include?("resolved_oclc") then
          need_resolve = true
          header << "resolved_oclc"
        end
        sql_cols = header.join(',')
        
      else
        # Get data from all subsequent lines        

        # -D
        unless date_in_file
          cols << committed_date
        end

        if need_resolve then
          # We may need to resolve local_oclc
          # puts "look for local_oclc_i in #{header.join(',')}"
          local_oclc_i = header.find_index("local_oclc")
          # puts "found local_oclc at pos #{local_oclc_i}"
          local_oclc   = cols[local_oclc_i]
          # puts "local_oclc is #{local_oclc}"
          cols << resolve_ocn(local_oclc)
        end
        sql_vals = cols.map{|x| "'#{x}'"}.join(',')
        # Obvi not great if you are inserting thousands of records
        # or if you feel bad about not validating jack.

        sql = "INSERT IGNORE INTO shared_print_commitments (#{sql_cols}) VALUES (#{sql_vals})"
        puts sql
        unless noop
          @conn.execute(sql)
        end
      end
    end    
    hdin.close()
  end

  def resolve_ocn(ocn)
    retval = ocn
    @resolve_q.enumerate(ocn) do |row|
      retval = row[:resolved]
    end
    return retval
  end
end

Script.new
