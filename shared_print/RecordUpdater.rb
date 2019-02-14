require 'hathidata';
require 'hathidb';
require 'hathilog';

class RecordUpdater

  def main
    # Setup
    infn  = ARGV.shift;
    @hdin = Hathidata::Data.new(infn).open('r');
    @log  = Hathilog::Log.new();    
    db    = Hathidb::Db.new();
    @conn = db.get_conn();

    # Prep queries
    @count_matches_q = @conn.prepare(
      %w<
        SELECT COUNT(*) AS c
        FROM shared_print_commitments
        WHERE member_id = ? AND local_oclc = ? AND local_id = ?
      >.join(' ')
    );

    # If we don't get a match in main table, see if it was deprecated.
    @check_deprecated_q = @conn.prepare(
      %w<
        SELECT deprecation_date, deprecation_status
        FROM shared_print_deprecated
        WHERE member_id = ? AND local_oclc = ? AND local_id = ?
      >.join(' ')
    );
    
    @input_req = %w[member_id local_oclc local_id];
    @input_opt = %w[local_bib_id local_item_id oclc_symbol local_item_location local_shelving_type lending_policy scanning_repro_policy];
    @col_map   = {};
    @header_line       = "";
    @current_line_data = "";
    @current_line_no   = 0;

    # Run
    begin
      parse();
    rescue StandardError => e
      puts e.message;
      puts "Died while reading file: #{infn}, line: #{@current_line_no}.";
      puts "Data:\n#{@header_line}\n#{@current_line_data}";
    ensure
      # Teardown
      @conn.close();
      @hdin.close();      
    end
    # __F_I_N__
  end
  
  def parse
    header_checked = false;
    
    @hdin.file.lines.each do |line|
      catch :next_line do
        line.strip!
        @current_line_data = line;
        @current_line_no   += 1;
        if !header_checked then
          check_header(line);
          header_checked = true;
        else
          process_line(line);
        end
      end
    end
  end

  def check_header (line)
    header_cols = line.split("\t");
    # Header must include req elements
    @input_req.each do |ir|
      if !header_cols.include?(ir) then
        @log.f("header missing required col #{ir}");
        raise "died";
      end
    end
    # Header may include optional elements,
    # but any unknown elements will be ignored.
    i = 0;
    header_cols.each do |hc|
      if !(@input_req + @input_opt).include?(hc) then
        @log.w("header includes invalid column #{hc}, it will be ignored.");
      else
        @log.d("#{hc} is in column #{i}");
        @col_map[hc] = i; # remember e.g. resolved_oclc is in column 3.
      end
      i+=1;
    end
    @header_line = line;
  end

  # Update shared_print_commitments accordingly.
  def process_line (line)
    cols = line.split("\t");
    # Get the identfying/required fields
    member_id  = cols[@col_map['member_id']];
    local_oclc = cols[@col_map['local_oclc']];
    local_id   = cols[@col_map['local_id']];

    # Only update if we find exactly one matching record.
    cm = count_matches(member_id, local_oclc, local_id);
    if cm != 1 then
      @log.w("#{cm} matches for {member_id:#{member_id}, local_oclc:#{local_oclc}, local_id:#{local_id}}");
      check_deprecated(member_id, local_oclc, local_id).each do |dep_row|
        @log.i("Deprecated on #{dep_row[:deprecation_date]} with status #{dep_row[:deprecation_status]}");
      end
      throw :next_line;
    end
    
    # Get the updating/optional fields.
    updating_fields = [];
    updating_vals   = [];
    @input_opt.each do |col_name|
      if @col_map.key?(col_name) then
        updating_fields << "#{col_name} = ?";
        updating_vals   << cols[@col_map[col_name]];
      end
    end
    
    # Construct update q
    update_sql = %W<
      UPDATE shared_print_commitments 
      SET #{updating_fields.join(', ')}
      WHERE member_id = ? AND local_oclc = ? AND local_id = ?
    >.join(' ');
    puts "#{update_sql} ; -- #{updating_vals.join(', ')}, #{[member_id, local_oclc, local_id].join(', ')}";
  end

  def count_matches(member_id, local_oclc, local_id)
    c = 0;
    @count_matches_q.enumerate(member_id, local_oclc, local_id) do |row|
      c = row[:c];
    end
    return c;
  end

  def check_deprecated(member_id, local_oclc, local_id)
    deprecated = [];
    @check_deprecated_q.enumerate(member_id, local_oclc, local_id) do |row|
      deprecated << row;
    end
    return deprecated;
  end
end

if $0 == __FILE__ then
  RecordUpdater.new().main();
end
