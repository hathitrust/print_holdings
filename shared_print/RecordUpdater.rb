require 'hathidata';
require 'hathidb';
require 'hathilog';

=begin

Call thusly: 
ruby RecordUpdater.rb <input_file>

Expects a .tsv file as input.
The file should have a single header line with AT LEAST the columns:

  member_id
  local_oclc
  local_id  

... and any combination of the following:

  local_bib_id
  local_item_id
  oclc_symbol
  local_item_location
  local_shelving_type
  lending_policy
  scanning_repro_policy

The script uses the required columns to look up matching records in the DB
and overwrites what's in the optional columns from the file.

So, given a file:

member_id local_oclc local_id local_item_location
loc       555        i555     basement

... and a DB record (here in hash format for readability):

{:member_id => 'loc', :local_oclc => '555', :local_id => 'i555', :local_item_location => 'shed'}

We'll look for a match and update local_item_location from 'shed' to 'basement'.
For each update we also write a revert-update to a file.

=end

class RecordUpdater

  def main
    # Setup
    infn  = ARGV.shift;
    @hdin = Hathidata::Data.new(infn).open('r');
    @undo = Hathidata::Data.new("#{infn}_undo_$ymd.sql").open('w');
    @log  = Hathidata::Data.new("#{infn}_$ymd.log").open('w');
    db    = Hathidb::Db.new();
    @conn = db.get_conn();

    # Prep queries
    @count_matches_q = @conn.prepare(%w<
      SELECT COUNT(*) AS c
      FROM shared_print_commitments
      WHERE member_id = ? AND local_oclc = ? AND local_id = ?
    >.join(' '));

    # If we don't get a match in main table, see if it was deprecated.
    @check_deprecated_q = @conn.prepare(%w<
      SELECT deprecation_date, deprecation_status
      FROM shared_print_deprecated
      WHERE member_id = ? AND local_oclc = ? AND local_id = ?
    >.join(' '));

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
      @undo.close();
      @log.close();
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
        @log.file.puts("header missing required col #{ir}");
        raise "died";
      end
    end
    # Header may include optional elements,
    # but any unknown elements will be ignored.
    i = 0;
    header_cols.each do |hc|
      if !(@input_req + @input_opt).include?(hc) then
        @log.file.puts("header includes invalid column #{hc}, it will be ignored.");
      else
        @log.file.puts("#{hc} is in column #{i}");
        @col_map[hc] = i; # remember e.g. resolved_oclc is in column 3.
      end
      i+=1;
    end

    prep_do_undo();
    @header_line = line;
  end

  def prep_do_undo
    # Get the updating fields from the column map.
    updating_fields = [];
    @input_opt.each do |col_name|
      if @col_map.key?(col_name) then
        updating_fields << col_name;
      end
    end

    # Now that we know which cols are being updated we can construct
    # the update query and know what to ask for to get the original
    # record, for undo purposes.
    update_set_str = updating_fields.map{|x| "#{x} = ?"}.join(', ');
    @update_sql = %W<
      UPDATE shared_print_commitments
      SET #{update_set_str}
      WHERE member_id = ? AND local_oclc = ? AND local_id = ?
    >.join(' ')
    @update_q = @conn.prepare(@update_sql);
    
    @get_original_q = @conn.prepare(%W<      
      SELECT #{updating_fields.join(', ')} FROM shared_print_commitments
      WHERE member_id = ? AND local_oclc = ? AND local_id = ?
    >.join(' '));
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
      @log.file.puts("#{cm} matches for {member_id:#{member_id}, local_oclc:#{local_oclc}, local_id:#{local_id}}");
      # Check if there are deprecated records.
      check_deprecated(member_id, local_oclc, local_id).each do |dep_row|
        @log.file.puts("Deprecated on #{dep_row[:deprecation_date]} with status #{dep_row[:deprecation_status]}");
      end
      throw :next_line;
    end

    # Get the value(s) for the updating field(s)
    updating_vals   = [];
    @input_opt.each do |col_name|
      if @col_map.key?(col_name) then
        updating_vals   << cols[@col_map[col_name]];
      end
    end

    # Get original record for undo purposes, write a reverse update to @undo.
    @get_original_q.enumerate(member_id, local_oclc, local_id) do |o_row|
      original = o_row.to_h;
      original.values.each do |v|
        v = v.to_s;
      end
      # We don't need these fields in the SET part.
      ["member_id", "local_oclc", "local_id"].each do |delete_field|
        original.delete(delete_field);
      end
      set_str = original.map{|k,v| "#{k} = '#{v}'"}.join(', ');
      set_str.gsub!(/''/, 'NULL');
      @undo.file.puts(%W<
        UPDATE shared_print_commitments 
        SET #{set_str}
        WHERE member_id = '#{member_id}' AND local_oclc = '#{local_oclc}' AND local_id = '#{local_id}';
      >.join(' '));
    end
    
    # do the actual update.
    @log.file.puts("#{@update_sql}; -- #{updating_vals.join(', ')}, #{[member_id, local_oclc, local_id].join(', ')}");
    @update_q.execute(*updating_vals, member_id, local_oclc, local_id);
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
