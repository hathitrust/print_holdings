require 'hathidata';
require 'hathilog';
require 'hathidb';

# usage:
# ruby ingest_phase2.rb <member_id> <path_to_file>

class Phase2Ingest
  @@req_fields_map = {
    'ht_member_id'            => 'member_id',
    'ht_oclc_symbol'          => 'oclc_symbol',
    'ht_oclc_number'          => 'local_oclc',
    'local_bib_record_number' => 'local_id'
  };

  @@log = Hathilog::Log.new();
  @@committed_date = '2019-02-28';
  @@east_member = Regexp.new(/^(bu|colby|nyu|pitt|rochester|tufts|union)$/);
  
  def main
    @member_id = ARGV.shift;
    fn        = ARGV.shift;

    @@log.i("Started processing #{fn}");
    @hdin  = Hathidata::Data.new(fn).open('r');
    @hdout = Hathidata::Data.new("shared_print_commitments/phase2/04_parsed/#{@member_id}_$ymd.tsv").open('w');
    @hdin_enum = @hdin.file.lines;
    @header = get_header(); # map of col_name to col_number, e.g. {'local_id' => 2, ...}
    parse_file();
    @header['committed_date'] = @header.keys.size;
    @hdout.close();
    @hdin.close();
    load_into_table();
    @@log.i("Finished processing #{fn}");
  end

  def get_header
    header = {};
    cols = @hdin_enum.first.split("\t");
    @@req_fields_map.keys.each do |req_key|
      if cols.include?(req_key) then
        header_key = @@req_fields_map[req_key];
        header_pos = cols.find_index(req_key);
        header[header_key] = header_pos;
        puts "found #{req_key} (#{header_key}) at pos #{header_pos}";
      else
        raise "Could not find required field #{req_key} in file header";
      end
    end
    puts "header: #{header}";
    return header;
  end

  def parse_file
    @hdin_enum.each do |line|
      # Gotta clean up bad utf before we can even split
      # encode from https://thoughtbot.com/blog/fight-back-utf-8-invalid-byte-sequences
      line = line.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '');
      # get out the cols we want
      cols = line.split("\t");
      filtered_cols = @header.keys.map{|k| cols[@header[k]]};
      # do any special treatment
      filtered_cols = special_treatment(filtered_cols);
      # print to file that we can later load
      @hdout.file.puts(filtered_cols.join("\t"));
    end
  end

  def special_treatment (cols)
    # If e.g. we're supposed to add a static value to a column for a given member
    # ... this is where we do it.
    cols << @@committed_date;
    
    if is_east_member then
      puts "special treatment for east member"
      # retention_date: 2031-06-30
      # insert record into shared_print_other and get the id      
    end

    return cols;
  end

  def load_into_table
    sql = [
      "LOAD DATA LOCAL INFILE '#{@hdout.path}' INTO TABLE shared_print_commitments",
      "(#{@header.keys.join(', ')})"
    ].join(' ');
    puts sql;
    # db    = Hathidb::Db.new();
    # conn  = db.get_conn();
    # query = conn.prepare(sql);
    # query.execute();
    # conn.close();
  end

  def is_east_member
    @member_id =~ @@east_member
  end
  
end

if __FILE__ == $0 then
  Phase2Ingest.new().main();
end
