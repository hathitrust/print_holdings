require_relative 'HathiBibAPI';

# Takes a .tsv file as input, looks stuff up in the Bib API and adds information

class APIAppender
  @@max_chunk_size = 20;
  
  def initialize (file, identifier_col, api_identifier, *extract)
    @file    = file;    # input file. write to @file.out
    @identifier_col = identifier_col; # which col in the file has the identifier used in api lookups
    @api_identifier = api_identifier; # what is the api lookup identifier
    @extract = extract; # which field(s) to extract and append to the input
    @wrapper = HathiBibAPI.new();
  end

  # open and close files, punt.
  def run
    STDERR.puts "Started #{Time.new}";
    @fhin  = File.open(@file, 'r');
    @fhout = File.open(@file + ".out", 'w');
    process_file();
    @fhout.close();
    @fhin.close();
    STDERR.puts "Finished #{Time.new}";
  end

  # get data from a single line, lookup in api and add to outfile
  def process_file_single
    @fhin.each_line do |line|
      line.strip!
      cols = line.split("\t");
      identifier = cols[@identifier_col];
      cols << extract_values(identifier);
      @fhout.puts(cols.join("\t"));
    end
  end

  # buffer up to 20 lines and make a single call
  def process_file
    line_buffer = [];
    @fhin.each_line do |line|
      line.strip!
      line_buffer << line;
      if line_buffer.size == @@max_chunk_size || @fhin.eof? then
        process_buffer(line_buffer);
        line_buffer = [];
      end
    end
  end

  def process_buffer (line_buffer)
    # put all identifiers in array
    identifiers = [];
    line_buffer.each do |line|
      cols = line.split("\t");
      identifiers << cols[@identifier_col];
    end

    # make a single call with all identifiers
    results = @wrapper.generate_calls(@api_identifier, *identifiers);
    extracted = HathiBibAPI.extract_from_results(results, *@extract);

    # extract the returned values and associate with the right line
    extracted.each_slice(line_buffer.size) do |result_slice|
      i = 0;
      result_slice.each do |result_slice_val|
        line_buffer[i] += "\t" + result_slice_val;
        i += 1;
      end
    end

    # print to outfile
    line_buffer.each do |line|
      @fhout.puts line;
    end
  end
  
  # make api call and extract desired data
  def extract_values (identifier)
    results = @wrapper.generate_calls(@api_identifier, identifier);
    extracted = HathiBibAPI.extract_from_results(results, *@extract);
    return extracted;
  end
  
end

if $0 == __FILE__ then
  APIAppender.new("test.tsv", 2, 'oclc', ['oclcs', 'titles', 'publishDates']).run();
end
