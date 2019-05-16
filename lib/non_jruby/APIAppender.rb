require_relative 'HathiBibAPI';

# Takes a .tsv file as input, looks stuff up in the Bib API and adds information

class APIAppender

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

  # get data from line, lookup in api and add to outfile
  def process_file
    @fhin.each_line do |line|
      line.strip!
      cols = line.split("\t");
      identifier = cols[@identifier_col];
      cols << extract_values(identifier);
      @fhout.puts(cols.join("\t"));
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
  APIAppender.new("/htapps/mwarin.babel/sporran_of_word_matching/data/zero_results_20190513.tsv", 0, 'recordnumber', ['oclcs']).run();
end
