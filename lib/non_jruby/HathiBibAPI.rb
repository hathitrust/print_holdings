require 'net/http';
require 'json';

# Minimal wrapper for HathiTrust Bib API.
# Does not work in JRuby for some reason.

class HathiBibAPI
  @@host = 'https://catalog.hathitrust.org';
  @@base = 'api/volumes';
  @@max_slice = 20;
  @@valid_levels = %w[brief full];
  @@valid_identifiers = %w{oclc lccn issn isbn htid recordnumber};
  
  # :symbol key gives path to that symbol in a results hash
  @@result_shortcuts = {
    'recordUrl'      => 'records',
    'titles'         => 'records',
    'isbns'          => 'records',
    'issns'          => 'records',
    'oclcs'          => 'records',
    'lccns'          => 'records',
    'publishDates'   => 'records',
    'orig'           => 'items',
    'fromRecord'     => 'items',
    'htid'           => 'items',
    'itemURL'        => 'items',
    'rightsCode'     => 'items',
    'lastUpdate'     => 'items',
    'enumcron'       => 'items',
    'usRightsString' => 'items',
  }
  
  def initialize (level = 'brief')
    if !@@valid_levels.include?(level) then
      raise ArgumentError.new("Invalid level argument '#{level}'. Valid: #{@@valid_levels.join(',')}");
    end
    @level = level;
  end

  # Pass in any number of ocns.
  def oclc(*ocns)
    generate_calls('oclc', *ocns);
  end

  # Will take any identifier and any number of values and make 20-sized slices,
  # make request(s) and return ressults.
  # generate_calls('oclc', [1,2,3]) will make call to api/volumes/#{mode}/json/oclc:1|oclc:2|oclc:3
  # Use this to build shortcut methods like oclc() above.
  def generate_calls (identifier, *values)
    if !@@valid_identifiers.include?(identifier) then
      raise ArgumentError.new("Invalid identifier '#{identifier}'. Valid: #{@@valid_identifiers.join(',')}");
    end
    results = [];
    values.each_slice(@@max_slice) do |value_slice|
      results << call(value_slice.map{|v| "#{identifier}:#{v}"}.join('|'));
    end
    return results;
  end

  # call api and return results
  def call (path_values)
    path = [@@base, @level, 'json', path_values].join('/');
    uri  = URI.parse(URI.encode([@@host, path].join('/')));
    STDERR.puts uri;
    data = Net::HTTP.get(uri);
    return JSON.parse(data);
  end

  # get specific values out of a result set
  def self.extract_from_results (results, extract)
    extracted = [];
    extract.each do |x|
      if !@@result_shortcuts.include?(x) then
        raise ArgumentError.new("Invalid result shortcut '#{x}'. Valid: #{@@result_shortcuts.keys.join(',')}");
      end
    end

    extract.each do |x|
      section = @@result_shortcuts[x];
      results.each do |r|
        r.keys.each do |rk|
          if r[rk][section].empty? then
            extracted << "";
          else
            section_keys = r[rk][section].keys;
            section_keys.each do |sk|
              extracted << r[rk][section][sk][x].join(';');
            end
          end
        end
      end
    end
    
    return extracted;
  end
  
end

if $0 == __FILE__ then
  hba = HathiBibAPI.new();
  res = JSON.pretty_generate(hba.oclc(424023, 555));
  puts res;
end
