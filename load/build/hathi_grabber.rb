# Part of step 1.
# Ruby rewrite of 
# /htapps/pete.babel/Code/phdb/bin/grab_hathi_file.py
# by Martin Warin 2013-10-31.
# Cut some corners but the overall functionality should be identical???
# For instance, month seems to only be half supported in the .py version
# so I took it out completely in the .rb version.
# Further rewrite using Net::HTTP for download and Hathidata for storage,
# 2014-01-17.

=begin

Call thusly:

  ruby hathi_grabber.rb

or

  hathi_grabber.rb hathi_full_#{YYYY}#{MM}01.txt.gz

Without filename as argument, it will download the 
hathi_full_YYYYMM01.txt.gz for the current year and month. 

With filename it will download the specified file from 
http://www.hathitrust.org/sites/www.hathitrust.org/files/hathifiles/
(view list of files at http://www.hathitrust.org/hathifiles)

The file is placed in data/ by Hathidata and inflated,
the original .gz file removed.

=end

require 'net/http';
require 'open-uri';
require 'hathidata';
require 'hathilog';

def run (fn)
  root_url = 'http://www.hathitrust.org/hathifiles';
  puts "Grabbing #{fn} from #{root_url}";
  files = get_HT_filenames(root_url);
  retrieve_HT_file(files, fn);
  puts 'Done.';
end

def get_HT_filenames (url)
  # Look for matching urls in the root url, return list of them.
  body = open(url).read;
  hits = [];
  if !body.empty? then
    hits = body.scan(/\"(http:.+\.txt\.gz)\"/).flatten;
  end

  return hits;
end

def retrieve_HT_file (urls, targetfile)
  # Go through list of urls and look for the target file.
  # If found, download and unzip.
  success = 0;
  urls.each do |url|
    bits  = url.split('/');
    filen = bits[-1];
    if filen == targetfile then
      hd = Hathidata::Data.new('builds/current/hathi_full.txt.gz');
      if !hd.exists? then
        hd.open('wb');
        puts "Saving #{url} to #{hd.path}";

        Net::HTTP.start("www.hathitrust.org") do |http|
          begin
            http.request_get(url) do |response|
              response.read_body do |segment|
                hd.file.write(segment)
              end
            end
          end
        end
        hd.close();
      end
      hd.inflate();
      success += 1;
    end
  end
  if success < 1
    puts "Did not find the specified file (#{targetfile})";
  end
end

# Get things started, make sure there is a filename coming in.
if __FILE__== $0
  fn = ARGV.shift;
  if !fn.nil? && !fn.strip.empty? then
    fn.strip!;
    run(fn);
  else
    yyyymm = Time.new().strftime('%Y%m');
    fn = "hathi_full_#{yyyymm}01.txt.gz";
    run(fn);
  end
end
