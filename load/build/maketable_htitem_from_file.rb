# Part of step 01.
# Copied from /htapps/pete.babel/Code/phdb/bin/maketable_htitem_from_file.rb
# encoding: UTF-8

require 'enum_chron_parser';
require 'hathidata';
require 'hathilog';

def get_reasons()
  reasons = %w(add bib cdpp con crms ddd del exp gatt
	       gfv ipma man ncn nfi pvt ren supp unp)
end

def generate_htitem_table(infilen, serialsfn)
  s = Hathidata::Data.new(serialsfn).open('r');
  # parse serials file to get record nums
  umrecords = Hash.new;
  s.file.each_line do |sline|
    line = sline.force_encoding("ISO-8859-1").encode("UTF-8");
    bits = line.split("\t");
    next unless bits.length > 0;
    um_id_i = bits[0].to_i;
    if um_id_i == 0
      puts "Problem with serials line '#{sline}'";
      next;
    end
    umrecords[um_id_i] = true;
  end
  s.close();

  f    = Hathidata::Data.new('builds/current/' + infilen).open('r');
  outf = Hathidata::Data.new("builds/current/hathi_full.data").open('w');
  e    = Hathidata::Data.new("builds/current/hathi_full.err").open('w');

  # parse the flatfile
  ecparser = EnumChronParser.new;
  reasons = get_reasons();
  puts "processing HathiTrust flatfile...";
  line_count = 0;
  out_count = 0;
  serial_count = 0;
  multi_count = 0;
  f.file.each_line do |line|
    line_count += 1;
    if ((line_count % 1000000) == 0)
      puts "#{line_count}...";
    end
    enum_chron = '';
    n_enum = '';
    n_chron = '';
    nline = line.force_encoding("ISO-8859-1").encode("UTF-8");
    # added .map{...sub()} to deal with end-of-column backslashes confusing the MySQL LOAD DATA statement.
    bits = nline.split("\t").map{|x| x.sub(/\\+$/, '')}
    if bits.length < 15
      puts "Line too short: '#{nline}'";
      e.file.puts(nline);
      next
    end
    bits.map{ |item| item.strip }
    um_id = bits[3].to_i;
    reason = bits[13];
    enum_chron = bits[4];
    itype = 'mono';
    if umrecords.include?(um_id)
      itype = 'serial';
      serial_count += 1;
    end
    if not reasons.include?(reason)
      puts "Bad Reason Code (#{reason}).  Line '#{nline.strip}'";
    end
    if enum_chron.length > 0
      enum_chron = enum_chron.gsub("\t", ' ');
      enum_chron = enum_chron.gsub /None/i, '';
      enum_chron = enum_chron.gsub /\?/, '';
    end
    if enum_chron.length > 0
      ecparser.parse(enum_chron);
      if ecparser.enum_str.length > 0
        n_enum = ecparser.normalized_enum.strip;
      end
      if ecparser.chron_str.length > 0
        n_chron = ecparser.normalized_chron.strip;
      end
    end
    if (itype == 'mono' and n_enum.length > 0)
      itype = 'multi';
      multi_count += 1;
    end
    outline = [bits[0..7], bits[13..14], itype, 0,  n_enum, n_chron, bits[15..19]].flatten.join("\t");
    out_count += 1;
    outf.file.puts(outline);
  end

  puts "#{line_count} lines processed.";
  puts "#{out_count} lines exported.";
  puts "#{serial_count} serials.";
  puts "#{multi_count} multis.";

  outf.close;
  f.close;
  e.close;
end

if $0 == __FILE__ then
  if ARGV.length != 2
    puts "Usage: ruby maketable_htitem_from_file.rb <hathifile> <serialfile>\n";
    exit 1;
  end

  log = Hathilog::Log.new();
  log.d("Started");
  log.d("Called with args #{ARGV[0]} and #{ARGV[1]}")
  generate_htitem_table(ARGV[0], ARGV[1])
  log.d("Finished");
end
