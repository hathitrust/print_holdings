=begin

Takes as input:

1) a file where the first column are sorted volume_ids 
2) a sorted and trimmed hathifile
3) and a list of the fieldnames in the sorted and trimmed hathifile
   and adds the fields from the hathifile to the input file when 
   volume_ids match

=end

TAB = "\t"

input_file       = ARGV.shift
sorted_hathifile = ARGV.shift
fieldnames       = ARGV

hdin    = File.open(input_file)
hdin_h  = File.open(sorted_hathifile)
outfile = File.open("#{input_file}.out", "w")
logfile = File.open("log.txt", "w")

# Enumerators for the infiles
enum   = hdin.lines.each
enum_h = hdin_h.lines.each

# For keeping track of where we are / what we've done
offset_h       = 0
last_volume_id = ""
last_match     = ""

enum.each_with_index do |line, i|
  line.chomp!
  if line.start_with?("volume_id") && i == 0 then
    # Copy header + appended fields from infile to outfile
    new_header = (line.split(TAB) + fieldnames).join(TAB)    
    outfile.puts(new_header)
    next
  end

  volume_id = line.split(TAB).first
  logfile.puts("(#{i}) look for #{volume_id}, offset #{offset_h}")
  
  # If there's an empty volume_id in the infile: output and move on
  if volume_id.nil? || volume_id.strip.empty?
    outfile.puts(line)
    next
  end

  # If the volume_id in the infile repeats, we don't want to increment 
  # the enumerator on the hathifile.
  if volume_id == last_volume_id
    outfile.puts("#{line}\t#{last_match}")
    next
  end
  
  # Increment enum_h until there's a volume_id match
  look_for = volume_id + "\t"
  enum_h.with_index(offset_h) do |line_h, j|
    if line_h.start_with?(look_for)
      line_h.chomp!
      cols_h      = line_h.split(TAB)
      volume_id_h = cols_h.shift
      last_match  = ""
      logfile.puts("found matching #{volume_id_h} on line #{j}")
      offset_h   = j
      last_match = cols_h.join(TAB)
      break
    end
  end
  
  last_volume_id = volume_id
  outfile.puts("#{line}\t#{last_match}")
end

hdin.close()
hdin_h.close()
outfile.close()
logfile.close()
