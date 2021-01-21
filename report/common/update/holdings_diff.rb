require 'hathidata'

# Takes a list of member_ids.
# For each, count the monos, multis, serials & totals both from old
# files in /loadfiles/ and in /memberdata/.
# Output counts and a diff for each.
# Takes a list of member_ids as input.
# Looks extra pretty if piped through " | column -t "

OLD  = 'old'
NEW  = 'new'
MON  = 'mono'
MUL  = 'multi'
SER  = 'serial'
TOT  = 'total'
DIFF = 'diff'
DIFP = 'diff%'

class Counter
  attr_reader :member_id, :counts

  def initialize(member_id)
    @member_id = member_id
    @counts = {
      MON => {OLD => 0, NEW => 0},
      MUL => {OLD => 0, NEW => 0},
      SER => {OLD => 0, NEW => 0},
      TOT => {OLD => 0, NEW => 0},
    }

    # Could add functionality to set these dynamically.
    @old_dir = "loadfiles"
    @new_dir = "memberdata/#{@member_id}"
  end

  def get_counts
    [MON, MUL, SER].each do |t|
      counts[t][OLD] = get_old_count(t)
      counts[t][NEW] = get_new_count(t)
      counts[TOT][OLD] += get_old_count(t)
      counts[TOT][NEW] += get_new_count(t)
    end
  end

  def get_old_count(item_type)
    get_count("#{@old_dir}/HT003_#{@member_id}.#{item_type}.tsv")
  end

  def get_new_count(item_type)
    get_count("#{@new_dir}/HT003_#{@member_id}.#{item_type}.tsv")
  end

  def get_count(dpath)
    hd = Hathidata::Data.new(dpath)
    count = %x{wc -l #{hd.path}}.strip.split(" ")[0]
    count.to_i
  end

  def get_diff(item_type)
    counts[item_type][NEW] - counts[item_type][OLD]
  end

  def get_perc(item_type)
    if [counts[item_type][OLD], counts[item_type][NEW]].include?(0)
      return 'N/A'
    end

    ci    = counts[item_type]
    ratio = (ci[NEW] - ci[OLD]) / ci[OLD].to_f
    return (ratio * 100).round(2)
  end

  def get_line(line)
    {
      OLD  => [counts[MON][OLD], counts[MUL][OLD], counts[SER][OLD], counts[TOT][OLD]],
      NEW  => [counts[MON][NEW], counts[MUL][NEW], counts[SER][NEW], counts[TOT][NEW]],
      DIFF => [get_diff(MON), get_diff(MUL), get_diff(SER), get_diff(TOT)],
      DIFP => [get_perc(MON), get_perc(MUL), get_perc(SER), get_perc(TOT)],
    }[line]
  end

  def get_dates
    dates = {OLD => 0, NEW => 0}
    [OLD, NEW].each do |age|
      [MON, MUL, SER].each do |item_type|
        path = case age
               when OLD
                 "#{@old_dir}/HT003_#{@member_id}.#{item_type}.tsv"
               when NEW
                 "#{@new_dir}/HT003_#{@member_id}.#{item_type}.tsv"                 
               end
        hd = Hathidata::Data.new(path)
        next unless hd.exists?
        mtime = File.mtime(hd.path).to_s[0,10]
        puts [age, item_type, mtime].join("\t")
      end
    end
  end
end

if $0 == __FILE__
  counts = []
  # Get data for each member.
  ARGV.each do |m|
    c = Counter.new(m)
    c.get_dates()
    puts "---"
    c.get_counts()
    puts [c.member_id, MON, MUL, SER, TOT].join("\t")
    [OLD, NEW, DIFF, DIFP].each do |line|
      puts [line, c.get_line(line)].flatten.join("\t")
    end
    puts "---\n"
  end
end
