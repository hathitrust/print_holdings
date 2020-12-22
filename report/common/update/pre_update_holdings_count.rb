require 'hathidata';

# Takes a list of member_ids.
# For each, count the monos, multis and serials both in holdings_memberitem 
# and in the files in /memberdata/. Output and give a diff for each. Takes 
# a list of member_ids as input. 
# Looks extra pretty if piped through " | column -t "

class Counter

  attr_reader :member_id, :counts;

  def initialize (member_id)
    @member_id = member_id;
    @counts    = {
      'mono'   => {'old' => 0, 'new' => 0},
      'multi'  => {'old' => 0, 'new' => 0},
      'serial' => {'old' => 0, 'new' => 0},
    };
  end

  def get_counts ()
    %w[mono multi serial].each do |t|
      counts[t]['old'] = get_old_count(t);
      counts[t]['new'] = get_new_count(t);
    end

    return self;
  end

  def get_old_count (item_type)
    dpath = "loadfiles/HT003_#{@member_id}.#{item_type}.tsv";
    hd    = Hathidata::Data.new(dpath);
    count = %x{wc -l #{hd.path}}.strip.split(" ")[0];
    return count.to_i;
  end

  def get_new_count (item_type)
    dpath = "memberdata/#{@member_id}/HT003_#{@member_id}.#{item_type}.tsv";
    hd    = Hathidata::Data.new(dpath);
    count = %x{wc -l #{hd.path}}.strip.split(" ")[0];
    return count.to_i;
  end

  def get_percent (item_type)
    if (counts[item_type]['old'] == 0) or (counts[item_type]['new'] == 0)
      return 'N/A';
    end
    
    ratio = (counts[item_type]['new'] - counts[item_type]['old']) / counts[item_type]['old'].to_f;
    return (ratio * 100).round(2);
  end

end

if $0 == __FILE__ then
  counts = [];
  # Get data for each member.
  ARGV.each do |m|
    c = Counter.new(m);
    counts << c.get_counts();
  end

  # Output header
  header = ['member_id'];
  %w[mono multi serial].each do |t|
    %w[old new diff].each do |s|
      header << "#{t}_#{s}";
    end
  end
  header << "tot_diff";
  %w[mono multi serial].each do |t|
    header << "%#{t}_diff";
  end
  puts header.join("\t");

  # Output lines of data.
  counts.each do |c|
    line = [];
    line << c.member_id;
    %w[mono multi serial].each do |t|
      %w[old new].each do |s|
        line << c.counts[t][s];
      end
      # Add diff for each type.
      line << c.counts[t]['new'] - c.counts[t]['old'];
    end    
    line << (line[3] + line[6] + line[9]); # Total diff

    %w[mono multi serial].each do |t|
      # Add diff% for each type.
      line << c.get_percent(t)
    end

    puts line.join("\t");
  end

end
