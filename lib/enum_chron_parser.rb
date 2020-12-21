# parser - a parser for enum/chron data

# Takes an enumeration/chronology string and classifies all terms as either enum and chron,

# PJU - May 2012 
# Copyright (C), University of Michigan Library

class EnumChronParser
  attr_reader :enum, :chron
  
  def initialize()
    @enum = []
    @chron = []
  end

  def preprocess(str)
    str.gsub! ',', ' '
    str.gsub! /\:/, ' '
    str.gsub! ';', ''
    str.gsub!(' - ', '-')
    str.gsub! '(', ' '
    str.gsub! ')', ' '
    # remove spaces after dots except when followed by a date
    newstr = dot_sub(str)
    # recover the 'n.s.' pattern
    newstr.sub!('n.s.', 'n.s. ')
    newstr.gsub!(/\s\s/, ' ')
  end

  def dot_sub(mstr)
    # remove spaces after dots except when followed by a date
    dot_pattern = /\.\s+([^\.]+)/
    date_pattern = /\. [0-2]\d{3}/
    positions = mstr.enum_for(:scan, dot_pattern).map { Regexp.last_match.begin(0) }
    iter = 0
    positions.each do |p|
      i = p - iter
      unless (mstr[i..i+5] =~ date_pattern)
      mstr.sub!('. ', '.')
      iter += 1
      end
    end
    mstr
  end

  def enum=(e)
    #ne = normalize_enum(e)
    @enum = e
  end
  
  def chron=(c)
    @chron = c
  end

  def add_to_enum(e)
    #normal_e = normalize_enum(e)
    @enum.push(e)
  end

  def add_to_chron(c)
    #normal_c = normalize_chron(c)
    @chron.push(c)
  end

  def enum_str
    return @enum.join(" ")
  end

  def chron_str
    return @chron.join(" ")
  end

  def clear_fields
    self.enum = []
    self.chron = []
  end

  def normalized_chron
    ## TO DO:  add season codes
    # first chron
    newchron = ''
    return if @chron.length == 0
    if @chron.class == Array
      chron = @chron.join(' ')
    else
      chron = @chron
    end
    # dates
    if chron.to_i
      if chron.length == 2
        if 0 < chron.to_i < 12 
          newchron = "20" << chron
        else
          newchron = "19" << chron
        end
      else
        newchron = chron
      end
    else
      newchron = chron
    end 
    return newchron
  end
  
  def normalized_enum
    new_enum = []
    return if @enum.length == 0
    if not @enum.class == Array
      enum = [@enum]
    else
      enum = @enum
    end
    enum.each do |en|
      if en[-1] == "."
        en = en[0..-2]
      end
      ren = en.gsub /\"\\\"/, ''
      ren = en.gsub /\.\s+/, '.'
      ren = ren.gsub /\s/, ''
      ren = ren.gsub /\(/, ''
      ren = ren.gsub /\)/, '' 
      ren = ren.gsub /\[/, ''
      ren = ren.gsub /\]/, ''
      ren = ren.gsub /\,/, ''
      ren = ren.gsub /\"/, ''
      ren = ren.gsub /C[a-z]*\./i, ''
      # handle special cases
      if ren =~ /([A-Za-z0-9\.]+)\((\d{2,4})\)/
        ren = "#{$1} #{$2}"
      end
      if ren =~ /([A-Z])(\d+)/i
        ren = "#{$1}.#{$2}"
      end
      if ren =~ /\w+\.(.*)/
        ren = $1
      end
      if ren =~ /[A-Za-z]+[\s]+([\d]+)/
        ren = $1
      end
      new_enum.push(ren)
    end
    if new_enum.length > 1
      renum = new_enum.join(':')
    else
      renum = new_enum[0]
    end
    renum = renum.gsub /::/, ':'
    if renum[-1] == ":"
      renum = renum[0..-2]
    end
    if renum[0] == ":"
      renum = renum[1..-1]
    end
    return renum
  end


  def parse(input_str)
    # return if empty input
    #return if input_str.length == 0
  
    # clear current data
    clear_fields
    
    # chron patterns
    date_re = /^([a-z]{0,7}\.)?[12]\d{3}/
    month_re = /(jan|january|febr?|february|mar|march|apr|april|may|june?|july?|aug|august|sept?|september|oct|october|nov|november|dec|december)[\.\?]?/i
    day_re = /[0-3]?[0-9](th|st|rd|nd)$/i
    day_re1 = /([0-9]{1,4})(th|st|rd|nd)?/i
    day_re2 = /[0-3]?[0-9](th|st|rd|nd)?/
    month_day_re = /(#{month_re} #{day_re1})/ 
    month_span_re = /(#{month_re}-#{month_re})/
    day_span_re = /(#{day_re2}-#{day_re2})/
    date_month_span_re = /(#{date_re}\-#{month_re})/
    month_day_span_re = /(#{month_re} #{day_span_re})/ 
    seasons_re = /(spring|summer|winter|fall)/i
    
    # enum patters
    enum_re = /(v\.|n\.s\.)/
    long_num_re = /^([0-9]{5,})/

    # preprocess
    pstr = preprocess(input_str)

    ### classify enum vs chron ###
    begin
      # "pullout" parses run 1st, followed by inlines
      if (input_str =~ month_day_span_re)
        matches = input_str.scan(month_day_span_re)
        matches.each do |m|
          #$stderr.puts "'#{m}'"
          add_to_chron(m[0])
          input_str = input_str.gsub!(m[0], '')
        end
      end
      if (input_str =~ month_span_re)
        matches = input_str.scan(month_span_re)
        matches.each do |m|
          #$stderr.puts "'#{pullout}'"
          add_to_chron(m[0])
          input_str = input_str.gsub!(m[0], '')
        end
      end
      if (input_str =~ month_day_re)
        matches = input_str.scan(month_day_re)
        matches.each do |m|
          #$stderr.puts "'#{pullout}'"
          add_to_chron(m[0])
          input_str = input_str.gsub!(m[0], '')
        end
      end
      return if not (input_str =~ /[a-z0-9]/i)
    rescue Exception => e  
      puts e.message  
      puts e.backtrace.inspect 
      $stderr.puts "[Parser] Problem parsing '#{input_str}'"
      return
    end
    
    # straight inline parse of what's left
    bits = input_str.split
    # deal with date-month case
    bits.each do |b|
      if (b =~ date_month_span_re)
        sub_b = b.split('-')
        i = bits.index(b)
        bits.insert(i, sub_b[0])
        bits.insert(i+1, sub_b[1])
        bits.delete(b)
      end
    end
    bits.each do |b|
      # match chron primaries
      if (b =~ enum_re or b=~ long_num_re)
        add_to_enum(b)
      elsif (b =~ date_re or b =~ month_day_re or b =~ month_re or b =~ seasons_re)
        b.gsub!(/\-/, '') if (b =~ /\-$/)   # delete trailing '-'
        add_to_chron(b)
      else
        add_to_enum(b)
      end
    end
    
  end

  def self.parse_file(file)
    f = File.open(file).each_line do |line|
      line.strip!
      orig = line.clone
      ecp = EnumChronParser.new
      ecp.parse(line)
      n_enum  = ecp.normalized_enum
      n_chron = ecp.normalized_chron
      puts [orig, "-->", n_enum, n_chron].join("\t")
    end
  end

  
  private :add_to_enum, :add_to_chron, :preprocess, :dot_sub, :clear_fields, :enum=

end

if $0 == __FILE__ then
  EnumChronParser.parse_file(ARGV.shift)
end

