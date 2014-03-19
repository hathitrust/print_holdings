class Array
  def ===(other)
    return false if (other.size != self.size)

    other_dup = other.dup
    all? do |e|
      e === other_dup.shift
    end
  end
end

class MemberCounts
  attr_accessor :member_id, :spm, :mpm, :serial, :label

  def initialize(member_id, label = '')
    @member_id = member_id
    @label = label
    @spm = @mpm = @serial = "0"
  end

  def set_type(itype, num)
    case itype
    when "mono"
      @spm = num
    when "multi"
      @mpm = num
    when "serial"
      @serial = num
    else
      $stderr.puts "problem with type '#{itype}'."
    end
  end

  def to_s
    @member_id + "\t" + @spm + "\t" + @mpm + "\t" + @serial
  end
end

class MemberHCounts
  attr_accessor :member_id, :spm_ic, :spm_pd, :mpm_ic, :mpm_pd,
  :serial_ic, :serial_pd, :label

  def initialize(member_id, label = '')
    @member_id = member_id
    @label = label
    @spm_ic = @spm_pd = @mpm_ic = @mpm_pd = @serial_ic, @serial_pd = "0"
  end

  def set_type(access, itype, num)
    key_array = [access, itype]
    case key_array
    when ['allow', 'mono']
      @spm_pd = num
    when ['allow', 'multi']
      @mpm_pd = num
    when ['allow', 'serial']
      @serial_pd = num
    when ['deny', 'mono']
      @spm_ic = num
    when ['deny', 'multi']
      @mpm_ic = num
    when ['deny', 'serial']
      @serial_ic = num
    else
      $stderr.puts "problem with type and/or access '#{itype}', '#{access}'."
    end
  end

  def to_s
    [
     @member_id, @spm_pd, @mpm_pd, @serial_pd, @spm_ic, @mpm_ic, @serial_ic
    ].map{|x| x || '0'}.join("\t")
  end
end

def build_data_hash1(infile)
  m_hash = {}
  File.readlines(infile).each do |line|
    bits = line.chomp.split("\t")
    memid,itype,num = line.chomp.split("\t")
    if m_hash.has_key?(memid)
      m_hash[memid].set_type(itype, num)
    else
      mc = MemberCounts.new(memid)
      mc.set_type(itype, num)
      m_hash[memid] = mc
    end
  end
  return m_hash
end

def build_data_hash2(infile)
  m_hash = {}
  File.readlines(infile).each do |line|
    bits = line.chomp.split("\t")
    memid,access,itype,num = line.chomp.split("\t")
    if m_hash.has_key?(memid)
      m_hash[memid].set_type(access, itype, num)
    else
      mc = MemberHCounts.new(memid)
      mc.set_type(access, itype, num)
      m_hash[memid] = mc
    end
  end
  return m_hash
end

if __FILE__ == $0 then
  infile = ARGV[0]
  format_type = ARGV[1]
  if format_type == '1'
    member_h = build_data_hash1(infile)
  elsif format_type == '2'
    member_h = build_data_hash2(infile)
  else
    puts "Not a known filetype, please enter 1 or 2."
    exit
  end

  member_h.each_pair do |k,v|
    puts v.to_s
  end
end
