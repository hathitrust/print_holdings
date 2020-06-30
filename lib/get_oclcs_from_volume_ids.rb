require 'hathidb';

=begin

Get the OCN from the Volume IDs. 

Read from file(s):

  $ ruby get_oclcs_from_volume_ids.rb file1 file2 file3

... or from commandline args

  $ ruby get_oclcs_from_volume_ids.rb --arg mdp.39015074057079 uc1.b4266193

=end

def run
  db    = Hathidb::Db.new();
  conn  = db.get_conn();
  
  get_volids(ARGV) do |line|
    volume_id = line.chomp
    lineout = []
    conn.query("SELECT oclc FROM holdings_htitem_oclc WHERE volume_id = '#{volume_id}'") do |row|
      lineout << row[:oclc] 
    end    
    puts "#{volume_id}\t#{lineout.join(',')}"
  end
end

def get_volids (argv)
  if argv.include?("--arg") then
    argv.delete("--arg")
    argv.each do |arg|
      yield arg
    end
  else
    argv.each do |arg|
      puts "# From file #{arg}:"
      File.open(arg).each do |line|
        yield line
      end
    end
  end
end

run if __FILE__ == $0
