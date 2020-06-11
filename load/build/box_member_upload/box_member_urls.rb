#!ruby
require 'hathidb'
require 'hathiquery'

db   = Hathidb::Db.new()
conn = db.get_conn()

all_members = {}
conn.query(Hathiquery.get_active_members) do |row|
  all_members[row[:member_id]] = true
end

result = `curl -s -n --list-only "ftps://ftp.box.com/Member Data/"`
result.each_line do |line|
  line.strip!
  if line.match(/^[a-z\-]+$/)

    if all_members.key?(line) then
      # Deleting seen members from the hash so we can see if there are any left
      # i.e. missing a box folder.
      all_members.delete(line)
    else
      warn "#{line} is not a member"
    end    
    puts "#{line}\tftps://ftp.box.com/Member Data/#{line}/"
  end
end

all_members.keys.each do |member|
  warn "did not see a box for member #{member}"
end

