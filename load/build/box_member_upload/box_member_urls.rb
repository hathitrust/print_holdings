#!ruby
require 'hathidb'
require 'hathiquery'

db   = Hathidb::Db.new()
conn = db.get_conn()

# Get members from db so we can see if anyone is missing
all_members = {}
conn.query(Hathiquery.get_active_members) do |row|
  all_members[row[:member_id]] = true
end
all_members.delete("hathitrust")

# Get shared print members so we can see that they have shared print subfolders
shared_print_members = {}
conn.query(Hathiquery.get_shared_print_members) do |row|
  shared_print_members[row[:member_id]] = true
end

TOP_FOLDER  = "ftps://ftp.box.com/Member Data"
SUB_FOLDERS = ["analysis", "print holdings", "shared print"]

# Get all folders under top folder
result = `curl -s -n --list-only "#{TOP_FOLDER}/"`
result.each_line do |line|
  line.strip!
  if line.match(/^[a-z\-]+$/)
    member_id = line
    if all_members.key?(member_id) then
      # Deleting seen members from the hash so we can see if there are any left
      # i.e. missing a box folder.
      all_members.delete(member_id)
    else
      # These should be moved out manually.
      warn "#{member_id} is not an active member, please move out manually"
      next
    end

    # Check make sure each folder contains the subfolders
    # Create missing subfolders
    # (only create shared print if they are a shared print member)
    sub_result = `curl -s -n --list-only "#{TOP_FOLDER}/#{member_id}/"`.split("\n")

    SUB_FOLDERS.each do |subf|
      if !sub_result.include?(subf) then
        if subf == "shared print" && !shared_print_members.key?(member_id) then
          # Member is missing a "shared print" subfolder,
          # but is not a shared print member, so let it slide.
          next
        end
        warn "Member #{member_id} is missing subfolder #{subf}. Creating it now..."
        # Create sub-folder
        res = `curl -n --ftp-create-dirs "#{TOP_FOLDER}/#{member_id}/#{subf}/"`
        warn res
      end
    end
    # Output to file:
    puts "#{member_id}\t#{TOP_FOLDER}/#{member_id}/"
  end
end

all_members.keys.each do |member|
  warn "did not see a box for member #{member}"
end
