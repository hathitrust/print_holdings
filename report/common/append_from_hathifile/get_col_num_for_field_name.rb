field_list_file   = ARGV.shift
extract_col_names = ARGV.uniq

TAB   = "\t"
COMMA = ","

field_list        = `head -1 #{field_list_file}`
field_list_arr    = field_list.chomp.split(TAB)

extract = {} # col_num : col_name

extract_col_names.each do |col_name|
  col_num = field_list_arr.find_index(col_name)
  if col_num.nil?
    puts "No such column name (#{col_name}) in #{field_list_file}. Look:\n"
    puts field_list_arr.join("\n")
    exit 1
  end
  extract[col_num+1] = col_name
end

sorted_keys = extract.keys.sort

puts ["colnums", TAB, sorted_keys.join(COMMA)].join
puts ["colnames", TAB, sorted_keys.map{|sk| extract[sk]}.join(COMMA)].join
