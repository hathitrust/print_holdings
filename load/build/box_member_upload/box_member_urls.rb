#!ruby

require 'pry'

dirs=[
"0 - Estimate only",
"0 - Purgatory",
"0 - Waiting for data",
"1 - Waiting to be checked",
"2 - Ready to load",
"3 - Needs Further Investigation",
"4 - Being Loaded",
"5 - Done_No Further Action"
]

insts = {}

dirs.each do |dir| 
  result = `curl -s -n --list-only "ftps://ftp.box.com/Print Holdings/#{dir}/"`
  result.each_line do |line|
    line.strip!
    if line.match(/^[a-z]+$/)
      puts "#{line}	ftps://ftp.box.com/Print Holdings/#{dir}/#{line}/"
      insts[line] = dir
    end
  end
end
