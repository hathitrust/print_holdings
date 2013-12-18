# Get the CIC specific stuff first
cic_counts = {};
cic_data = File.open('step_04_out.tsv', 'r');
cic_data.each_line do |line|
  line.strip!
  m = /^(\d+)_sum\s+(\d+)_open\s+(\d+)_closed$/.match(line);
  if m != nil && m.length == 4 then
    c = m[1];
    cic_counts[c] = {'open' => m[2], 'closed' => m[3]};
  end
end
cic_data.close();
#puts cic_counts;

# Then get the stuff for all the hathi members.
hathi_counts = {};
hathi_data = File.open('hathi_cic_counts.tsv', 'r');
hathi_data.each_line do |line|
  if line =~ /^#cic/ then
    next;
  end
  line.strip!
  m = /^(\d+)\t(\d+)\t(\d+)\t(\d+)\t(\d+)\t([0-9\.]+)\t([0-9\.]+)$/.match(line);

  if m != nil && m.length == 8 then
    c = m[1];
    if !hathi_counts.has_key?(c) then
      hathi_counts[c] = {
        'open' => { # Where cic is open, hathi has x open and y closed
          'open'   => m[2],
          'closed' => m[3],
        },
        'closed' => {
          'open'   => m[4],
          'closed' => m[5],
        },
        'open_held_avg'   => m[6],
        'closed_held_avg' => m[7],
      };
    end
  else
    puts "Bad line #{line} | #{m.length}";
  end
end
hathi_data.close();
#puts hathi_counts;

cell_width = 12;
col_heads = ['CIC_H', 'Access', 'CIC_Vol', 'Hathi_Open', 'Hathi_Closed', 'Hathi_Havg'];
sep = Proc.new {
  [['-' * cell_width] * col_heads.length].join("\t")
};

# Combine the data sets, prettyprint to file.
report = File.new('cic_report_pretty.tsv', 'w');

report.puts col_heads.map{|x| x.rjust(cell_width)}.join("\t");
(1 .. 14).each do |c|
  report.puts sep.call;
  c = c.to_s;
  ['open', 'closed'].each do |oc|
    line = [];
    line << c;
    line << oc;
    line << cic_counts[c][oc];
    line << hathi_counts[c][oc]['open'];
    line << hathi_counts[c][oc]['closed'];
    line << hathi_counts[c][oc + '_held_avg'];
    report.puts line.map{|x| x.rjust(cell_width)}.join("\t");
  end
end
report.close();
