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

puts cic_counts;

non_cic_counts = {};
non_cic_data = File.open('non_cic_average_out.tsv', 'r');
non_cic_data.each_line do |line|
  line.strip!
  m = /^(\d+)\t(\d+)\t([0-9\.]+)\t([0-9\.]+)$/.match(line);
  if m != nil && m.length == 5 then
    c = m[1];
    if !non_cic_counts.has_key?(c) then
      non_cic_counts[c] = {'open' => m[3], 'closed' => m[4]};
    end
  end
end
non_cic_data.close();

puts non_cic_counts;

sep = Proc.new {
  [['-' * 7] * 4].join("\t")
};

report = File.new('cic_counts_report.tsv', 'w');

report.puts ['Count', 'Access', 'CIC', 'Non-CIC Avg'].join("\t");
(1 .. 14).each do |c|
  report.puts sep.call;
  c = c.to_s;
  ['open', 'closed'].each do |oc|
    line = [];
    line << c.rjust(7);
    line << oc;
    line << cic_counts[c][oc].rjust(7);
    line << non_cic_counts[c][oc].rjust(7);
    report.puts line.join("\t");
  end
end

report.close();
