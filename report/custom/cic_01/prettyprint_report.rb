cic_counts = {};

crystals = File.open('cic_crystal.tsv', 'r');
crystals.each_line do |line|
  line.strip!
  m = /^(\d+)_sum\s+(\d+)_open\s+(\d+)_closed$/.match(line);
  if m != nil && m.length == 4 then
    c = m[1];
    cic_counts[c] = {'open' => m[2], 'closed' => m[3]};
  end
end
crystals.close();

avg_counts = {};
non_cic_avg = File.open('non_cic_avg.tsv', 'r');
non_cic_avg.each_line do |line|
  line.strip!
  m = /^(Open|Closed)\t(\d+)\t(\d+)$/.match(line);
  if m != nil && m.length == 4 then
    c = m[2];
    if !avg_counts.has_key?(c) then
      avg_counts[c] = {'open' => 0, 'closed' => 0};
    end
    oc = m[1].downcase;
    avg_counts[c][oc] = m[3];
  end
end
non_cic_avg.close();

sep = Proc.new {
  puts [['-' * 7] * 4].join("\t")
};

puts ['Count', 'Access', 'CIC', 'Non-CIC Avg'].join("\t");
sep.call;
(1 .. 14).each do |c|
  c = c.to_s;
  ['open', 'closed'].each do |oc|
    line = [];
    line << c.rjust(7);
    line << oc;
    line << cic_counts[c][oc].rjust(7);
    line << avg_counts[c][oc].rjust(7);
    puts line.join("\t");
  end
  sep.call;
end
