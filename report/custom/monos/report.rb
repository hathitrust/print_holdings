require 'hathidata';
require 'hathidb';
require 'hathilog';

=begin

Answer these questions:

1. How much of the Hathi digital corpus is monographs?

2. Starting from Hathi digital surrogate corpus, what is the # of print copies per title held by Hathi members?
OCLC#   Copies
394890  5 (Michigan, Cornell, Illinois, Delaware, Harvard)
432429  4 (Michigan, California, Harvard, Wisconsin)
340293  4 (Michigan, California, Illinois, Wisconsin)
...

3. Number of print monographs held by each Hathi member institution
University of Michigan  7,213,342
University of Illinois  9,234,322
UC Berkeley     7,234,342
Stanford University     7,343,234
...

4. Number of print monographs with any digital surrogate in Hathi held by each Hathi member institution
University of Michigan 6,234,234
University of Illinois 1,234,342
UC Berkeley     2,033,324
Stanford University     1,523,234

Broken down as one method per question.

=end
class Report
  attr_reader :log;
  attr_reader :conn;
  @log  = nil;
  @conn = nil;

  def initialize 
    @log = Hathilog::Log.new();
    db   = Hathidb::Db.new();
    @conn = db.get_conn();
  end

  def run
    @log.d("Started");
    q1();
    q2();
    q3();
    q4();
    @log.d("Finished");
  end

  def q1
    q = "SELECT item_type, COUNT(item_type) AS c FROM holdings_htitem GROUP BY item_type";
    @log.d(q);
    @log.d(@conn.class);
    r = self;
    Hathidata.write("reports/counts_q1.tsv") do |hdout|
      r.conn.query(q) do |row|
        i = row[:item_type];
        c = row[:c].to_i;
        item_type_counts[i] = c;
        hdout.file.puts("#{i}\t#{c}");
      end
      total = item_type_counts.values.inject(:+);
      hdout.file.puts("Total #{total}");
      hdout.file.puts(item_type_counts['mono'].to_f / total);
    end
  end

  def q2
    q = "SELECT DISTINCT hf.oclc, hhh.H FROM hathi_files AS hf JOIN holdings_htitem_H AS hhh ON (hf.htid = hhh.volume_id)";
    @log.d(q);
    r = self;
    Hathidata.write("reports/counts_q2.tsv") do |hdout|
      r.conn.query(q) do |row|
        hdout.file.puts(row.to_a.join("\t"));
      end
    end
  end

  def q3
    q = "SELECT member_id, COUNT(item_type) AS c FROM holdings_memberitem WHERE item_type = 'mono' GROUP BY member_id ORDER BY member_id";
    @log.d(q);
    r = self;
    Hathidata.write("reports/counts_q3.tsv") do |hdout|
      r.conn.query(q) do |row|
        hdout.file.puts(row.to_a.join("\t"));
      end
    end
  end

  def q4
    q = %w[
        SELECT hhhj.member_id, SUM(hhhj.copy_count)
        FROM   holdings_htitem AS hh
        JOIN   holdings_htitem_htmember_jn AS hhhj
        ON     (hh.volume_id = hhhj.volume_id)
        WHERE  hh.item_type = 'mono'
        AND    hhhj.member_id != ''
        GROUP BY hhhj.member_id
        ORDER BY hhhj.member_id
  ].join(' ');
    @log.d(q);
    r = self;
    Hathidata.write("reports/counts_q4.tsv") do |hdout|
      r.conn.query(q) do |row|
        hdout.file.puts(row.to_a.join("\t"));
      end
    end
  end
end

if $0 == __FILE__ then
  r = Report.new();
  r.run();
end
