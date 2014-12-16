require 'hathidata';
require 'hathidb';

=begin

From: Jeremy.

Hi Martin and Tim,

I wonder if you could help me with a little more data on this. I'm 
trying to get a list, including bibliographic data (title, imprint, 
publication date, author) for works that institutions would have 
been eligible to access under Section 108. This would be:

Works that we have determined to be out of print
  -intersect-
Works that institutions provided LM or BRT information about
  -intersect-
Works that are monographs (not serials). 

=end

db   = Hathidb::Db.new();
conn = db.get_conn();

q = %w[
  SELECT DISTINCT h3.volume_id
  FROM holdings_htitem_htmember_jn AS h3
  JOIN hathi_files AS hf
  ON (h3.volume_id = hf.htid)
  WHERE hf.rights = 'opb'
  AND (h3.lm_count >= 1 OR h3.brt_count >= 1)
].join(' ');

Hathidata.write('op_lm_brt_monographs.tsv') do |hdout|
  conn.query(q) do |row|
    hdout.file.puts(row[:volume_id]);
  end
end
