require 'hathidb';
require 'hathidata';
require 'hathiquery';
require_relative 'country_access';

# Get a detailed overlap report comparing holdings,
# including comparison of holdings enumcs against ht enumcs

def get_it(member_id)
  q = %w{
    SELECT
      hm.oclc,
      hm.local_id,
      hm.item_type,
      hc.cluster_type,
      hh.rights,
      MAX(hh.volume_id) AS volume_id
    FROM
      holdings_memberitem AS hm
    LEFT JOIN
      holdings_htitem_oclc AS hho ON (hm.oclc = hho.oclc)
    LEFT JOIN
      holdings_htitem AS hh ON (hho.volume_id = hh.volume_id)
    LEFT JOIN
      holdings_cluster_htitem_jn AS hchj ON (hh.volume_id = hchj.volume_id)
    LEFT JOIN
      holdings_cluster AS hc ON (hchj.cluster_id = hc.cluster_id)
    WHERE
      hm.member_id = ?
    GROUP BY
      hm.oclc,
      hm.local_id,
      hm.item_type,
      hc.cluster_type,
      hh.rights
    ORDER BY
      hc.cluster_type DESC,
      hm.oclc ASC
  }.join(" ");
  # psu test cases, ^^^ append to WHERE: ^^^
  # -- AND hm.oclc IN (70, 227, 321, 1139)

  # Get enumc from holdings.
  holdings_enumc_sql = %w<
    SELECT n_enum, n_chron
    FROM holdings_memberitem
    WHERE member_id = ?
    AND oclc        = ?
    AND local_id    = ?
    ORDER BY n_enum ASC, n_chron ASC
  >.join(' ');

  # Given a volume_id, get all other volumes in the cluster
  # along with their enumchron.
  hathi_sql = %w<
    SELECT MAX(hh.volume_id) AS volume_id, hh.n_enum, hh.n_chron
    FROM holdings_cluster_htitem_jn AS t1
    JOIN holdings_cluster_htitem_jn AS t2 ON (t1.cluster_id = t2.cluster_id)
    JOIN holdings_htitem            AS hh ON (t2.volume_id  = hh.volume_id)
    WHERE t1.volume_id = ?
    AND   hh.rights    = ?
    GROUP BY hh.n_enum, hh.n_chron
  >.join(' ');

  # Prep queries.
  pq               = @conn.prepare(q);
  holdings_enumc_q = @conn.prepare(holdings_enumc_sql); # <- member_id, oclc, local_id
  hathi_q          = @conn.prepare(hathi_sql);          # <- volume_id, rights

  # Get country code from member so we can do rights -> access
  country_code         = "";
  get_country_code_sql = "SELECT country_code FROM holdings_htmember WHERE member_id = ?";
  get_country_code_q   = @conn.prepare(get_country_code_sql);
  get_country_code_q.enumerate(member_id) do |row|
    country_code = row[:country_code];
  end
  country_access = CountryAccess.new(country_code);

  header = true;
  Hathidata.write("etas_extended_overlap_$ymd_#{member_id}.tsv") do |hdout|
    pq.enumerate(member_id) do |row|
      # Turn the row into a hash so we can manipulate it
      rowh = row.to_h;
      rowh["ec_match"] = "";
      rowh["access"]   = "";

      # If there is a rights value, get the access value
      if !rowh["rights"].nil? then
        rowh["access"] = country_access.check_access(rowh["rights"]);
      end

      if header then
        header_keys = rowh.keys;
        # manipulate header keys here if necessary
        hdout.file.puts(header_keys.join("\t"));
        header = false;
      end

      if rowh["volume_id"] != "" then
        # Only bother checking enumchron if there is a volume in HT
        if rowh["cluster_type"] == "mpm" then
          # Get member holding enumc
          holdings_hash = {};
          holdings_enumc_q.enumerate(member_id, rowh["oclc"], rowh["local_id"]) do |holdings_row|
            key = "#{holdings_row[:n_enum]}+#{holdings_row[:n_chron]}";
            holdings_hash[key] = 1;
          end

          # Get other HT volumes in the same cluster & their enumc
          hathi_hash = {};
          hathi_q.enumerate(rowh["volume_id"], rowh["rights"]) do |hathi_row|
            key = "#{hathi_row[:n_enum]}+#{hathi_row[:n_chron]}";
            hathi_hash[key] = hathi_row[:volume_id];
          end

          # Now, if holdings_enumc contains an empty, then repeat rowh once for each record in hathi_q
          # If not, match them up and only print matches.
          # So, was there an empty enumc in the holdings?
          empty_holdings_enumc = holdings_hash.key?("+");

          if empty_holdings_enumc then
            # List all hathi records.
            hathi_hash.each do |key,volume_id|
              rowh["volume_id"] = volume_id;
              rowh["ec_match"]  = "* #{key}";
              hdout.file.puts(rowh.values.join("\t"));
            end
          else
            # Get the intersection of member_holdings & hathi.
            overlap_keys = holdings_hash.keys & hathi_hash.keys;
            overlap_keys.sort.each do |key|
              rowh["volume_id"] = hathi_hash[key];
              rowh["ec_match"]  = key;
              hdout.file.puts(rowh.values.join("\t"));
            end
          end
        else
          # spm/ser prints here. mpm prints above
          hdout.file.puts(rowh.values.join("\t"));
        end
      end
    end
  end
end

if __FILE__ == $0 then
  db    = Hathidb::Db.new();
  @conn = db.get_conn();

  if ARGV.empty? then
    @conn.query(Hathiquery.get_us_members) do |row|
      get_it(row[:member_id]);
    end
  else
    ARGV.each do |arg|
      get_it(arg);
    end
  end
  @conn.close();
end
