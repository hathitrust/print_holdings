require 'hathidb';
require 'hathilog';
require 'hathidata';

=begin

2015-02-11, Jeremy York requested:

[v] # and % of UM titles present in HathiTrust
[ ] # and % of UM volumes present in HathiTrust
[v] # and % of UM serial and monograph titles present in HathiTrust
[ ] # and % of UM serial and monograph volumes present in HathiTrust
[v] # and % of volumes that are available worldwide, and separately pdus.
[ ] # of copies held (e.g., 1,021<TAB>7, meaning that there are 7 copies of 1,021 volumes at the institution

=end

module Overlap
  class Report

    @log;
    @member_id;
    @member_ids;
    @db;
    @conn;

    def initialize ()
      @log        = Hathilog::Log.new();
      @db         = Hathidb::Db.new();
      @conn       = @db.get_conn();
      @member_id  = ARGV.shift;
      @member_ids = get_member_ids();

      return self;
    end

    def main
      @hdout_deet = Hathidata::Data.new("reports/overlap_details_#{@member_id}.tsv").open('w');
      @log.d(@member_ids.join(", "));

      @total_submitted_count = 0;
      @total_in_hathi_count  = 0;

      %w[mono multi serial].each do |item_type|
        @log.d(item_type);
        get_submitted_and_overlapping(item_type);
      end

      @hdout_deet.file.puts("# TOTAL");
      @hdout_deet.file.puts("submitted:\t#{@total_submitted_count}");
      @hdout_deet.file.puts("in_hathi:\t#{@total_in_hathi_count}");
      if @total_submitted_count > 0 then
        @hdout_deet.file.puts("percent:\t#{@total_in_hathi_count.to_f / @total_submitted_count}");
      end

      get_copy_counts();
      get_rights();
      @hdout_deet.close();
    end

    def get_member_ids()
      if @member_id.nil? then
        raise "Need member_id as 1st arg";
      end

      # Return appropriate member_ids if given 'ucal' or 'test'
      # as input, otherwise just return input.

      out_arr = [];
      if @member_id == 'ucal' then
        q = "SELECT member_id FROM holdings_htmember WHERE member_parent = 'ucal'";
        @conn.query(q) do |row|
          out_arr << row[:member_id];
        end
      elsif @member_id == 'test' then
        # ucmerced has really small holdings, good for test purposes.
        out_arr = ['ucmerced'];
      else
        out_arr << @member_id;
      end
      return out_arr;
    end

    def get_submitted_and_overlapping(item_type)
      member_ids_str = @member_ids.map{|x| "'#{x}'"}.join(", ");

      sql = %W[
        SELECT DISTINCT
            hm.oclc           AS submitted,
            !ISNULL(hco.oclc) AS in_hathi
        FROM
            holdings_memberitem   AS hm
        LEFT JOIN
            holdings_cluster_oclc AS hco ON (hm.oclc = hco.oclc)
        WHERE
            hm.member_id IN (#{member_ids_str})
            AND
            hm.item_type = ?
      ].join(" ");

      q = @conn.prepare(sql);

      submitted_count = 0;
      in_hathi_count  = 0;

      Hathidata.write("reports/overlap_#{@member_id}_#{item_type}.tsv") do |hdout|
        hdout.file.puts "# submitted\tin_hathi";
        q.enumerate(item_type) do |row|
          hdout.file.puts "#{row[:submitted]}\t#{row[:in_hathi]}";
          submitted_count += 1;
          in_hathi_count  += row[:in_hathi];
        end
      end
      @hdout_deet.file.puts("# #{item_type}");
      @hdout_deet.file.puts("submitted:\t#{submitted_count}");
      @hdout_deet.file.puts("in_hathi:\t#{in_hathi_count}");
      if submitted_count > 0 then
        @hdout_deet.file.puts("percent:\t#{in_hathi_count.to_f / submitted_count}");
      end

      @total_submitted_count += submitted_count;
      @total_in_hathi_count  += in_hathi_count;
    end

    def get_copy_counts
      member_ids_str = @member_ids.map{|x| "'#{x}'"}.join(", ");
      sql = %W[
        SELECT
            COUNT(volume_id) AS no_of_vols,
            copy_count
        FROM
            holdings_htitem_htmember_jn
        WHERE
            member_id IN (#{member_ids_str})
        GROUP BY
            copy_count
        ORDER BY
            copy_count ASC
      ].join(" ");

      q = @conn.prepare(sql);

      Hathidata.write("reports/#{@member_id}_copy_counts.tsv") do |hdout|
        hdout.file.puts("no_of_vols\tcopy_count");
        q.enumerate() do |row|
          hdout.file.puts("#{row[:no_of_vols]}\t#{row[:copy_count]}");
        end
      end
    end

    def get_rights
      # This one is a slow whopper of a beast, especially for ucal.
      # For each right (ic, pd, cc, etc) count how many distinct volumes
      # are held by the @member_ids member(s).

      get_all_rights_sql = "SELECT DISTINCT rights FROM holdings_htitem ORDER BY rights";

      member_ids_str = @member_ids.map{|x| "'#{x}'"}.join(", ");
      sql = %W[
        SELECT
          COUNT(DISTINCT hh.volume_id) AS rights_count
        FROM holdings_memberitem        AS hm
        JOIN holdings_cluster_oclc      AS hco  ON (hm.oclc = hco.oclc)
        JOIN holdings_cluster_htitem_jn AS hchj ON (hco.cluster_id = hchj.cluster_id)
        JOIN holdings_htitem            AS hh   ON (hchj.volume_id = hh.volume_id)
        WHERE
          hm.member_id IN (#{member_ids_str})
          AND
          hh.rights = ?
      ].join(" ");

      q = @conn.prepare(sql);
      hdout = Hathidata::Data.new("reports/#{@member_id}_rights.tsv").open('w');
      @conn.query(get_all_rights_sql) do |rights_row|
        rights_attr = rights_row[:rights];
        @log.d(rights_attr);
        q.enumerate(rights_attr) do |row|
          hdout.file.puts([rights_attr, row[:rights_count]].join("\t"));
        end
      end
      hdout.close();
    end

  end
end

if __FILE__ == $0 then
  r = Overlap::Report.new();
  r.main();
end
