require 'hathidata';
require 'hathidb';
require 'hathilog';

module Hathibase
  class BaseReport
    def initialize
      @log  = Hathilog::Log.new();
      @db   = Hathidb::Db.new();
      @conn = @db.get_conn();
    end

    # Yields 1 row at a time from an executed sql.
    def rows(sql)
      q = @conn.prepare(sql);
      q.enumerate do |row|
        yield row;
      end
    end
    
    # Takes a sql string and a filename string.
    # Runs query and puts results in file.
    # Returns the HathiData object it wrote to.
    def output_query_to_file(sql, fn, header = true)
      hdout  = Hathidata::Data.new(fn).open('w');
      rows(sql) do |row|
        if header then
          hdout.file.puts(row.to_h.keys.join("\t"));
          header = false;
        end
        hdout.file.puts(row.to_a.join("\t"));
      end
      hdout.close();
      return hdout
    end    
  end
end
