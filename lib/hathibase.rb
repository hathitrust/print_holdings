require 'hathidata';
require 'hathidb';
require 'hathilog';

module Hathibase
  class BaseReport
    def initialize
      puts "base class being initialized"
      @log  = Hathilog::Log.new();
      @db   = Hathidb::Db.new();
      @conn = @db.get_conn();
    end

    # Takes a sql string and a filename string.
    # Runs query and puts results in file.
    def output_query_to_file(sql, fn)
      hdout  = Hathidata::Data.new(fn).open('w');
      header = true;
      q = @conn.prepare(sql)
      q.enumerate do |row|
        if header then
          hdout.file.puts(row.to_h.keys.join("\t"));
          header = false;
        end
        hdout.file.puts(row.to_a.join("\t"))
      end
      hdout.close();
    end    
  end
end
