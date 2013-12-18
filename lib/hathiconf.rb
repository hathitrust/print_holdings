=begin
Reads from configuration files and/or ENV as apropriate.
=end

require 'pathname';

module Hathiconf
  class Conf
    # Both conf files are read.
    # Data in local conf has precedence over global conf.
    # Conf files are expected to be key-value pairs separated by a tab.

    THIS_PATH        = Pathname.new(__FILE__).expand_path;
    GLOBAL_CONF_PATH = '/etc/conf/rubyconf.prop';
    LOCAL_CONF_PATH  = THIS_PATH + '../../conf/rubyconf.prop';
    
    g_read = File.readable?(GLOBAL_CONF_PATH);
    l_read = File.readable?(LOCAL_CONF_PATH);
    
    if !g_read && !l_read then
      message = [
                 "No readable conf files:",
                 "#{GLOBAL_CONF_PATH} readable? #{g_read}",
                 "#{LOCAL_CONF_PATH} readable? #{l_read}",
                ].join("\n\t* ");
      raise message;
    end
    @mem;

    def initialize()
      @mem = {};
      read_conf(GLOBAL_CONF_PATH);
      read_conf(LOCAL_CONF_PATH);
      
      @mem.each_key do |k|
        if k =~ /pass(word)?|pwd?/ then
          STDERR.puts "[#{k}]=[***password***]";
        else
          STDERR.puts "[#{k}]=[#{@mem[k]}]"
        end
      end
    end
    
    def get(key) 
      if @mem.has_key?(key) then
        return @mem[key];
      else
        STDERR.puts "Config could not find key #{key}";
        return '';
      end
    end

    def read_conf (path)
      cpath = Pathname.new(path).expand_path();
      if cpath.exist? then
        STDERR.puts("Reading conf #{cpath}");
        cfile = File.open(cpath, 'r');
        cfile.each_line do |line|
          line.strip!;
          # Expect lines like:
          # db_user=mwarin2000
          # ... with any amount of spaces anywhere.
          # Ignore lines starting with #
          next if line.length == 0;
          next if line =~ /^\s*#/
          key, val = *line.scan(/^\s*([a-z_0-9]+)\s*=\s*(.+)\s*$/).flatten
          @mem[key] = val;
        end
        return true;
      else
        STDERR.puts "Config could not find file #{cpath}";
      end
      return false;
    end
  end
end
