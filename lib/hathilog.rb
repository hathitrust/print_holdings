=begin

Uniform logging for all Hathi scripts and modules.

require 'hathilog';

lg = Hathilog::Log.new(); # STDERR logger
lg.d("d does debug");
...
lg.f("f for fatal");

lg.set_level(2); # Will now ignore debug. 

# File logger to LOG_DIR_PATH/foo.log.
lg = Hathilog::Log.new({:file_name => 'foo.log'});
# Add '$ymd' to :file_name to get current date as yyyymmdd.

# File logger to /etc/foo.log
lg = Hathilog::Log.new({:file_path => '/etc/foo.log'});

...

lg.close();

=end

require 'logger';
require 'pathname';

module Hathilog
  class Log

    THIS_PATH    = Pathname.new(__FILE__).expand_path;
    LOG_DIR_PATH = THIS_PATH + '../../log/';

    def self.get_log_dir_path
      return LOG_DIR_PATH;
    end

    @logger;
    attr_reader :file_path;
    attr_reader :log_level;

    def initialize (parameters = {})

      # Specify a full path, and we'll write to it.
      @file_path = parameters[:file_path] || nil;

      # Or, specify just a file name and we'll write to that file
      # in the LOG_DIR_PATH.
      @file_name = parameters[:file_name] || nil;

      # Set at creation and/or change later. See set_level().
      @log_level = parameters[:log_level] || nil;
      
      # Make a file_path if file_name given.
      if @file_path == nil && @file_name != nil then
        @file_path = LOG_DIR_PATH.to_s + @file_name;
      end

      # Determine autoflush.
      @log_sync = parameters[:log_sync] || false;
      
      if @file_path != nil then
        # $ymd -> date expansion in path name.
        if @file_path[/\$ymd/] then
          ymd = Time.new().strftime("%Y%m%d");
          @file_path.gsub!(/\$ymd/, ymd);
        end

        STDERR.puts "Logging to #{@file_path}";
        f = File.open(@file_path, File::WRONLY | File::APPEND | File::CREAT);

        if @log_sync == true then
          f.sync = true;
        end

        @logger = Logger.new(f, 2, 1048576); # Max 2 x 1MB
      else
        @logger = Logger.new(STDERR);
      end

      set_level(@log_level);

      @logger.formatter = proc do |severity, datetime, progname, msg|
        fileLine = caller(2)[4].split(':')[0,2].join(':');
        "#{datetime} | #{fileLine} | #{severity} | #{msg}\n";
      end
    end

    # Called by initialize, but can also be called later by public.
    # 0 = debug, 5 = fatal. Needs a little more logic than an
    # attr_accessor.
    def set_level(level)
      @level = level.to_i;
      if level != nil && level >= 0 && level <= 5 then
        @logger.level = level;
      else
        @logger.level = Logger::DEBUG;
      end
    end

    def close
      # We don't want to close STDERR.
      if @file_path != nil then
        @logger.close();
      end
    end
    
    # Shorthand for the 5 main log levels.
    def d(msg)
      @logger.debug(msg);
    end
    def i(msg)
      @logger.info(msg);
    end
    def w(msg)
      @logger.warn(msg);
    end
    def e(msg)
      @logger.error(msg);
    end
    def f(msg)
      @logger.fatal(msg);
    end
  end
end
