require 'java';
require 'jdbc-helper';
require 'mysql-connector-java-5.1.17-bin.jar';
require 'io/console';
require 'hathiconf';
require 'hathienv';

module Hathidb
  class Db

    def initialize ()
      @conf = Hathiconf::Conf.new();
    end

    # Implicitly 'dev'.
    def get_conn()
      conn = JDBCHelper::Connection.new(
       :driver           => @conf.get('db_driver'),
       :url              => @conf.get('db_url'),
       :user             => ENV['CMDLINE_ENV_DB_USER']     || @conf.get('db_user'),
       :password         => ENV['CMDLINE_ENV_DB_PASSWORD'] || @conf.get('db_pw'),
       :useCursorFetch   => 'true', 
       :defaultFetchSize => 10000,
       );
      return conn;
    end

    # Explicitly 'prod'.
    def get_prod_conn()

      if Hathienv::Env.is_dev?() then
        raise "You cannot access the production database from here.";
      end

      conn = JDBCHelper::Connection.new(
       :driver           => @conf.get('prod_db_driver'),
       :url              => @conf.get('prod_db_url'),
       :user             => @conf.get('prod_db_user'),
       :password         => @conf.get('prod_db_pw'),
       :useCursorFetch   => 'true', 
       :defaultFetchSize => 10000,
       );
      return conn;
    end

    def get_interactive()
      # Like get_conn but getting username & password from stdin.      
      print "\n";
      print "DB User: >>";
      db_user = ENV['CMDLINE_ENV_DB_USER'] || STDIN.noecho(&:gets).strip;
      print "\n";
      print "DB Password: >>";
      db_pw   = ENV['CMDLINE_ENV_DB_PASSWORD'] || STDIN.noecho(&:gets).strip;
      print "\n";
      
      conn = JDBCHelper::Connection.new(
       :driver           => @conf.get('db_driver'),
       :url              => @conf.get('db_url'),
       :user             => db_user,
       :password         => db_pw,
       :useCursorFetch   => 'true', 
       :defaultFetchSize => 10000,
       );
      return conn;
    end

    def get_prod_interactive()
      # Like get_prod_conn but getting username & password from stdin.

      if Hathienv::Env.is_dev?() then
        raise "You cannot access the production database from here.";
      end

      print "\n";
      print "Prod DB User: >>";
      db_user = ENV['CMDLINE_ENV_DB_USER'] || STDIN.noecho(&:gets).strip;
      print "\n";
      print "Prod DB Password: >>";
      db_pw   = ENV['CMDLINE_ENV_DB_PASSWORD'] || STDIN.noecho(&:gets).strip;
      print "\n";
      conn = JDBCHelper::Connection.new(
       :driver           => @conf.get('prod_db_driver'),
       :url              => @conf.get('prod_db_url'),
       :user             => db_user,
       :password         => db_pw,
       :useCursorFetch   => 'true', 
       :defaultFetchSize => 10000,
       );
      return conn;
    end

  end
end
