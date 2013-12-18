#require 'rubygems'; # Not needed?
require 'java';
require 'jdbc-helper';
require 'mysql-connector-java-5.1.17-bin.jar';
require 'hathiconf';

module Hathidb
  class Db

    def initialize ()
      @conf = Hathiconf::Conf.new();
    end

    def get_conn()
      conn = JDBCHelper::Connection.new(
       :driver           => @conf.get('db_driver'),
       :url              => @conf.get('db_url'),
       :user             => @conf.get('db_user'),
       :password         => @conf.get('db_pw'),
       :useCursorFetch   => 'true', 
       :defaultFetchSize => 10000,
       );
      return conn;
    end

    def get_interactive()
      # Like get_conn but getting username & password from stdin.
      require 'io/console';
      print "\n";
      print "User: >>";
      db_user = STDIN.noecho(&:gets).strip;
      print "\n";
      print "Password: >>";
      db_pw   = STDIN.noecho(&:gets).strip;
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

  end
end
