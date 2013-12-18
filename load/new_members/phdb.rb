module Phdb
  class Db
    @config;
    
    def initialize(c)
      @config = c; # PHCONFIG.new();
    end
    
    def foo()
      puts @config;
    end 

  end
end
