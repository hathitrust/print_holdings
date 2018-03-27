require 'java'
java_import 'java.lang.Runtime'

module Hathienv
  class Env

    def Env.is_dev? ()
      return !Env.is_prod?()
    end

    def Env.is_prod? ()
      return %x(hostname).include?('grog');
    end

    def Env.require_minimum_ram (needed_mb)    
      mxm = Runtime.getRuntime.maxMemory.to_i;
      # byte to megabyte conversion
      mxm_mb = mxm / 1048576;
      $stderr.puts "#{$0}: Required: #{needed_mb} mb, available #{mxm_mb} mb";
      if needed_mb > mxm_mb then
        return false;
      end
      return true;
    end
    
  end
end

if $0 == __FILE__ then
  puts "Testing Hathienv:";
  puts "Dev? "  + Hathienv::Env.is_dev?().to_s;
  puts "Prod? " + Hathienv::Env.is_prod?().to_s;
  puts Hathienv::Env.require_minimum_ram(100);
  puts Hathienv::Env.require_minimum_ram(1000);
end
