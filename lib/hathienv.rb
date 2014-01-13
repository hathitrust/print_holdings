module Hathienv
  class Env

    def Env.is_dev? ()
      return !Env.is_prod?()
    end

    def Env.is_prod? ()
      return %x(hostname).include?('grog');
    end

  end
end

if $0 == __FILE__ then
  puts "Testing Hathienv:";
  puts "Dev? "  + Hathienv::Env.is_dev?().to_s;
  puts "Prod? " + Hathienv::Env.is_prod?().to_s;
end
