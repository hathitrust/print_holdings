require 'hathidb';
require 'hathilog';

=begin

Reloads the resolved_oclc field for holdings, commitments, or both.
Run under db_auth_nohup.sh, because it's going to take a while.
Pass "holdings" and/or "commitments" as args to reload their ocns.
Pass -n for noop.

E.g.

  $ bash ../../lib/db_auth_nohup.sh ruby reload_resolved_oclc.rb holdings
  $ bash ../../lib/db_auth_nohup.sh ruby reload_resolved_oclc.rb commitments
  $ bash ../../lib/db_auth_nohup.sh ruby reload_resolved_oclc.rb holdings commitments

=end

class Reloader
  def initialize(args)
    @args = args;
    @log  = Hathilog::Log.new();    
    db    = Hathidb::Db.new();
    @conn = db.get_interactive();
    @noop = false; # -n
    
    if ARGV.include?("-n") then
      @noop = true
      ARGV.delete("-n")
    end
    
    @update_holdings_sql = %W<
      UPDATE holdings_memberitem 
      SET resolved_oclc = COALESCE(
        (SELECT MAX(o.resolved) FROM oclc_concordance AS o WHERE o.variant = oclc),
        oclc
      )
    >.join(' ');

    @update_commitments_sql = %W<
      UPDATE shared_print_commitments 
      SET resolved_oclc = COALESCE(
        (SELECT MAX(o.resolved) FROM oclc_concordance AS o WHERE o.variant = local_oclc),
        local_oclc
      )
    >.join(' ');
  end
  
  def run  
    @log.i("Started");
    if @args.include?("holdings") then    
      @log.i("updating holdings...");
      do_update(@update_holdings_sql)
      @log.i("done updating holdings");
    end  

    if @args.include?("commitments") then
      @log.i("updating commitments...");
      do_update(@update_commitments_sql);
      @log.i("done updating commitments");
    end

    @log.i("Finished");
  end

  def do_update(sql)
    @log.i(sql);
    update_q = @conn.prepare(sql);
    if @noop then
      @log.d("Not actually updating because noop=#{@noop}")
    else
      update_q.execute() 
    end
  end
  
end

if $0 == __FILE__ then
  r = Reloader.new(ARGV);
  r.run();
end
