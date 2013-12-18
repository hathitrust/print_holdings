require 'pathname';
gem 'test-unit';
require 'test/unit';

require 'hathidb';
require 'hathilog';
require 'hathidata';
require 'hathiconf';

class HathiUnit < Test::Unit::TestCase
  
  def self.scrub_scrub_scrub
    # Squeaky clean.
    puts "scrub scrub scrub";

    if File.exists?('/tmp/mwruby.log') then
      File.delete('/tmp/mwruby.log');
    end

    # Delete files in the log dir.
    logdir  = Hathilog::Log.get_log_dir_path();
    %W<default.log levels.log>.each do |logfn|
      logpath = logdir.to_s + '/' + logfn;
      if File.exists?(logpath) then
        File.delete(logpath);
      end
    end
   
    %W<appendable bunny toad hasbackup.gz unittest/fox unittest/touche>.each do |hdfn|
      hd = Hathidata::Data.new(hdfn);
      puts "Deleting #{hd.path}";
      hd.delete();
    end
    
    datadir  = Hathidata::Data.get_data_dir_path();
    unit_dir = datadir.to_s + '/unittest/';
    if File.directory?(unit_dir) then
      Dir.rmdir(unit_dir);
    end
  end

  def self.startup
    scrub_scrub_scrub();
  end

  def self.shutdown
    scrub_scrub_scrub();
  end

  # hathiconf
  def test_conf_keys()
    hc = Hathiconf::Conf.new();
    assert_equal('', hc.get('does_not_exist'));
    assert_not_equal('', hc.get('db_user'));
  end

  # hathidb
  def test_db_simple_select
    db = Hathidb::Db.new();
    c = db.get_conn();
    q = "SELECT 5 AS five";
    c.query(q) do |r|
      assert_equal(5, r[:five].to_i);
    end
    c.close();
  end

  # hathilog
  def test_log_simple
    lg = Hathilog::Log.new();
    lg.d("lala");
    lg.i("ok");
    lg.w("umm");
    lg.e("eeeeeeeh");
    lg.f("splat");
    lg.close();    

    assert_equal(1, 1);
  end

  def test_log_to_filepath
    logpath = '/tmp/mwruby.log';
    assert_equal(false, Pathname.new(logpath).exist?());

    lg = Hathilog::Log.new({:file_path => logpath});
    lg.d("lala");
    lg.i("ok");
    lg.w("umm");
    lg.e("eeeeeeeh");
    lg.f("splat");
    lg.close();

    assert_equal(true, Pathname.new(logpath).exist?());
  end

  def test_log_to_filename
    logdir  = Hathilog::Log.get_log_dir_path();
    logpath = logdir.to_s + '/default.log';
    assert_equal(false, Pathname.new(logpath).exist?(), "What, #{logpath} should not exist yet.");

    lg = Hathilog::Log.new({:file_name => 'default.log'});
    lg.d("lala");
    lg.i("ok");
    lg.w("umm");
    lg.e("eeeeeeeh");
    lg.f("splat");
    lg.close();

    assert_equal(true, Pathname.new(logpath).exist?());
  end

  def test_level_log

    lg = Hathilog::Log.new({:log_level => 3, :file_name => 'levels.log'});

    lg.d("no debug");
    lg.i("no info");
    lg.w("no warn");
    lg.e("yes error");
    lg.f("yes fatal");

    lg.set_level(1);
    lg.d("no debug");
    lg.i("yes info");

    lg.close();
    line_count = 0;

    f = File.open(lg.file_path, 'r');
      f.each_line do |line|
        line_count += 1;
      end
    f.close();

    assert_equal(3, line_count);
  end

  ## hathidata
  def test_data_read_write
    # Part 1
    hd = Hathidata::Data.new('bunny')
    assert_equal(false, hd.exists?());
    hd.open('w');
    hd.file.puts "FOO time #{Time.new()}";
    hd.file.flush();
    hd.close();
    assert_equal(true, hd.exists?());
    assert_equal(true, hd.file.closed?());

    # Part 2
    hd2 = Hathidata::Data.new('bunny');
    assert_equal(true, hd2.exists?(), "#{hd2.path} does not exist?");

    hd2.open.file.each_line do |line|
      assert_match(/FOO time/, line);
      break;
    end
    hd2.close();
  end

  def test_data_deflate
    # Part 1
    hd = Hathidata::Data.new('toad');
    assert_equal(false, hd.exists?());
    hd.open('w').file.puts "Ribbit #{Time.new()}";
    hd.close();
    hd.deflate();
    assert_equal(true, hd.exists?());

    # Part 2
    hd2 = Hathidata::Data.new('toad.gz');
    assert_equal(true, hd2.exists?());
    hd2.inflate.open.file.each_line do |line|
      puts "inflate_file: #{line}";
    end
    hd2.close();
    assert_equal(true, hd2.exists?());
  end

  def test_data_close_unopened
    assert_nothing_raised do
      hd = Hathidata::Data.new('froofroo');
      hd.close();
    end
  end

  def test_data_full_circle
    # We make sure path is created all the way.
    hd = Hathidata::Data.new('unittest/fox');
    assert_equal(false, hd.exists?());
    hd.open('w').file.puts "BARK BARK";
    hd.close();
    assert_equal(true, hd.exists?());
    hd.deflate();
    assert_equal(true, hd.exists?());

    # Make sure we can read from inflated file.
    hd2 = Hathidata::Data.new('unittest/fox.gz');
    assert_equal(true, hd2.exists?());
    hd2.inflate.open.file.each_line do |line|
      assert_match(/BARK/, line);
    end
    hd2.close();
  end

  def test_data_touch
    t = Hathidata::Data.new('unittest/touche')
    assert_equal(false, t.exists?());
    t.touch();
    assert_equal(true, t.exists?());
  end

  def test_data_append
    # Part 1
    app = Hathidata::Data.new('appendable')
    assert_equal(false, app.exists?());
    app.open('w').file.puts "1";
    app.close();

    # Part 2
    assert_equal(true, app.exists?());
    app.open('a').file.puts "2";
    app.close();

    # Part 3
    sum = 0;
    app.open('r').file.each_line do |line|
      sum += line.strip.to_i;
    end
    app.close();
    assert_equal(3, sum);
  end

  def test_data_exists
    hd = Hathidata::Data.new('doesnotexist');
    assert_equal(false, hd.exists?);
    hd.touch();
    assert_equal(true, hd.exists?);
    hd.delete;
    assert_equal(false, hd.exists?);
  end

  def test_data_backup
    hd = Hathidata::Data.new('hasnobackup');
    assert_equal(false, hd.exists?);
    assert_equal(false, hd.backup?);

    # Create a backup file for 'hasbackup'
    Hathidata::Data.new('hasbackup').touch().deflate();
    hd2 = Hathidata::Data.new('hasbackup');

    assert_equal(false, hd2.exists?);
    assert_equal(true, hd2.backup?);

    hd2.get_backup_or_open('w');
    assert_equal(true, hd2.exists?);
    hd2.file.puts Time.new().to_s;
    hd2.close().deflate();
  end

end
