require 'date';
require 'enum_chron_parser';
require 'hathidata';
require 'hathidb';
require 'hathilog';
require 'iconv';
require 'json';
require 'open-uri';

NON_PRINT = /^((cd|dvd)(-?rom)?)$/i.freeze
FACSIMILE = /(fasc\.?\s*\d*)/i
EXPONENTL = /[Ee]\+\d+/

# --- Instructions: ---
# Set up a directory /data/memberdata/<member_id>/
# (or /data/memberdata/<member_id>.estimate/).
# In it, put one file each for monos, multis, serials.
# Generate a scrub_conf.json in that directory by running:
# $ ruby lib/scrub.rb <member_id> --generate_conf
# Edit the .json file. There is some documentation about what 
# the fields are for where the variable conf_template is declared.
# Once the .json is done, scrub files by running:
# $ ruby lib/scrub.rb <member_id>
# You will end up with one HT003_<member_id>.<item_type>.tsv 
# per item_type (mono, multi, serial).

def main
  member_id = ARGV.shift;
  estimate  = false;
  base_dir  = "memberdata";
  log       = Hathilog::Log.new();

  # Keys are strings, not symbols, because they will be read from JSON.
  # Not messing around with indifferent-access hashes.
  # Values for col_-prefixed fields should be 0-indexed positive ints.
  # col_condition: which column in the file has condition:<CH|LM|WD>? Only for mono & multi.
  # col_enum_chron: which column has the enumchron:<NULL|free text>. Only for multis.
  # col_gov_doc: which column in the file has govdoc:<NULL|1|0>?
  # col_issn: which column has the issn. Only for serials.
  # col_local_id: which column in the file has members' local id?
  # col_oclc: which column has the OCLC number?
  # col_status: which column has status:<NULL|BRT>. Only for mono & multi.
  # header_lines: number of lines at top of file that shouldn't be parsed
  # infile: name of file relative to the data/memberdata/<member_id>/ dir.
  # min_cols: skip line if it has fewer cols than this.
  # You can use comments of sorts in the json, by prefixing their keys with "_".
  conf_template = {
    "mono" => {
      "col_condition" => "optional",
      "col_gov_doc"   => "optional",
      "col_local_id"  => "optional",
      "col_oclc"      => "required",
      "col_status"    => "optional",
      "infile"        => "required",
      "header_lines"  => "required",
      "min_cols"      => "required",
    },
    "multi" => {
      "col_condition"  => "optional",
      "col_enum_chron" => "optional",
      "col_gov_doc"    => "optional",
      "col_local_id"   => "optional",
      "col_oclc"       => "required",
      "col_status"     => "optional",
      "infile"         => "required",
      "header_lines"   => "required",
      "min_cols"       => "required",
    },
    "serial" => {
      "col_gov_doc"  => "optional",
      "col_issn"     => "optional",
      "col_local_id" => "optional",
      "col_oclc"     => "required",
      "infile"       => "required",
      "header_lines" => "required",
      "min_cols"     => "required",
    }
  };

  if member_id.nil? then
    raise "This script requires a member_id as 1st arg.";
  end

  # Get path to member dir. Guess estimate if not found.
  dir = nil;
  dir_paths = ["#{base_dir}/#{member_id}", "#{base_dir}/#{member_id}.estimate"];
  dir_paths.each do |p|
    if dir.nil? then
      log.d("Is dir #{p}?");
      d = Hathidata::Data.new(p);
      if d.exists? then
        dir = d;
        log.d("Yes, found dir: #{p}");
      else
        log.d("No, could not find dir: #{p}");
      end
    end
  end
  if dir.nil? then
    raise "Could not find dir, neither in #{dir_paths.join(" or ")}";
  end

  # Find the conf in the dir.
  conf = Hathidata::Data.new("#{dir.path}/scrub_conf.json");
  # Check if we should generate a dummy conf. If so, do it and exit.
  if ARGV.include?("--generate_conf") then
    log.d("Generating conf.");
    conf.open("w");
    conf.file.puts(JSON.pretty_generate(conf_template));
    conf.close();
    # BYE.
    exit();
  end
  if conf.exists? then
    log.d("Reading conf #{conf.path}");
  else
    raise ["Could not find conf, expected at #{conf.path}.",
           "If you don't have one you can generate with:",
           "$ ruby #{$0} #{member_id} --generate_conf"
          ].join("\n");
  end

  # Parse conf file (json) into ruby hash.
  conf_hash = JSON.parse(File.read(conf.path));
  puts conf_hash;

  # Make sure conf_hash only contains the keys in conf_template.
  conf_hash.keys.sort.each do |item_type|
    next if item_type.start_with?('_');
    if !conf_template.has_key?(item_type) then
      raise "Conf contains disallowed item_type #{item_type}";
    end
    conf_hash[item_type].keys.sort.each do |attr|
      next if attr.start_with?('_');
      if !conf_template[item_type].has_key?(attr) then
        raise "Conf contains disallowed attr #{attr} for item_type #{item_type}";
      end
    end
  end

  # Make sure the required values in conf_template are present in conf_hash.
  conf_template.keys.sort.each do |item_type|
    if conf_hash.has_key?(item_type) then
      conf_template[item_type].keys.each do |attr|
        if conf_template[item_type][attr] == "required" then
          if !conf_hash[item_type].has_key?(attr) then
            raise "#{conf.path} does not contain required item #{item_type}=>#{attr}";
          elsif ["required", "optional"].include?(conf_hash[item_type][attr]) then
            # Make sure they are not dummy values.
            raise ["#{conf.path} contains a dummy value in #{item_type}=>#{attr}.",
                   "You need to replace that with something useful"].join("\n");
          end
        end
      end
    end
  end

  # Make sure the values are of the right class.
  # infile must be a string (with ""s in the JSON),
  # all other values ints (unquoted in the JSON).
  conf_hash.keys.sort.each do |item_type|
    next if item_type.start_with?('_');
    conf_hash[item_type].keys.each do |attr|
      value = conf_hash[item_type][attr];
      require_class(value, attr == "infile" ? String : Fixnum, "#{item_type}.#{attr}");
    end
  end
  
  scrub_log_path = Hathidata::Data.new("#{dir.path.to_s}/scrub_#{member_id}_$ymd.log.txt").path.to_s;
  scrub_log      = Hathilog::Log.new({:file_path => scrub_log_path});

  # Generate an options hash for each item_type, send to MemberScrubber.
  conf_hash.keys.sort.each do |item_type|
    next if item_type.start_with?('_');
    log.d(item_type);
    options = conf_hash[item_type];
    options["member_id"] = member_id;
    options["infile"]    = Hathidata::Data.new("#{dir.path.to_s}/#{options["infile"]}");
    options["outfile"]   = Hathidata::Data.new("#{dir.path.to_s}/HT003_#{member_id}.#{item_type}.tsv");
    options["logger"]    = scrub_log;
    options["data_type"] = item_type;
    if !options["infile"].exists? then
      raise "Cannot find #{item_type} infile #{options["infile"].path}";
    end
    MemberScrubber.new(options).process_data();
  end
  scrub_log.close();
end

def require_class (value, cl, json_path)
  if value.class != cl then
    raise "Value for #{json_path} must be a #{cl} instance. Check the quotes in the conf JSON file.";
  end
end

# Stores the information on which data are in which member-submitted columns
class ColumnMapping
  attr_accessor :oclc, :local_id, :status, :condition, :enum_chron, :issn, :item_id, :gov_doc;

  def initialize( options = {} )
    @oclc       = options["col_oclc"]       || 0;
    @local_id   = options["col_local_id"]   || 1;
    @status     = options["col_status"]     || nil;
    @condition  = options["col_condition"]  || nil;
    @enum_chron = options["col_enum_chron"] || nil;
    @issn       = options["col_issn"]       || nil;
    @item_id    = options["col_item_id"]    || nil;
    @gov_doc    = options["col_gov_doc"]    || nil;
  end
end

# Main parser module.
class MemberScrubber
  attr_accessor :member_id, :mapper, :data_type, :delim, :min_cols, :header_lines, :long_lines;
  @@VERBOSE       = false;
  @@REPORT_MULTIS = true;

  def initialize( options = {} )
    @member_id    = options["member_id"];
    @mapper       = ColumnMapping.new(options);
    @data_type    = options["data_type"]    || raise("Need at least data_type");
    @delim        = options["delim"]        || "\t";
    @min_cols     = options["min_cols"]     || 2;
    @header_lines = options["header_lines"] || 0;
    @long_lines   = options["long_lines"]   || 9; # What is this for?

    @conn          = Hathidb::Db.new().get_conn();
    count_oclc_sql = "SELECT COUNT(*) FROM holdings_htitem_oclc WHERE oclc = ?";
    @count_oclc_q  = @conn.prepare(count_oclc_sql);

    @hdin         = options["infile"];
    @hdout        = options["outfile"];
    @logger       = options["logger"];
    @max_ocn      = get_max_ocn();
    @counter      = {};
    countables    = %w[total_lines short_lines long_lines bad_oclc good_status bad_status
                       output_lines condition_items gov_docs multi_ocn_items blank_lines];
    countables.each do |c|
      @counter[c.to_sym] = 0;
    end

    @current_date = Date.today.to_s;
  end

  def count(label, inc=1)
    @counter[label] ||= 0;
    @counter[label] +=  1;
  end

  def report_count
    out = ["Counts:"];
    @counter.keys.sort.each do |label|
      out << "#{label}\t#{@counter[label]}";
    end
    return out.join("\n");
  end

  def get_max_ocn
    # Look up current max oclc number. Manually delete file to get the freshest.
    hd  = Hathidata::Data.new("max.ocn");
    ocn = 0;
    if hd.exists? then
      hd.open('r').file.each_line do |line|
        line.strip!;
        if line =~ /^(\d+)$/ then
          ocn = line.to_i;
          break;
        end
      end
      hd.close();
    else
      max_oclc_url = "http://www.oclc.org/apps/oclc/wwg";
      body = open(max_oclc_url).read;
      if body.empty? then
        raise "Fail! Body empty. Check if #{max_oclc_url} is b0rk.";
      else
        j = JSON.parse(body);
        if (j.class.to_s == 'Hash' && j['oclcNumber']) then
          @logger.i("Current max OCLC: #{j['oclcNumber'].strip}");
          ocn = j['oclcNumber'].strip.to_i;
          hd.open('w').file.puts ocn;
          hd.close();
        else
          raise 'fail, no oclc in body';
        end
      end
    end

    return ocn;
  end

  def clean_line(untrusted_string)
    ic = Iconv.new('UTF-8//IGNORE', 'UTF-8');
    valid_string = ic.iconv(untrusted_string + ' ')[0..-2];
  end

  def choose_ocn_using_db(ocnline, delim)
    # pick the first matching OCN, or default to the first entry if none found
    bits = ocnline.split(delim).select{|x| x =~ /\d/}.uniq;
    bits.each do |bit|
      bi = bit.to_i;
      next if bi == 0;
      row    = @count_oclc_q.enumerate(bi);
      result = row.to_a.uniq[0];
      count  = result[0]; # grab the first term too
      if (count.to_i > 0)
        return [bit, 'match'];
      end
    end
    return [bits[0], 'no match'];
  end

  def parse_multiple(ocnline, delim=';')
    # deprecated
    bits = ocnline.split(delim);
    max  = 0;
    bits.each do |bit|
      if bit.to_i > max
        max = bit.to_i;
      end
    end
    return max;
  end

  # A bit different than the 'parse_oclc' routine in phdb_utils.
  # Handles several user-specific things.
  def parse_oclc(ocn)
    return 0 if ocn.nil?;
    return 0 if ocn.empty?;
    ocnp = ocn.strip.gsub(/\(OCoLC\)/, '');
    ocnp.gsub!(/ocl7/i, '');
    ocnp.gsub!(/oc[nm]/i, '');
    ocnp.gsub!(/oclc/i, '');
    ocnp.gsub!(/\\a/, '');
    multi_toggle = false;
    hit_toggle = 'no match';
    if ocnp =~ /;/
      count(:multi_ocn_items);
      multi_toggle = true;
      ocnp, hit_toggle = choose_ocn_using_db(ocnp, ";");
    elsif ocnp =~ /\,/
      ocnp.gsub!(/\s/, ',');
      count(:multi_ocn_items);
      multi_toggle = true;
      ocnp, hit_toggle = choose_ocn_using_db(ocnp, ",");
    elsif ocnp =~ /\s/
      count(:multi_ocn_items);
      multi_toggle = true;
      ocnp, hit_toggle = choose_ocn_using_db(ocnp, "\s");
    elsif ocnp =~ /\\z/
      count(:multi_ocn_items);
      multi_toggle = true;
      ocnp, hit_toggle = choose_ocn_using_db(ocnp, '\z');
    end
    if multi_toggle and @@REPORT_MULTIS
      @logger.i("Multi OCLC: '#{ocn}'->'#{ocnp}' (#{hit_toggle})");
    end
    if (ocnp =~ /\D/)
      if @@VERBOSE
        STDERR.puts("Adjusting '#{ocn}'->'#{ocnp}'->'#{ocni}'");
      end
      ocnp = ocnp.gsub(/\D/, '');
    end
    ocni = ocnp.to_i;
  end

  def test_oclc(ocn)
    if (ocn.is_a?(Integer))
      return ((ocn <= @max_ocn) && (ocn > 0)) ? true : false;
    else
      return false;
    end
  end

  def test_status(status)
    status_list = %w[CH LM WD ch lm wd] << nil << '';
    return true if status == nil;
    if status_list.include?(status.strip)
      return true;
    else
      return false;
    end
  end

  def test_condition(condition)
    condition_list = ['BRT', 'DAMAGED', nil, ''];
    if condition_list.include?(condition)
      return true;
    else
      return false;
    end
  end

  def test_type(itype)
    itype_list = %w[mono serial multi];
    if itype_list.include?(itype)
      return true;
    else
      return false;
    end
  end

  def parse_issn(issn)
    if issn and issn.length > 40
      bits = issn.split(';');
      issn = bits[0];
      if issn.length > 40
        issn = '';
      end
    end

    return issn;
  end

  def replace_type_in_outline(outl, type)
    bits = outl.strip.split("\t");
    bits.map{ |item| item.gsub!(/\"/, '') };
    bits[7] = type;
    return bits.join("\t");
  end

  def process_data ()
    @logger.i("Started scrubbing #{@data_type}s from #{@member_id}");
    @logger.i(@hdin.path);
    @hdin.open('r');
    @hdout.open('w');
    @hdout.file.puts %w[OCN BIB MEMBER_ID STATUS CONDITION DATE ENUM_CHRON TYPE ISSN N_ENUM N_CHRON GOV_DOC].join("\t");
    ecparser = EnumChronParser.new;

    i = 0;
    @hdin.file.each_line do |line|
      line = line.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
      line.rstrip!;
      i += 1;
      # puts i;
      if (i <= @header_lines) then
        @logger.i("skipping header #{i} of #{@header_lines}: #{line}");
        count(:skipped_header_lines);
      else
        last_bib  = nil;
        last_line = nil;
        in_multi  = 0;
        # loop over data lines
        count(:total_lines);
        unless line.valid_encoding? # handle invalid encoding
          @logger.w("\tEncoding fail... skipping.");
          count(:bad_encoding);
          next;
        end
        bits = line.split(@delim);
        bits.map{ |item| item.gsub!(/\"/, '') };
        bits.map{ |item| item.sub!(/\\+$/, '') }; # Dealing with especially enum_chrons ending with a backslash.
        if bits.length < 1
          count(:blank_lines);
          next;
        end
        unless bits.length >= @min_cols
          @logger.i("Short Line on line #{i}: '#{line.strip}'");
          count(:short_lines);
          next;
        end
        if bits.length >= @long_lines
          @logger.i("Long Line: '#{line.strip}'");
          count(:long_lines);
        end
        ## handle required fields (oclc, local_id)
        rawocn = bits[@mapper.oclc];

        if rawocn =~ EXPONENTL then
          @logger.i("Exponential OCN on line #{i}: #{rawocn}")
          count(:exponential_ocn)
        end

        ocn = parse_oclc(rawocn);
        unless test_oclc(ocn)
          @logger.i("Bad OCLC (#{ocn}) on line #{i}: '#{line.strip}'");
          count(:bad_oclc);
          next;
        end
        bib = bits[@mapper.local_id];
        if !bib.nil?
          bib.strip!
        end
        ## handle optional fields
        status     = '';
        condition  = '';
        enum_chron = '';
        n_enum     = '';
        n_chron    = '';
        issn       = '';
        gov_doc    = '\N';
        type       = @data_type;
        if (@mapper.status)
          if (bits[@mapper.status] == nil)
            status = '';
          else
            status = bits[@mapper.status].strip;
          end
          if status =~ /LLMLM/
            status.gsub!(/LLMLM/i, 'LM'); # mistake in arizona
          elsif status =~ /LLM/
            status.gsub!(/LLM/i, 'LM');
          end
          if test_status(status)
            count(:good_status);
            count("status_#{status}".to_sym);
          else
            @logger.i("Bad Status on line #{i}: (#{status}) '#{line.strip}'");
            count(:bad_status);
            status = '';
          end
        end
        if (@mapper.condition)
          bits[@mapper.condition] ||= '';
          condition = bits[@mapper.condition].strip;
          if test_condition(condition)
            count(:condition_items);
            count("condition_#{condition}".to_sym);
          else
            @logger.i("Bad condition on line #{i}: (#{condition}) '#{line.strip}'");
            count(:bad_condition);
            condition = '';
          end
        end
        if (@mapper.enum_chron)
          enum_chron = bits[@mapper.enum_chron];
          unless (enum_chron.nil?)
            enum_chron = enum_chron.gsub("\t", ' ');
            enum_chron = enum_chron.gsub(/None/i, '');
            enum_chron = enum_chron.gsub(/NULL/i, '');
            enum_chron = enum_chron.gsub(/\?/, '');
            # "c.1" intervention
            enum_chron = enum_chron.gsub(/c\.1$/, '');
            enum_chron = enum_chron.gsub(/c\. 1$/, '');
            
            # Filter out some common non-print enumchrons.
            if enum_chron =~ NON_PRINT then
              @logger.i("Skipping record with non-print enumchron on line #{i}: (#{enum_chron}) '#{line.strip}'");
              count(:non_print_enumchron);
              next;
            end
            
            if enum_chron =~ FACSIMILE then
              @logger.i("Potential facsimile? Line #{i} (#{enum_chron})")
              count(:facsimile_enumchron);
            end
            
            ecparser.parse(enum_chron);

            if ecparser.enum_str.length > 0 then
              n_enum = ecparser.normalized_enum.strip;
            end
            if ecparser.chron_str.length > 0 then
              n_chron = ecparser.normalized_chron.strip;
            end
            count(:enumc_lines);
          end
          if n_enum.length > 60 then
            @logger.w("Long enum on line #{i}: #{n_enum}");
            count(:long_enum);
          end
          if n_chron.length > 60 then
            @logger.w("Long chron on line #{i}: #{n_chron}");
            count(:long_chron);
          end
        end
        if (@mapper.issn)
          issn_raw = bits[@mapper.issn];
          issn     = parse_issn(issn_raw);
          if issn =~ /\d+/ then
            count(:issn_lines);
          end
        end

        if (@mapper.gov_doc) then
          gov_doc_raw = bits[@mapper.gov_doc];
          gov_doc_raw.nil? || gov_doc_raw.strip!;
          if gov_doc_raw == '1' || gov_doc_raw == '0' then
            gov_doc = gov_doc_raw;
            if gov_doc == '1' then
              count(:gov_docs);
            end
          elsif !gov_doc_raw.nil? then
            count("bad_gov_doc_#{gov_doc_raw}".to_sym);            
          end
        end

        # tricky bit to handle 'multi' status in monos - only used when item_ids are present
        if (@mapper.item_id)
          item_id = bits[@mapper.item_id];
          if (last_bib && (bib == last_bib))
            new_last_line = replace_type_in_outline(last_line, 'multi');
            @hdout.file.puts(new_last_line);
            count(:output_lines);
            in_multi = 1;
          else
            if (last_line && (in_multi==1))
              new_last_line = replace_type_in_outline(last_line, 'multi');
              @hdout.file.puts(new_last_line);
              count(:output_lines);
              in_multi = 0;
            else
              if last_line
                @hdout.file.puts(last_line);
                count(:output_lines);
              end
            end
          end
          outstr = [ocn, item_id, @member_id, status, condition, @current_date, enum_chron, type.strip, issn, n_enum, n_chron, gov_doc].join("\t");
          last_bib  = bib;
          last_line = outstr;
        else
          count(:output_lines);
          outstr = [ocn, bib, @member_id, status, condition, @current_date, enum_chron, type.strip, issn, n_enum, n_chron, gov_doc].join("\t");
          last_line = outstr;
          @hdout.file.puts(outstr);
        end
      end
    end
    @hdout.close();
    @hdin.close();
    @logger.i("Finished scrubbing #{@data_type}s from #{@member_id}");
    @logger.i(report_count);
  end
end

if $0 == __FILE__ then
  main();
end
