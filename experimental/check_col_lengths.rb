require 'hathidata';
require 'hathilog';

# Make sure that a HT003_xxx.yyy.tsv file doesn't contain bad stuff.

item_types = %w[mono multi serial];

# Using output from DESC holdings_memberitem:
spec = {
  'oclc'           => "int 20",
  'local_id'       => "varchar 50",
  'member_id'      => "varchar 20",
  'status'         => ['','CH','LM','WD'],
  'item_condition' => ['','BRT'],
  'process_date'   => "date",
  'enum_chron'     => "varchar 100",
  'item_type'      => ['mono','multi','serial'],
  'issn'           => "varchar 50",
  'n_enum'         => "varchar 60",
  'n_chron'        => "varchar 60",
  'gov_doc'        => "int 1"
}

@verbose = false;
logger   = Hathilog::Log.new({:file_name => 'check_ht003_cols_$ymd.log'});
ARGV.each do |member_id|
  item_types.each do |item_type|
    i = 0;
    hdin = Hathidata::Data.new("memberdata/#{member_id}/HT003_#{member_id}.#{item_type}.tsv");
    hdin.open('r').file.each_line do |line|
      i += 1;
      next if i == 1;
      puts "#{member_id} #{item_type} #{i}" if i % 25000 == 0;
      cols = line.split("\t");
      cols.each_with_index do |col,j|
        col.strip!;
        spec_key = spec.keys[j];
        spec_val = spec[spec_key];
        if spec_val.class == [].class then
          if spec_val.include?(col) then
            @verbose && puts("#{col} OK in #{spec_val.join(', ')}");
          else
            logger.w("#{member_id} #{item_type} #{i}: Bad value '#{col}' for #{spec_key} in #{line}");
          end
        elsif spec_val =~ /varchar (\d+)/ then
          len = $1.to_i;
          if col.length <= len then
            @verbose && puts("#{col} OK length (#{col.length} <= #{len})");
          else
            logger.w("#{member_id} #{item_type} #{i}: #{col} too long (#{col.length} > #{len}) in #{line}");
          end
        elsif spec_val =~ /int (\d+)/ then
          len = $1.to_i;
          if col =~ /^\d+$/ then
            if col.length <= len then
              @verbose && puts("#{col} OK length (#{col.length} <= #{len})");
            else
              logger.w("#{member_id} #{item_type} #{i}: #{col} too long (#{col.length} > #{len}) in #{line}");
            end
          elsif spec_key == 'gov_doc' && col == '\N' then
            @verbose && puts("NULL is OK here.");
          else
            logger.w("#{member_id} #{item_type} #{i}: Non-int for #{spec_key} #{col} in #{line}");
          end
        elsif spec_val == 'date' then
          if col =~ /^\d{4}-\d{2}-\d{2}$/ then
            @verbose && puts("ok date");
          else
            logger.w("Bad date '#{col}' in #{line}");
          end
        end
      end
    end
    hdin.close();
  end
end
