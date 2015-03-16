# Common queries used in several pieces of code.
# Also loose bits of hardcoded stuff that oughtn't be copy'n'pasted.
# This way, all the hardcoded evil is contained (I hope) in one place.

module Hathiquery
  def Hathiquery.source_map
    {
      'CHI'  => 'uchicago',
      'COO'  => 'cornell',
      'DUL'  => 'duke',
      'FU'   => 'flbog',
      'HVD'  => 'harvard',
      'IEN'  => 'northwestern',
      'INU'  => 'iu',
      'LOC'  => 'loc',
      'MCHB' => 'bc',
      'MDL'  => 'umn',
      'MIU'  => 'umich',
      'NCSU' => 'ncsu',
      'NJP'  => 'princeton',
      'NNC'  => 'columbia',
      'NYP'  => 'nypl',
      'PST'  => 'psu',
      'PUR'  => 'purdue',
      'UC'   => 'berkeley',
      'UCM'  => 'ucm',
      'UIU'  => 'illinois',
      'UMN'  => 'umn',
      'UNC'  => 'unc',
      'USU'  => 'usu',
      'UVA'  => 'virginia',
      'WU'   => 'wisc',
      'YALE' => 'yale',
    };
  end

  def Hathiquery.cali_members 
    %w(berkeley ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf);
  end

  def Hathiquery.check_count(table)
    "SELECT COUNT(*) AS c FROM #{table}";
  end

  def Hathiquery.get_all_members
    "SELECT DISTINCT member_id FROM holdings_htmember ORDER BY member_id";
  end

  def Hathiquery.get_active_members
    "SELECT DISTINCT member_id FROM holdings_htmember WHERE status = 1 ORDER BY member_id";
  end

end
