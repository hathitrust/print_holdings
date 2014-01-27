# Common queries used in several pieces of code.
# Also loose bits of hardcoded stuff that oughtn't be copy'n'pasted.
# This way, all the hardcoded evil is contained (I hope) in one place.

module Hathiquery
  def Hathiquery.source_map
    {
      'CHI'  => 'chi',
      'COO'  => 'cornell',
      'DUL'  => 'duke',
      'FU'   => 'ufl',
      'HVD'  => 'harvard',
      'IEN'  => 'nwu',
      'INU'  => 'ind',
      'LOC'  => 'loc',
      'MCHB' => 'bc',
      'MDL'  => 'minn',
      'MIU'  => 'uom',
      'NCSU' => 'ncsu',
      'NJP'  => 'prnc',
      'NNC'  => 'columbia',
      'NYP'  => 'nypl',
      'PST'  => 'psu',
      'PUR'  => 'purd',
      'UC'   => 'berkeley',
      'UCM'  => 'ucm',
      'UIU'  => 'uiuc',
      'UMN'  => 'minn',
      'UNC'  => 'unc',
      'USU'  => 'usu',
      'UVA'  => 'uva',
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

end
