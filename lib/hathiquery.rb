# Common queries used in several pieces of code.
# Also loose bits of hardcoded stuff that oughtn't be copy'n'pasted.
# This way, all the hardcoded evil is contained (I hope) in one place.

module Hathiquery
  def Hathiquery.source_map
    # Check regularly if there are values missing from here.
    # SELECT DISTINCT source FROM holdings_htitem;
    # Then look the missing ones up in prod:
    # mwarin@mysql-sdr [ht_repository]> select collection,responsible_entity from ht_collections where collection IN ('AEU','AUBRU' ... );
    # ... and add those member_ids here.
    {
      'AEU'    => 'ualberta',
      'AUBRU'  => 'uq',
      'CHI'    => 'uchicago',
      'CMALG'  => 'getty',
      'COO'    => 'cornell',
      'CTU'    => 'uconn',
      'DEU'    => 'udel',
      'DUL'    => 'duke',
      'FMU'    => 'miami',
      'FU'     => 'flbog',
      'GEU'    => 'emory',
      'HVD'    => 'harvard',
      'IAU'    => 'uiowa',
      'IEN'    => 'northwestern',
      'INU'    => 'iu',
      'JTKU'   => 'hathitrust', # Keio
      'LOC'    => 'loc',
      'MCHB'   => 'bc',
      'MDL'    => 'umn',
      'MDU'    => 'umd',
      'MIEM'   => 'msu',
      'MIU'    => 'umich',
      'MMET'   => 'tufts',
      'MOU'    => 'missouri',
      'MU'     => 'umass',
      'MWICA'  => 'umich',
      'NCSU'   => 'ncsu',
      'NCWSW'  => 'wfu',
      'NJP'    => 'princeton',
      'NNC'    => 'columbia',
      'NYP'    => 'nypl',
      'OU'     => 'osu', # was OSU=>osu
      'PST'    => 'psu',
      'PU'     => 'upenn',
      'PUR'    => 'purdue',
      'QMM'    => 'mcgill',
      'TXCM'   => 'tamu',
      'TXU'    => 'utexas',
      'UC'     => 'berkeley',
      'UCM'    => 'ucm',
      'UIU'    => 'illinois',
      'UKLOKU' => 'hathitrust',
      'UMN'    => 'umn',
      'UNC'    => 'unc',
      'USU'    => 'usu',
      'UVA'    => 'virginia',
      'WAU'    => 'washington',
      'WU'     => 'wisc',
      'YALE'   => 'yale'
    };
  end

  def Hathiquery.cali_members
    %w{berkeley ucdavis uci ucla ucmerced ucr ucsb ucsc ucsd ucsf};
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

  def Hathiquery.get_us_members
    "SELECT member_id FROM holdings_htmember WHERE country_code = 'us' ORDER BY member_id";
  end

  def Hathiquery.get_nonus_members
    "SELECT member_id FROM holdings_htmember WHERE country_code  != 'us' ORDER BY member_id";
  end

  def Hathiquery.get_shared_print_members
    "SELECT DISTINCT member_id FROM shared_print_commitments ORDER BY member_id"
  end
    
end
