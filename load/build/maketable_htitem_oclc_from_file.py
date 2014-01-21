#!/usr/bin/python
"""
This routine generates the htitem_oclc_jn table for the 
PHDB database.  Takes as input a 'full' tab-delimited flatfile and 
generates flatfiles suitable for direct load into the database.
"""

import sys, re
from sys import argv


def generate_htitem_oclc_data(ht_filen, jn_outfilen):
    """ Parses the HT metadata flatfile and generates a flatfile suitable for
    auto-load into the htitem_oclc_jn table.  """
    
    print "Parsing data from %s." % ht_filen
    sys.stdout.flush()
    
    # open files
    infile      = file(ht_filen)
    jn_outfile  = file(jn_outfilen, 'w')
    err_outfile = file(ht_filen+'.err', 'w')
    
    global_counter   = 0
    no_oclc          = 0
    oclc_counter     = 0
    bad_line_counter = 0
    
    # containers to test uniqueness and store table data
    vol_oclc_set = set([])  # for 'htitem_oclc_jn' table 
    oclc_set     = set([])       
    
    # loop through all lines and write join table lines  
    for line in infile.readlines():
        # progress indicator
        global_counter += 1
        if ((global_counter % 1000000) == 0):
            print "%i rows parsed..." % (global_counter)
            sys.stdout.flush()
        # grab vol_id and oclc fields 
        tline = line.rstrip()
        bits = tline.split("\t")
        if (len(bits) < 15):
            err =  "bad line: '%s'" % line
            err_outfile.write(err+"\n")
            bad_line_counter += 1
            continue
        vol_id = bits[0].strip()
        oclc = bits[7].strip()
        if (oclc == ''):
            no_oclc += 1
            err = "Missing OCLC: '%s'\n" % line
            err_outfile.write(err)
            continue
        
        # parse out individual oclc nums
        oclcs = []
        # check if multiple oclcs
        if (re.search("\,", oclc)):
            obits = oclc.split(",")
            [o.strip() for o in obits]
            oclc_nums = obits
            for onum in oclc_nums:
                onum_i = int(onum)
                # test uniqueness
                test_key = "%s-%i" % (vol_id, onum_i)
                if (test_key in vol_oclc_set):
                    err = "volume_id collision: '%s' '%s'" % (test_key, line)
                    print err
                    err_outfile.write(err+'\n')
                    continue
                else:
                    vol_oclc_set.add(test_key)
                if not (onum_i in oclc_set):
                    oclcs.append(onum_i)
                    oclc_set.add(onum_i)
                    oclc_counter += 1
        # only one oclc
        else:
            try:
                oclc_i = int(oclc)
                # test uniqueness
                test_key = "%s-%i" % (vol_id, oclc_i)
                if (test_key in vol_oclc_set):
                    err = "volume_id collision: '%s' '%s'" % (test_key, line)
                    print err
                    err_outfile.write(err+'\n')
                    continue
                else:
                    vol_oclc_set.add(test_key)
                if not (oclc_i in oclc_set):
                    oclcs.append(oclc_i)
                    oclc_set.add(oclc_i)
                    oclc_counter += 1
            except:
                # non integer OCLC
                err = "Unexpected: '%s'\n" % sys.exc_info()[0]
                print err
                print "%s (OCLC: %s)" % (line, oclc)
                sys.exit()
        
    # write htitem_oclc_jn table 
    print "Writing %s" % jn_outfilen
    sys.stdout.flush()
    for val in vol_oclc_set:
        vol_id, oc = val.split('-')
        outline = "%s\t%s\t0\n" % (vol_id, oc)
        jn_outfile.write(outline)
        
    print "%i rows parsed, %i oclc numbers in file." % (global_counter, len(oclc_set))
    print "%i rows had no oclc number" % (no_oclc)
    print "%i rows had incorrect number of columns" % (bad_line_counter)
    sys.stdout.flush()
    
    infile.close()
    jn_outfile.close()
    err_outfile.close()


if __name__ == '__main__':
    
    if (len(argv) != 3):
        print """Usage: 'python maketables_htitem_oclc_jn_oclc from_file.py [tab-file] [htitem_oclc_jn out]'"""
        sys.exit()
    
    ### htitem_oclc_jn and oclc table generation ###
    htfn = argv[1]
    jnfn = argv[2]
    generate_htitem_oclc_data(htfn, jnfn)
    
    print "done."
