#!/usr/bin/env python
"""
Originally /htapps/pete.babel/Code/phdb/bin/cluster_oclc3.py

This module implements a clustering approach for OCLC numbers based on 
HathiTrust data and data acquired via the OCLC Translation Table.  
This version is a re-write to try and speedup the algorithm.
v.2 - removing 'rights' queries, farm out cluster_oclc and cluster_htitem_jn
tables to core memory.
v.3 - re-implementation of the fundamental aggregation loop
v.4 - Loading the table with the output file, getting db connection data 
from conf file, truncating tables, removed some dead code. MW Jan 2014.
"""

import sys, re, os, time, MySQLdb
from hathiconf import Hathiconf

VERBOSE = 0
NOW     = time.strftime("%Y-%m-%d" + ' 00:00:00')

def get_connection():
    # open DB connection
    hc = Hathiconf()
    try:
        conn = MySQLdb.connect (
            host   = hc.get('db_host'),
            port   = int(float(hc.get('db_port'))),
            user   = hc.get('db_user'),
            passwd = hc.get('db_pw'),
            db     = hc.get('db_name'),
            local_infile = 1
            )
    except MySQLdb.Error, e:
        print "Couldn't get connection."
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit()
    return conn


def run_list_query(cursor, query):
    """ Generic query runner, appropriate for queries that return
    simple lists. """
    if VERBOSE:
        print query
    try:
        cursor.execute(query)
    except MySQLdb.Error, e:
        print "[run_list_query] Error %d: %s" % (e.args[0], e.args[1])
        print "Exiting..."
        sys.exit(1)
    items = []
    if (cursor.rowcount > 0):
        while(1):
            row = cursor.fetchone()
            if row == None:
                break
            items.append(row[0])
    return items


def run_single_query(cursor, query):
    """ Generic query runner, for a single result row """
    if VERBOSE:
        print query
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        result = row[0]
    except MySQLdb.Error, e:
        print "[run_single_query] Error %d: %s" % (e.args[0], e.args[1])
        print "Exiting..."
        sys.exit()
    return result


def create_cluster(ccursor, ocns, vol_id):
    """ Creates a new cluster and populates tables appropriately. """
    
    # insert into cluster, get id
    ocn0 = ocns[0]
    query2 = "INSERT INTO holdings_cluster (cost_rights_id, osici, last_mod) VALUES (0, '%s', '%s')" % (ocn0, NOW)
    try:
        if VERBOSE:
            print query2
        ccursor.execute(query2)
        pkid = int(ccursor.lastrowid)
    except MySQLdb.Error, e:
        print "[create_cluster] Error %d: %s" % (e.args[0], e.args[1])
        print "Exiting..."
        sys.exit()
    # insert OCNs into cluster_oclc tables
    for nocn in ocns:
        oc = int(nocn)
        if (pkid in Cluster_oclc_d):
            Cluster_oclc_d[pkid].add(oc)
        else:
            Cluster_oclc_d[pkid] = set([oc])
        if (Oclc_cluster_d.has_key(oc)):
            print "2 cluster oclc: vid=%s, oc=%i, pkid=%i, cid=%s" % (vol_id, oc, pkid, Oclc_cluster_d[oc])
            sys.exit()
        else:
            Oclc_cluster_d[oc] = pkid    
             
    # insert volume_id into cluster_htitem_jn
    query4 =  """ INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id)
                  VALUES (%s, '%s') """ % (pkid, vol_id)
    try:
        if VERBOSE:
            print query4
        ccursor.execute(query4)
    except MySQLdb.Error, e:
        print "[create_cluster] Error %d: %s" % (e.args[0], e.args[1])
        print "Exiting..."
        sys.exit()
    
    return pkid


def merge_clusters(cid1, cid2):
    """ Merges clusters together.  Uses c1's id, adds
    c2 OCNs and volume_ids to c1, resolves rights, deletes c2 entries from 
    tables. """
    
    #if VERBOSE:
    print "Merging '%i'->'%i'" % (cid2, cid1)
    sys.stdout.flush()
    lconn = get_connection()
    lcursor = lconn.cursor()
    
    # get volume_ids
    queryc1a = "SELECT volume_id FROM holdings_cluster_htitem_jn WHERE cluster_id = %s" % cid1
    queryc2a = "SELECT volume_id FROM holdings_cluster_htitem_jn WHERE cluster_id = %s" % cid2
    c1vids = run_list_query(lcursor, queryc1a)
    c2vids = run_list_query(lcursor, queryc2a)
    # insert c2 vol_ids into c1
    for vid in c2vids:
        if not (vid in c1vids):
            mcquery2 = """ INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id)  
                           VALUES (%s, '%s') """ % (cid1, vid)
            try:
                if VERBOSE:
                    print mcquery2
                lcursor.execute(mcquery2)
                lconn.commit()
            except MySQLdb.Error, e:
                print "[merge_clusters 1] Error %d: %s" % (e.args[0], e.args[1])
                print "Exiting..."
                sys.exit(1)
    # insert c2 OCNs into c1
    c2ocns = Cluster_oclc_d[cid2]
    for ocn in c2ocns:
        Cluster_oclc_d[cid1].add(ocn)
        Oclc_cluster_d[ocn] = cid1
                
    # delete c2
    del Cluster_oclc_d[cid2]
    
    mcquery5a = "DELETE FROM holdings_cluster_htitem_jn WHERE cluster_id = %s" % cid2
    mcquery5c = "DELETE FROM holdings_cluster WHERE cluster_id = %s" % cid2
    try:
        lcursor.execute(mcquery5a)
        lcursor.execute(mcquery5c)
        lconn.commit()
    except MySQLdb.Error, e:
        print "[merge_clusters 3] Error %d: %s" % (e.args[0], e.args[1])
        print "Exiting..."
        sys.exit(1)
        
    lconn.commit()
    lconn.close()    


def truncate_tables():
    """ 
    The tables holdings_cluster, holdings_cluster_oclc, and holdings_cluster_htitem_jn 
    need to be emptied before we can do anything here. Used to be a manual step, 
    scripted by mwarin 2014-01-03.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    tables = ['holdings_cluster', 'holdings_cluster_oclc', 'holdings_cluster_htitem_jn']
    for t in tables:
        count_q = "SELECT COUNT(*) AS c FROM %s" % t
        count_r = run_single_query(cursor, count_q)
        print "%s -- gave %s rows" % (count_q, count_r)
        trunc_q = "TRUNCATE %s" % t
        print trunc_q
        cursor.execute(trunc_q)
    conn.commit()
    conn.close()

def load_table():
    infile = get_loadfile_path()
    q      = "LOAD DATA LOCAL INFILE '%s' INTO TABLE holdings_cluster_oclc" % infile
    conn   = get_connection()
    cursor = conn.cursor()
    print q
    cursor.execute(q)
    conn.commit()
    conn.close()

def get_loadfile_path():
    libpath = os.environ['PYTHONPATH']
    outfn  = re.sub('\/lib\/.*', ('/data/builds/current/cluster_oclc.data'), libpath)
    if outfn == libpath:
        print "Error: An assumption about directories is wrong."
        exit(1)
    return outfn

def cluster_main():
    """ main routine to create PHDB clusters. Pass it the cursor. """
    
    conn   = get_connection()
    cursor = conn.cursor()

    # Will write to a file the data/ dir.
    outfn = get_loadfile_path()
    print "Will print to %s" % outfn
    sys.stdout.flush()

    ### outer loop over all volume_ids ###
    print "Grabbing volume_ids..."
    sys.stdout.flush()
    query1 = "SELECT DISTINCT(volume_id) FROM holdings_htitem LIMIT 0,50000"
    all_vids = run_list_query(cursor, query1)
    print "%i ids received..." % len(all_vids)
    sys.stdout.flush()

    viter = 0
    for vid in all_vids:
        viter += 1
        if (len(vid)<3):
            print "skipping: '%s'" % vid
            sys.stdout.flush()
            continue
                
        ## get the OCNs for each volume_id ##
        query3 = "SELECT oclc FROM holdings_htitem_oclc WHERE volume_id = '%s'" % vid
        ocns = run_list_query(cursor, query3)
        # skip htitems with no oclc number
        if (len(ocns) == 0):
            continue
        
        # are any OCNs already participating in other clusters? #
        pclusters = set([])
        for ocn in ocns:
            if (Oclc_cluster_d.has_key(ocn)):
                cid = Oclc_cluster_d[ocn]
                pclusters.add(cid)
        
        # if yes, aggregate
        if (len(pclusters)>0):
            # add current volume_id to lowest matching cluster number
            cids = list(pclusters)
            cids.sort()
            lcid = cids.pop(0)
            query4 =  """ INSERT INTO holdings_cluster_htitem_jn (cluster_id, volume_id)
                  VALUES (%s, '%s') """ % (lcid, vid)
            try:
                if VERBOSE:
                    print query4
                cursor.execute(query4)
            except MySQLdb.Error, e:
                print "[create_cluster] Error %d: %s" % (e.args[0], e.args[1])
                print "Exiting..."
                sys.exit()
            # add all OCNs to lowest matching cluster number
            for ocn in ocns:
                Oclc_cluster_d[ocn] = lcid
                Cluster_oclc_d[lcid].add(ocn)
            # merge remaining clusters into root cluster
            while (len(cids)>0):
                cid = int(cids.pop())
                # merge the cid with lcid
                merge_clusters(lcid, cid)
        else:
            # make new cluster
            create_cluster(cursor, ocns, vid)
            conn.commit()
            
        # export data struct every 100k
        if ((viter % 100000)==0):
            print "%s\t%s" % (time.strftime("%Y-%m-%d %H:%M:%S"), viter)
            sys.stdout.flush()
            dump_data_structure(Cluster_oclc_d, outfn)
                   
    conn.commit()
    conn.close()         
    print time.strftime("%Y-%m-%d %H:%M:%S")
    print "dumping final data structure"
    sys.stdout.flush()
    dump_data_structure(Cluster_oclc_d, outfn)
    
def dump_data_structure(dstruct, outfn):
    """ Exports one of the table data structures to a flatfile.  Structs are
    hashes of lists (sets). """
    outfile = file(outfn, 'w')
    for k, v in dstruct.iteritems():
        for val in v:
            outline = "%s\t%s\n" % (k, val)
            outfile.write(outline)
    
if __name__ == '__main__':
    print "Started %s" % time.strftime("%Y-%m-%d %H:%M:%S")
    sys.stdout.flush()

    Volid_cluster_d = {}
    Cluster_volid_d = {}    
    Cluster_oclc_d  = {}
    Oclc_cluster_d  = {}
    
    # Start with blank slate.
    truncate_tables()
    # Calculate clusters and write to file.
    cluster_main()
    # Load file into holdings_cluster_oclc.
    load_table()
    
    print "Done %s" % time.strftime("%Y-%m-%d %H:%M:%S")
