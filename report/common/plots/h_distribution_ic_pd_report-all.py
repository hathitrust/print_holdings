#!/usr/bin/python
"""
Creates a report to plot H-distributions for all members in PHDB.
Exports a flatfile with a 2D H,member array.  Modify the q2string to 
choose different versions of H_count.
"""
 
import sys, re
import MySQLdb
from hathiconf import Hathiconf

# currently storing passwords in separate files in my
# project folder (under /etc).  Files are named
# after username and contain a single line.
def get_password_from_file(fn):
    pwfile = file(fn, "r")
    line = pwfile.readline()
    return line.rstrip()


def get_connection(hst, dbn, usr, pw):
    # open DB connection
    try:
        conn = MySQLdb.connect (host = hst,
                            port=3306,
                            user = usr,
                            passwd = pw,
                                db = dbn)
    except MySQLdb.Error, e:
        print "Couldn't get connection."
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit()
    return conn


def generate_hplot(dbhost, dbname, dbusr, dbpw):

    access_types = ['allow', 'deny']
    
    # set up DB
    conn = get_connection(dbhost, dbusr, dbpw)
    cursor = conn.cursor()
    
    # query1 get all members
    q1string = "select member_id, member_name from holdings_htmember where status=1 order by member_id;"
    cursor.execute(q1string)
    # loop through results
    members = []
    while (1):
        row = cursor.fetchone()
        if row == None:
            break
        # putting these together so's I can sort together
        member_key = "%s-%s" % (row[0].strip(), row[1].strip())
        members.append(member_key)
    members.sort()
    #print members
    
    # create a separate outfile for each access type
    for atype in access_types:
        outfn = "hcounts.%s.csv" % atype
        outfile = file(outfn, 'w')
        # query2
        for member in members:
            bits = member.split('-')
            print bits
            member_id = bits[0]
            member_name = bits[1]
            member_name = re.sub(',', ' -', member_name)
            q2string = "SELECT H_id, SUM(H_count) AS H_count FROM holdings_H_counts WHERE member_id='%s' AND access = '%s' GROUP BY H_id;" % (member_id, atype)
            cursor.execute(q2string)
    
            # loop through results
            counts = [0] * len(members)      # initialize an empty list
            while (1):
                row = cursor.fetchone()
                if row == None:
                    break
                h = int(row[0])
                count = int(row[1])
                counts[h] += count
            # format results
            counts.pop(0)     # eliminate the 'H=0' case
            countstr_list = [str(c) for c in counts]
            cstring = ",".join(countstr_list)
            outstr = "%s,%s,%s\n" % (member_id, member_name, cstring)
            outfile.write(outstr)
        outfile.close()
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    hc = Hathiconf();
    dbhost = hc.get('db_host')
    dbname = hc.get('db_name')
    dbusr  = hc.get('db_user')
    dbpw   = hc.get('db_pw')
    generate_hplot(dbhost, dbname, dbusr, dbpw)
    print 'done.'
