import sys, re, os

""" 
Python rewrite of the ruby Hathiconf. I thought it easier to rewrite
a few simple core modules and live with a few of Pete's python scripts
than trying to rewrite them all to ruby. The long term goal, though,
is for everything to be ruby.

Martin Warin, 2014-01-22.
"""

THIS_PATH        = os.path.realpath(__file__)
GLOBAL_CONF_PATH = os.path.realpath('/etc/conf/hathiconf.prop')
LOCAL_CONF_PATH  = os.path.realpath(THIS_PATH + '../../../conf/hathiconf.prop')

g_read = os.path.exists(GLOBAL_CONF_PATH)
l_read = os.path.exists(LOCAL_CONF_PATH)

if (not g_read) and (not l_read) :
    raise Exception("No readable conf files")

class Hathiconf:
    def __init__(self):        
        self.mem = {};
        self.read_conf(GLOBAL_CONF_PATH)
        self.read_conf(LOCAL_CONF_PATH)

    def read_conf(self, path):
        if os.path.exists(path):
            print "reading conf %s" % (path)
            with open(path) as f:
                for line in f:
                    line = line.rstrip()
                    if line.__len__() == 0:
                        continue
                    comment_match = re.search('^\s*#', line)
                    if comment_match == None:
                        m = re.match('^\s*([a-z_0-9]+)\s*=\s*(.+)\s*$', line)
                        if m:
                            key = m.group(1)
                            val = m.group(2)
                            self.mem[key] = val

    def get(self, key):
        if key in self.mem:
            return self.mem[key]
        print "Config could not find key %s\n" % (key)
        
        return ''
                            
if __name__ == "__main__":
    print "ok"
    h = Hathiconf()

    print "db_user is %s" % (h.get('db_user'))
    print "nonesuch is %s" % (h.get('nonesuch'))
