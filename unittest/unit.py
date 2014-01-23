from hathiconf import Hathiconf

h = Hathiconf()

dbu = h.get('db_user')
print "db_user is %s" % (dbu)

if dbu == '':
    raise Exception("db_user was not supposed to be empty")

non = h.get('nonesuch')
print "nonesuch is %s" % (non)

if not (non == ''):
    raise Exception("nonesuch was supposed to be empty")

print "Terrible python unit test passed."
