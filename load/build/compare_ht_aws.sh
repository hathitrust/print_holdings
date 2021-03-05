# A quick and dirty filename-only comparison of the HT003 files on server vs AWS.

s3cmd ls s3://umich-lib-phdb-1/input/Current/ | grep -Po 'HT003.+'   | sort > aws_files.txt
ls -w1 /htapps/mwarin.babel/phdb_scripts/data/loadfiles | grep HT003 | sort > ht_files.txt

md5sum aws_files.txt ht_files.txt
