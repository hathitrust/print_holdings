cd /htapps/mwarin.babel/phdb_scripts/data/memberdata; 
perl ~/useful/tree.pl | grep HT003 | egrep -v '\.estimate|old' | sed -r 's/.+(HT003_(.+).(mono|multi|serial).tsv)/\2\t\3/g' | sort;
cd -;