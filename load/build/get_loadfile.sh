cd /htapps/mwarin.babel/phdb_scripts/data/memberdata; 
perl ~/useful/tree.pl | grep HT003 | grep -v estimate | grep -v old | sed -r 's/.+(HT003_(.+).(mono|multi|serial).tsv)/\2\t\3/g' | sort;
cd -;