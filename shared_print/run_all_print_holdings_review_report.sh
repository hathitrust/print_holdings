pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

cat ../data/shared_print_members.tsv |
    while read line
    do :
       ruby print_holdings_review_report.rb $line;
    done

ruby print_holdings_review_report.rb ivyplus;
ruby print_holdings_review_report.rb big10;
