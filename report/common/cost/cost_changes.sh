#!/bin/bash
# MW Nov 2017.
# Attempting to automate the construction of the cost report so that it takes as little manual work as possible.
# Re-creates the whole cost history from scratch each time, and outputs 3 separate report files.

root_dir="/htapps/mwarin.babel/phdb_scripts";
ymd=`date +'%Y-%m-%d'`;

# Get the lines with a current member_id (or header).
member_list=`ruby $root_dir/lib/active_members.rb | tr '\n' '|'`;
keep_lines="^($member_list";
keep_lines+="_header)\t";

# Get all raw cost reports.
function find_all_cost_reports () {
    find $root_dir/data/costreport/ -regex '.*/costreport_[0-9]+.tsv' | sort;
}

# Get member_id and total for each member.
function print_member_totals () {
    # Assumes that member_id is always in column 1 and total cost is always in column 8.
    xargs -I{} bash -c 'cut -f1,8 $1 > $1.total' -- {};
}

# Find all totals files and append them.
function append_totals () {
    perl $root_dir/report/append_sheets.pl --header $all_report_totals;
}

# Compare sheet n with n+1 for diff over builds.
function diff_totals () {
    perl $root_dir/report/append_sheets.pl --header --op='-' $all_report_totals;
}

# Compare sheet n with n+1 for diff% over builds.
function diff_percent_totals () {
    perl $root_dir/report/append_sheets.pl --header --op='%' $all_report_totals;
}

# Header values are full file path. Shorten to just the date part.
function clean_header () {
    sed -r 's/[^\t]+?_([0-9]+).tsv.total/\1/g';
}

# Putting it all together.
find_all_cost_reports | print_member_totals;
all_report_totals=`find $root_dir/data/costreport/ -regex '.*/costreport_[0-9]+.tsv.total' | sort -n | tr '\n' ' '`;
# Output reports.
append_totals       | clean_header | grep -P $keep_lines > $root_dir/data/append_totals_$ymd.tsv;
diff_totals         | clean_header | grep -P $keep_lines > $root_dir/data/diff_totals_$ymd.tsv;
diff_percent_totals | clean_header | grep -P $keep_lines > $root_dir/data/diff_percent_totals_$ymd.tsv;
