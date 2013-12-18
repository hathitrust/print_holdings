#!/bin/bash
echo -e '#items\tcc\tac' > step_03_out.tsv
egrep -o $'^([0-9]+\t[0-9]+)' step_02_out.tsv | sort | uniq -c | sort -nr >> step_03_out.tsv