#!/bin/bash

awk -f condense_mysql_outpt.awk step_01_out.tsv | sort -n > step_02_out.tsv