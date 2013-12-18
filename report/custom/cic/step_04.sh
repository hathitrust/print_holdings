#!/bin/bash

awk -f sum_cic_freq_list.awk step_03_out.tsv | sort -n > step_04_out.tsv