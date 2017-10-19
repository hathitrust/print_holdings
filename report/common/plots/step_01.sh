#!/bin/bash

d=`date '+%Y-%m-%d'`;
tarfile="Hplot-stacked-$d.tgz";

/usr/bin/python h_distribution_report-all.py;
/usr/bin/python plot_H_histogram_stacked.py hcounts.mono.csv hcounts.multi.csv hcounts.serial.csv;
tar --create --gzip --verbose --remove-files --file ${tarfile} *.png;
rm hcounts.mono.csv hcounts.multi.csv hcounts.serial.csv;
echo "Done. Images compressed to ${tarfile}";
