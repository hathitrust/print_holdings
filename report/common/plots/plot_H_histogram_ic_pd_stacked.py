#!/usr/bin/python
"""
Code to plot a dual histogram of the H-distribution data, ic and pd separate.
"""

import sys
import matplotlib
matplotlib.use('Agg') # need this if doing ssh
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

MAX_H = 100 # it is terrible that this is hardcoded.

def plot_H_stack(id, title, hcounts_ic, hcounts_pd):
    """ Takes a list of H-counts and plots them as a bar chart """
    fig_name = "%s-Hplot_ic_pd-stacked.png" % id
    print "making %s ..." % fig_name
    llen = max(len(hcounts_ic), len(hcounts_pd))
    xterms = range(1, llen+1)

    # font
    font1 = {'family' : 'stix',
             'weight' : 'bold',
             'size'   : 8}
    font2 = {'family' : 'normal',
             'weight' : 'normal',
             'size'   : 10}
    font3 = {'family' : 'normal',
             'weight' : 'bold',
             'size'   : 24}

    fig = plt.figure()
    bwidth = 0.75

    # set axes
    axis = [int(x) for x in range(1, llen+1)]
    xtics = [int(x) for x in range(0, llen+1, 5)]
    ptitle = "%s" % title
    plt.title(ptitle, font3)
    plt.xlabel('H', font3)
    plt.ylabel('Count', font3)
    plt.xticks(xtics)
    plt.bar(axis, hcounts_ic, width=bwidth, align='center', color='#0000ee', edgecolor='#0000ee', label='IC')
    plt.bar(axis, hcounts_pd, width=bwidth, align='center', color='#eeaa00', edgecolor='#eeaa00', bottom=hcounts_ic, label='PD')
    plt.legend()
    plt.savefig(fig_name)

def parse_Hfiles(icfilen, pdfilen):
    """ Parses the 'h_distribution_report.py' outfile to plot all distributions.  Assumes
    there are individual files for ic and pd holdings """

    icfile = file(icfilen)
    pdfile = file(pdfilen)

    # parse ic & pd datafiles, create hashes of each keyed on member_id
    ic_map = {}
    for line in icfile.readlines():
        bits = line.split(',')
        member_id = bits[0]
        hcounts = bits[2:]
        data1 = [int(c) for c in hcounts]
        ic_map[member_id] = data1

    # loop and stack
    for line in pdfile.readlines():
        bits = line.split(',')
        member_id = bits[0]
        member_name = bits[1]
        hcounts = bits[2:]
        data_pd = [int(c) for c in hcounts]
        # this is inefficient, but whatever...
        data_ic = ic_map[member_id]
        np_ic = np.array(data_ic)
        np_pd = np.array(data_pd)
        plot_H_stack(member_id, member_name, np_ic, np_pd)

if __name__ == '__main__':

    ic = sys.argv[1]
    pd = sys.argv[2]

    parse_Hfiles(ic, pd)

    print "done."
