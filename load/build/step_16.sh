#!/bin/bash

bt="holdings_htitem_htmember_jn";

echo "Back up table $bt"
./backuptable.sh $bt;

ruby populate_holdings_htitem_htmember_jn_dev.rb