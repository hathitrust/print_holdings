require 'hathidata';
require 'hathidb';
require 'hathilog';

infn = ARGV.shift;
hdin = Hathidata::Data.new(infn).open('r');

# Do stuff here.
# Read file with one-record per row updates.
# Check header and make sure it is OK.
# Update shared_print_commitments accordingly.

hdin.close();
