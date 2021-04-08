# Expects $loadfile in a certain place, and that it contains rows like:
# <member_id><TAB><(mono|multi|serial)>
# ... and uploads the corresponding files to AWS.

# Been having problems with timeouts lately. Aaron sez:
#   "If it keeps failing, it would probably be straightforward to
#   change it to use awscli (aws s3 cp file s3://bucket-name/whatever),
#   or aws s3 sync if you want to copy a directory"

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

if [ -z "$s3_main_bucket" ]; then
    echo "You must set s3_main_bucket in conf/hathiconf.prop";
    exit 1;
fi

loadfile=$DATADIR/builds/current/load.tsv;
input_current="s3://$s3_main_bucket/input/Current";

echo `date` Started;

upload_to_aws () {
    echo member $1 type $2;
    # Turn on the -n flag for dry-run.
    s3cmd --no-progress put $HTDIR/HT00*_$1.$2.tsv $input_current/;
}

# Skip commented-out lines. Only get ones with a tab.
egrep -v '#' $loadfile | egrep -o $'([^\t]+\t[^\t]+)' | while read -r line ; do
    upload_to_aws $line
done

echo `date` Finished;
