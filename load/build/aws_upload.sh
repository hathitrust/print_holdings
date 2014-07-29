# Takes a path to a tab-separated file with format:
# <member_id><TAB><(mono|multi|serial)>
# ... and uploads the corresponding files to AWS.

pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;
source $SCRIPTPATH/build_lib.sh;

input_current="s3://umich-lib-phdb-1/input/Current";

upload_to_aws () {
    echo member $1 type $2;
    # Turn on the -n flag for dry-run.
    s3cmd --no-progress put $HTDIR/HT00*_$1.$2.tsv $input_current/;
}

# Skip commented-out lines. Only get ones with a tab.
egrep -v '#' $1 | egrep -o $'([^\t]+\t[^\t]+)' | while read -r line ; do
    upload_to_aws $line
done
