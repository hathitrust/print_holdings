gs=`git status . | grep 'modified:'`;

if [ -z "$gs" ]; then
    echo "No uncommitted changes, you may proceed.";
else
    echo "There are uncommitted changes:";
    echo "$gs";
    echo -n "Proceed anyways? y/n >> ";
    read proceed_yn;
    echo "";
    if [ "$proceed_yn" = "y" ]; then
	echo "You may proceed.";
	exit 0;
    elif [ "$proceed_yn" = "n" ]; then
	echo "Exiting.";
	exit 1;
    else
	echo "Bad answer.";
	exit 1;
    fi
fi
