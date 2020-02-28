# These are 4 steps that take a short time and never seem to be any
# trouble, so if you just want to run them all in one go, voila.

date;
echo 'd';
ruby step_10d.rb > step_10d.log;

date;
echo 'e';
ruby step_10e.rb > step_10e.log;

date;
echo 'f';
ruby step_10f.rb > step_10f.log;

date;
echo 'h';
ruby step_10h.rb > step_10h.log;

date;
echo 'done';
