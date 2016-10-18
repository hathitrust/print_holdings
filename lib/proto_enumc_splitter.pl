use strict;
use warnings;

=pod

Imagine, if you will, a file that looks like:

OCLC  <TAB> ID   <TAB> STATUS  <TAB> CONDITION  <TAB> ENUMC            <TAB> GOVDOC
555   <TAB> 123  <TAB> CH      <TAB> BRT        <TAB> v.1;v.2;v.3      <TAB> 0
5050  <TAB> 678  <TAB> LM      <TAB>            <TAB> v.1pt.1;v.1pt.2  <TAB> 1
999   <TAB> 999  <TAB> WD      <TAB>            <TAB> suppl            <TAB> 1

Call thusly:

perl proto_enumc_splitter.pl INPUTFILE > OUTPUTFILE

Your outputfile will look like:

OCLC  <TAB> ID   <TAB> STATUS  <TAB> CONDITION  <TAB> ENUMC     <TAB> GOVDOC
555   <TAB> 123  <TAB> CH      <TAB> BRT        <TAB> v.1       <TAB> 0
555   <TAB> 123  <TAB> CH      <TAB> BRT        <TAB> v.2       <TAB> 0
555   <TAB> 123  <TAB> CH      <TAB> BRT        <TAB> v.3       <TAB> 0
5050  <TAB> 678  <TAB> LM      <TAB>            <TAB> v.1 pt.1  <TAB> 1
5050  <TAB> 678  <TAB> LM      <TAB>            <TAB> v.1 pt.2  <TAB> 1
999   <TAB> 999  <TAB> WD      <TAB>            <TAB> suppl     <TAB> 1

In the examples above, "<TAB>" is used for illustration purposes.
In reality, use the actual horizontal tab character.

=cut

# Using tab as column delimiter, replace as appropriate.
my $col_delimiter = "\t";
# Using semicolon as delimiter between enumcs inside column, replace as appropriate.
my $enumc_delimiter = ";";

# Reads one line at a time.
while (<>) {
    my $line = $_;
    # Strips off newline.
    chomp $line;
    # Split into columns and assign to variables. Rename/reorder as appropriate.
    my ($oclc, $id, $status, $cond, $enumc, $govdoc) = split($col_delimiter, $line);
    if ($enumc =~ m/$enumc_delimiter/) {
	# If the $enumc col contains the $enumc_delimiter, then:
	# Loop over the values for $enumc
	foreach my $enumc_part (split($enumc_delimiter, $enumc)) {
	    # Print one line (with all other cols the same) for each $enumc_part.
	    print join($col_delimiter, ($oclc, $id, $status, $cond, $enumc_part, $govdoc)) . "\n";
	}
    } else {
	# If there were no $enumc_delimiter in $enumc, just print $line.
	print $line . "\n";
    }
}
