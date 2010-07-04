#!/usr/bin/perl

######################################################################
# CSV2Ledger.pl
#
# This script reads a CSV file and outputs it in Ledger format. It is
# useful for converting data from financial institutions into your
# Ledger.
#
# Optional processing matches records by regexp and can perform
# substitutions, as well as choose a source and destination account
# for that record. This can automatically rename vendors, and assign
# records to specific accounts.
#
# Written by: Russell Adams <rladams@adamsinfoserv.com>
# License: GPLv2
#
#
# Ledger entry model
#
# CSV from bank:
#
# 2008/08/08,2134,Exxon,20
#
# %Transaction = (
#     'Date' = '2008/08/08'                # CSV
#     , 'Cleared' = ''                     # Default
#     , 'CheckNum' = '2134'                # CSV
#     , 'Desc' = 'Exxon'                   # CSV
#     , 'Source' = 'Liabilities:VISA'      # Via Lookup
#     , 'Dest' = 'Expenses:Auto:Gas'       # Via Lookup
#     , 'Amount' = 20                      # CSV
#     , 'MD5Sum' = '.......'               # Generated
#     , 'CSV'    = 'csv data'              # Generated
#     );
#
# becomes
#
# 2008/08/08 (2134) Exxon
#  Liabilities:VISA         $20.00
#  Expenses:Auto:Gas
#  (CSV2Ledger:MD5Sum)   ; MD5Sum: .......
#  (CSV2Ledger:CSV)      ; CSV: csv data
#
##################################################

# Always
use strict;
use warnings;

# Modules
use YAML;            # Dispatch Table Loading
use Getopt::Long qw(:config bundling);    # Options processing
use File::Find;
use Date::Format;    # Date conversion
use Date::Parse;
use Smart::Comments -ENV, "###";    # Ignore my headers
use Digest::MD5 qw/md5_hex/;        # Help detect duplicates later

# Globals
use Template;
my $TT = Template->new();

use Text::CSV;                      # Required for import
my $CSV = Text::CSV->new();

my @Data;

my %Options;

my $Total        = 0;
my $Skipped      = 0;
my $MatchedFiles = 0;

##################################################
# Defaults
#
# Different CSV files use different fields.
# These are the defaults from my financial
# institution, and can be specified via arguments.

%Options = (

# RecordRE is used to match lines to import, and exclude any comments or junk, Mine all start with the date

    'RecordRE' => "^[0-9][0-9]\/[0-9][0-9]\/",

# CSV field layout, a comma delimited list of fields being read from each line, This is used to match position to label
    'CSVFields' =>
"Posted Date,Check Number,Description,Transaction Amount,Principal Amount,Interest Amount,Balance,Fee Amount",

    'DateField' => "Posted Date",    # Which field name is our date?

    'CheckField' => "Check Number",  # Which field is a check number?

    'DescField' => "Description",    # Which field is the description?

    'AmountField' => "Transaction Amount",    # Which field is the amount?

    'DefaultSource' => "Assets:Unknown",      # What default source account?

    # Include default files now if they exist in the current dir
    'FileMatchFile' => -e "./FileMatches.yaml"
    ? "./FileMatches.yaml"
    : undef,

    'AccountMatchFile' => -e "./AccountMatches.yaml"
    ? "./AccountMatches.yaml"
    : undef,

    'PreProcessFile' => -e "./PreProcess.yaml"
    ? "./PreProcess.yaml"
    : undef,

    # Simple options
    'ShowHelp'   => undef,
    'Cleared'    => undef,
    'Negate'     => undef,
    'DetectDups' => undef,
    'FindFiles'  => undef,
    'FindDir'    => ".",
    'InputFile'  => undef,
    'OutputDir'  => ".",
    'OutputFile' => "CSV2Ledger.dat",

);

# End defaults
##################################################

##################################################
# Options processing

GetOptions(
    'r=s'    => \$Options{'RecordRE'},
    'c=s'    => \$Options{'CSVFields'},
    'd=s'    => \$Options{'DateField'},
    'n=s'    => \$Options{'CheckField'},
    't=s'    => \$Options{'DescField'},
    'a=s'    => \$Options{'AmountField'},
    's=s'    => \$Options{'DefaultSource'},
    'x'      => \$Options{'Cleared'},
    'g'      => \$Options{'Negate'},
    'z'      => \$Options{'DetectDups'},
    'X'      => \$Options{'FindFiles'},
    'G=s'    => \$Options{'FindDir'},
    'f=s'    => \$Options{'FileMatchFile'},
    'p=s'    => \$Options{'PreProcessFile'},
    'm=s'    => \$Options{'AccountMatchFile'},
    'i=s'    => \$Options{'InputFile'},
    'o=s'    => \$Options{'OutputFile'},
    'D=s'    => \$Options{'OutputDir'},
    'h|help' => \$Options{'ShowHelp'}
) || die 'Failed to get options';

defined $Options{'ShowHelp'} && do {
    print <<EOF;
Usage: CSV2Ledger [OPTION] -i FILE -o FILE
Converts CSV entries to Ledger format.

Options:
 -i <file>                Input filename
 -o <file>                Output filename
 -D <directory>           Base directory for output files
 -r "regexp"              Record matching RE
 -c "label,label,<...>"   Field label list
 -d "label"               Date field label
 -n "label"               Check Number field label
 -t "label"               Description field label
 -a "label"               Amount field label
 -x                       Mark transactions cleared
 -g                       Negate the transaction amount
 -z                       Turn on duplicate detection (SLOW)
 -X                       Turn on file location
 -G <directory>           Source directory for files search
 -s "Account"             Default Source Account
 -f <file>                File matching table YAML file
 -p <file>                Preprocess table YAML file
 -m <file>                Account matching table YAML file

EOF
    exit;
};

$Options{'Cleared'} = defined $Options{'Cleared'} ? "* " : "";
$Options{'Negate'}  = defined $Options{'Negate'}  ? -1   : 1;

defined $Options{'InputFile'}  || die "Specify an input file";
defined $Options{'OutputFile'} || die "Specify an output file";

( -f $Options{'InputFile'} ) || die "Specify an existing file";
( defined $Options{'FindFiles'} ) && do {
    ( -d $Options{'FindDir'} ) || die "Specify an existing directory for files";
};

### Default + CLI Options: %Options

######################################################################
# Table of dynamic preprocessing
#
# Each match will be compared against the transaction prior to being
# split. The the match works, then the substitution will execute.
#

my @PreProcessReTable = ();    # Now imported via YAML

sub PreProcess {

    my ($orig) = @_;

    for my $CurRe (@PreProcessReTable) {

        # If we match
        eval "\$orig =~ $CurRe->[0]" &&

          # Apply the substition
          eval "\$orig =~ $CurRe->[1]";

    }

    return $orig;

}

#
##################################################

######################################################################
# Table of dynamic preprocessing
#
# Each match will be compared against the transaction prior to being
# split. The the match works, then the accounts are returned.
#

my @AccountMatchTable = ();    # Now imported via YAML

sub AccountMatch {

    my ($orig) = @_;

    for my $CurRe (@AccountMatchTable) {

        # If we match
        eval "\$orig =~ $CurRe->[0]" &&

          # Return first match
          return {
            'Source' => $CurRe->[1],
            'Dest'   => $CurRe->[2]
          };
    }

    return {
        'Source' => $Options{'DefaultSource'},
        'Dest'   => 'Expense:Unknown'
    };

}

#
##################################################

######################################################################
# Table of file specific options
#
# Options to load based on the input filename

my @FileMatchTable = ();    # Now imported via YAML

sub FileMatch {

    for my $CurHash (@FileMatchTable) {

        # If we match the input filename
        if ( defined $CurHash->{'FileRe'} ) {

            if ( eval "\$Options{'InputFile'} =~ $CurHash->{'FileRe'}" ) {

                # Override the options hash by loading from the file
                map( $Options{$_} = $CurHash->{$_}, keys %{$CurHash} );

                return;

            }

        }

        # If we match a line in the file, like a header with account #
        if ( defined $CurHash->{'HeaderRe'} ) {

            # Open input file
            open( TEST, "<" . "$Options{'InputFile'}" )
              || die "Failed to open $Options{'InputFile'} for reading";

            # Grep the input file for the header
            if ( grep( /$CurHash->{'HeaderRe'}/, <TEST> ) ) {

                # Override the options hash by loading from the file
                map( $Options{$_} = $CurHash->{$_}, keys %{$CurHash} );

                return;

            }

            close TEST;

        }

    }

}

#
##################################################

##############################
# MAIN

# Load FileMatch dispatch table
defined $Options{'FileMatchFile'} && do {

    @FileMatchTable = YAML::LoadFile( $Options{'FileMatchFile'} );

    # Load our options.
    &FileMatch();

    ### FileMatch Updated Options: %Options

};

# Parse the field names into the field array
my @Fields = split( /,/, $Options{'CSVFields'} );

### @Fields

# Load PreProcess dispatch table
defined $Options{'PreProcessFile'} && do {

    @PreProcessReTable = YAML::LoadFile( $Options{'PreProcessFile'} );

    #### @PreProcessReTable

};

# Load AccountMatch dispatch table
defined $Options{'AccountMatchFile'} && do {

    @AccountMatchTable = YAML::LoadFile( $Options{'AccountMatchFile'} );

    #### @AccountMatchTable

};

# Open input/output
open( FH, $Options{'InputFile'} ) || die "Failed to open $Options{'InputFile'}";

# Main loop, one line at a time from our input CSV
while (<FH>) {

    my %Transaction;

    chomp;

    #### Original CSV: $_

    # Only process records matching the RecordRE
    # This skips comments and cruft
    /$Options{'RecordRE'}/ || next;

    # Up transaction count
    $Total++;
    #### $Total

    # Keep the original for ledger comments
    my $OrigCSV = $_;

    # Run the preprocessor, don't assume $_
    $_ = &PreProcess($_);

    #### CSV After PreProcess: $_

    # Use the CSV module to pull in the input line
    my $Status  = $CSV->parse($_);
    my @Columns = $CSV->fields();

    #### @Columns

    # Check column count
    die "Field count did not match in $Options{'InputFile'}"
      if ( $#Columns ne $#Fields );

    # Fast way to put the columns into the hash by field
    # both arrays must match by position
    @Transaction{@Fields} = @Columns;

    #### %Transaction

    # Create transaction
    # Includes date processing, Date::Parse is pretty intelligent
    # so I don't have to give it an input format string
    my $TempTrans = {
        'Date' => time2str(
            "%Y/%m/%d", str2time( $Transaction{ $Options{'DateField'} } )
        ),
        'Cleared'  => $Options{'Cleared'},
        'CheckNum' => $Transaction{ $Options{'CheckField'} },
        'Desc'     => $Transaction{ $Options{'DescField'} },
        'Amount'   => $Transaction{ $Options{'AmountField'} } *
          $Options{'Negate'},
        'MD5Sum' => md5_hex($OrigCSV)    # later dup detect
        , 'CSV' => $OrigCSV              # reference
    };

    # Match our source / dest accounts
    $TempTrans = { %{$TempTrans}, %{ &AccountMatch($_) } };

    # Set to default if undefined
    if ( !defined $TempTrans->{'Source'} ) {
        $TempTrans->{'Source'} = $Options{'DefaultSource'};
    }

    # Set to default if empty
    if ( $TempTrans->{'Source'} eq '' ) {
        $TempTrans->{'Source'} = $Options{'DefaultSource'};
    }

    # Only include check num ()'s if present
    if ( $TempTrans->{'CheckNum'} ne '' ) {
        $TempTrans->{'CheckNum'} = "(" . $TempTrans->{'CheckNum'} . ") ";
    }

    #### $TempTrans

    #########################################################
    # Duplicate Detection
    #
    # Yes, I'm reopening and scanning the file each time.
    # Its terribly inefficient, and a real hack.
    # If your ledger file is long enough this is causing a
    # problem, consider archiving. ;]

    defined $Options{'DetectDups'} && do {

        # If the output file doesn't exist, don't sweat it
        ( -f $Options{'OutputFile'} ) && do {

            # Open output file
            open( DUP, "<" . "$Options{'OutputFile'}" )
              || die "Failed to open $Options{'OutputFile'} for reading";

            # Grep the output file for the MD5 sum, skip if present.
            if ( grep( /$TempTrans->{'MD5Sum'}/, <DUP> ) ) {
                close DUP;
                print STDERR "Skipping $TempTrans->{'MD5Sum'}\n";
                $Skipped++;
                next;
            }

            close DUP;

        };

    };

    ##################################################
    # Find Files
    #
    # Attach files that match the txn to the payee.
    # Filenames should be in the form: YYYYMMDD_Vendor_Dollars_Cents.*$
    # Vendor name isn't likely to match, so lets focus on the date and dollars
    # Then we use the <<file:/dir/filename [,file:...] >> attachment syntax

    my $TempAmount = sprintf( "%.2f", $Transaction{ $Options{'AmountField'} } );
    $TempAmount =~ tr/./_/;

    my $SearchRegexp =
        time2str( "%Y%m%d", str2time( $Transaction{ $Options{'DateField'} } ) )
      . '_' . '.*?' . '_'
      . $TempAmount . '[_.]';

    #### $SearchRegexp

    my $FilesList = "";

    find(
        sub {
            if (/$SearchRegexp/) {
                if ($FilesList) { $FilesList .= ','; }
                $FilesList .= 'file:' . $File::Find::name;
                print STDERR "Matched $_ to "
                  . $Transaction{ $Options{'DateField'} } . " "
                  . $Transaction{ $Options{'DescField'} } . " "
                  . $Transaction{ $Options{'AmountField'} } . "\n";
                $MatchedFiles++;
            }
        },
        $Options{'FindDir'}
    );

    if ($FilesList) { $FilesList = '<<' . $FilesList . '>>'; }

    ########################################
    # Output

    # Add fileslist for tempate
    $TempTrans->{'FilesList'} = $FilesList;

    my $TxnOutputTemplate = <<EOF;
[% USE format %][% ToDollars = format('%30.2f') %]
[% Date %] [% Cleared %][% CheckNum %][% Desc %] [% FilesList %]
	[% Source %]	[% ToDollars(Amount) %]
	[% Dest %]
	(CSV2Ledger:MD5Sum)   ; MD5Sum: [% MD5Sum  %]
	(CSV2Ledger:CSV)      ; CSV: [% CSV %]

EOF

    $TT->process( \$TxnOutputTemplate, $TempTrans,
        ">" . "$Options{'OutputFile'}" );

}

close FH;

# Output totals
printf(
    "*** Imported %d / Skipped %d / Total %d\n",
    $Total - $Skipped,
    $Skipped, $Total
);

printf( "*** Files Matched %d of Imported %d\n",
    $MatchedFiles, $Total - $Skipped );

# END
##################################################
