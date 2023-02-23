#!/usr/local/bin/perl

# File: Update_ExtentAttribute.pl
# Author: 
# Created: March 17, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# Update_ExtentAttribute.pl Takes a tab-delimited file with database, Extent_id, 
# ExtentAttributeType, and value and it inserts/update the value of that ExtentAttribute

=head1 NAME
    
    Update_ExtentAttribute.pl
    
=head1 USAGE

    Update_ExtentAttribute.pl [-]-in[put_file] <attribute_values_table> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-in[put_file] <attribute_values_table>

Tab-delimited file with database, Extent_id, ExtentAttributeType, and value

=for Euclid:
    attribute_values_table.type: readable

=back

=head1 OPTIONS

=over

=item [-]-dont_load

It simulates the loading, but it does not actually load anything in the databases

=item [-]-ignore_empty_files

This option is used when the script is run as part of a cron job and it is acceptable to have "dry runs"


=item [-]-server <db_server> | -S <db_server>

Database server (default: SYBPROD)

=for Euclid:
    db_server.type:    string
    db_server.default: 'SYBPROD'

=item [-]-pass[word]_file <pwd_file>

File with database username and password (username on the first line, password on the second).

=for Euclid:
    pwd_file.type: readable

=item [-]-debug [<log_level>]

Logging threshold for the error log file.
Valid values: TRACE, DEBUG, INFO, WARN, ERROR, and FATAL
Not specifying any value, it will default to 'DEBUG'

=for Euclid:
    log_level.type:        string
    log_level.default:     'NOT_SET'
    log_level.opt_default: 'DEBUG'
    
=item [-]-log[_file] <error_log_file>

Local logging file, used to monitor the process.

=for Euclid:
    error_log_file.type:   writeable

=item --help

    Prints this documentation and quit
    
=back

=head1 DESCRIPTION

Update_ExtentAttribute.pl Takes a tab-delimited file with database, Extent_id, ExtentAttributeType, and value and it inserts/update the value of that ExtentAttribute

=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
use lib ("$FindBin::Bin/../lib");
use Getopt::Euclid 0.2.4 (qw(:vars));

#use Data::Dumper;
use File::Basename;
use File::Path;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use TIGR::GLKLib;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant DEFALUT_START_DB => 'giv';
use constant ELVIRA_PROP_FILE  => '/usr/local/devel/VIRIFX/software/Elvira.Java.props';

our ($ARGV_input_file, $ARGV_dont_load, $ARGV_ignore_empty_files, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

my $jdb = defined($ARGV_password_file) ? JCVI::DB_Connection::VGD_Connect->new(db => DEFALUT_START_DB, server => $ARGV_server, pass_file => $ARGV_password_file) :
                                         JCVI::DB_Connection::VGD_Connect->new(db => DEFALUT_START_DB, server => $ARGV_server);

my $glk  = TIGR::GLKLib->new($jdb->dbh());
$glk->setLogger($logger);

open(my $table, $ARGV_input_file) || $logger->logdie("Impossible to open the file \"$ARGV_input_file\" for reading.");
my %samples = ();
my $tot_good = 0;

while (<$table>) {
    chomp();
    if (/^\s*$/ || /^#/) {
        next;
    }
    my ($db, $eid, $attr_type, $val) = split /\t/;
    my $good = 1;
    
    if (!defined($db) || $db !~ /\S+/) {
        $logger->error("undefined/missing database name (Line $.: \"$_\") - skipping this line.");
        $good = 0;
    }
    elsif (!$glk->isVgdDb($db)) { ## No warnings, since this is a cronjob script and we are preserving in JIRA all the history of any sample, independently from the database being on or off-line
        next
    }
    if (!defined($eid) || $eid !~ /^\d+$/) {
        $logger->error("Undefined/missing/invalid Extent_id (Line $.: \"$_\") - skipping this line.");
        $good = 0;
    }
    if (!defined($attr_type) || $attr_type !~ /^\w+$/) {
        $logger->error("undefined/missing ExtentAttributeType (Line $.: \"$_\") - skipping this line.");
        $good = 0;
    }
     if (!defined($val) || $val !~ /\S/) {
        $logger->error("undefined/missing Attribute value (Line $.: \"$_\") - skipping this line.");
        $good = 0;
    }
    if ($good) {
        ++$tot_good;
    }
    else {
        next;
    }
    push(@{$samples{$db}}, [$eid, $attr_type, $val]);
}
close($table);

unless ($tot_good) {
    if ($ARGV_ignore_empty_files) {
        $logger->info("No valid records present in input list \"$ARGV_input_file\" Exiting without updating the database.");
        exit(0);
    }
    else {
        $logger->logdie("Not a single valid attribute to be updated has been found in the input file \"$ARGV_input_file\".");
    }
}
my ($updated, $errors) = (0) x 2;

while (my ($db, $data) = each(%samples)) {
    $logger->info("Now processing samples from database $db.");
    $glk->changeDb($db);
    
    foreach my $sample (@{$data}) {
        my ($eid, $attr_type, $val) = @{$sample};
        my $message = "Extent $eid - Attribute Type \"$attr_type\" - Value: \"$val\"";
        
        if ($ARGV_dont_load) {
            $message .= ' - Simulating the loading.';
            $logger->info($message);
            ++ $updated;
        }
        else {
            if ($glk->setExtentAttribute($eid, $attr_type, $val)) {
                $message .= ' - Attribute updated';
                $logger->info($message);
                ++ $updated;
            }
            else {
                $message .= ' - Problems inserting/updating the attribute.';
                $logger->error($message);
                ++$errors;
            }
        }
    }
}
if ($errors) {
    my $plural = $errors == 1 ? '' : 's';
    my $att_pl = $updated == 1 ? '' : 's';
    $logger->logdie("The script produced $errors error$plural attempting to load the attribute in the list \"$ARGV_input_file\"\n$updated attribute$att_pl were successfully inserted/updated.");
}
else {
    my $plural = $updated == 1 ? '' : 's';
    $logger->info("Done. Inserted/Updated $updated attribute$plural from the list \"$ARGV_input_file\" without a single problem.");
}
