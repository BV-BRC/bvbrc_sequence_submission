#!/usr/local/bin/perl

# File: Update_GLK_Jira_Status.pl
# Author: 
# Created: February 18, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# Update_GLK_Jira_Status.pl Queries GLK for all samples having a 'jira_id' attribute, generates a list
# of jira_ids, call the script pullJiraStatus, parses the output of that script and updates all GLK records
# whose jira_status attribute doesn't exist or doesn't match the current jira_status in SampleTracking Jira.

=head1 NAME
    
    Update_GLK_Jira_Status.pl
    
=head1 USAGE

    Update_GLK_Jira_Status.pl [options]

=head1 OPTIONS

=over


=item [-]-dont_load

It simulates the loading, but it does not actually load anything in the databases

=item [-]-server <db_server> | -S <db_server>

Database server (default: SYBPROD)

=for Euclid:
    db_server.type:    string
    db_server.default: 'SYBPROD'

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

Update_GLK_Jira_Status.pl Queries GLK for all samples having a 'jira_id' attribute, generates a list of jira_ids, call the script pullJiraStatus, parses the output of that script and updates all GLK records whose jira_status attribute doesn't exist or doesn't match the current jira_status in SampleTracking Jira.



=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
#use lib ("$FindBin::Bin/../perllib");
use Getopt::Euclid 0.2.4 (qw(:vars));
#use Data::Dumper;

use File::Path;
use File::Basename;
use TIGR::GLKLib;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use DateTime;

## Constants declaration
#
use constant TEMP_FILES_DIR    => '/usr/local/scratch/VIRAL/ST';
use constant START_DB          => 'giv';
use constant GLK_QUERYING_PRG  => "$FindBin::Bin/Get_All_Samples_w_Jira_ID.pl";
use constant JIRA_QUERYING_PRG => "$FindBin::Bin/pullJiraStatus";
use constant GLK_UPDATING_PRG  => "$FindBin::Bin/Compare_Jira_Status_And_Update_GLK.pl";


our ($ARGV_dont_load, $ARGV_server, $ARGV_debug, $ARGV_log_file);

my $dt = DateTime->now();
my $files_uniquifier = TEMP_FILES_DIR . '/Update_GLK_Jira_Status'  . '_' . $dt->ymd() . '_' . $dt->hms('.') . '_' . $$;

unless (defined($ARGV_log_file)) {
    $ARGV_log_file = "$files_uniquifier.log";
} 

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Composing the various 

my $glk_table   = "$files_uniquifier.glk.table";
my $jira_map    = "$files_uniquifier.jira.map";
my $jira_ids    = "$files_uniquifier.jira_id.list";
my $pull_log    = "$files_uniquifier.GetAllSamples.log";
my $upd_log     = "$files_uniquifier.CompareJiraStatusAndUpdate.log";
my $upd_summary = "$files_uniquifier.CompareJiraStatus_Updates.summary";

## Running the scripts

my $pull_glk_cmd  = GLK_QUERYING_PRG  . " --jira_id_list $jira_ids --jira_attr_table $glk_table --log_file $pull_log --debug $ARGV_debug";
my $jira_pull_cmd = JIRA_QUERYING_PRG . " -input $jira_ids -output $jira_map";
my $glk_upd_cmd   = GLK_UPDATING_PRG  . " --jira_map $jira_map --jira_attr_table $glk_table --summary $upd_summary --log_file $upd_log --debug $ARGV_debug";

if ($ARGV_dont_load) {
    $glk_upd_cmd .= ' --dont_load';
}
## Pullind all samples with jira_id from GLK...
$logger->trace("Now Running the command: \"$pull_glk_cmd\"");
system($pull_glk_cmd) && $logger->logdie("Problem running the command: \"$pull_glk_cmd\"");

unless (-s $glk_table && -s $jira_ids) {
    $logger->logdie("The command \"$pull_glk_cmd\" ended without error codes, but the output files are empty.");
}

$logger->trace("Now Running the command: \"$jira_pull_cmd\"");
system($jira_pull_cmd) && $logger->logdie("Problem running the command: \"$jira_pull_cmd\"");

unless (-s $jira_map) {
    $logger->logdie("The command \"$jira_pull_cmd\" ended without error codes, but the output file is empty.");
}

$logger->trace("Now Running the command: \"$glk_upd_cmd\"");
system($glk_upd_cmd) && $logger->logdie("Problem running the command: \"$glk_upd_cmd\"");
