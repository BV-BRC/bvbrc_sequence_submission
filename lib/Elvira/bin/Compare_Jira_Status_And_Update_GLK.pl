#!/usr/local/bin/perl

# File: Compare_Jira_Status_And_Update_GLK.pl
# Author: 
# Created: February 19, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# Compare_Jira_Status_And_Update_GLK.pl Takes a tab-separated file with annotation database, 
# Extent_id, jira_id, and jira_status and a comma-separated file with jira_id and jira_status, 
# compares the records in both and updates the GLK records needing any update.
 

=head1 NAME
    
    Compare_Jira_Status_And_Update_GLK.pl
    
=head1 USAGE

    Compare_Jira_Status_And_Update_GLK.pl [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-jira_map <Jira_SampleTracking_IDs_Status_map> 

Full path to the comma-separated file of jira_id and jira_status values extracted from Jira SampleTracking. 

=for Euclid:
    Jira_SampleTracking_IDs_Status_map.type:  readable

=item [-]-jira_attr_table <table_of_glk_samples> 

Full path of the tab-delimited table with the mapping between jira SampleTracking IDs, annotation database, Extent_id, and GLK's jira_status from GLK 

=for Euclid:
    table_of_glk_samples.type:  readable

=item [-]-summary <summary_file> 

Full path to the output file summarizing the changes to thedatabase  

=for Euclid:
    summary_file.type:  writeable

=back

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

Compare_Jira_Status_And_Update_GLK.pl Takes a tab-separated file with annotation database, Extent_id, jira_id, and jira_status and a comma-separated file with jira_id and jira_status, compares the records in both and updates the GLK records needing any update.



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

use File::Path;
use File::Basename;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use TIGR::GLKLib;
## Constants declaration
#
use constant ELVIRA_PROP_FILE  => '/usr/local/devel/VIRIFX/software/Elvira.Java.props';
use constant TEMP_FILES_DIR    => '/usr/local/scratch/VIRAL/ST';
use constant START_DB          => 'giv';
use constant JIRA_DEPRECATED   => 'Deprecated';

our ($ARGV_jira_map, $ARGV_jira_attr_table, $ARGV_summary, $ARGV_dont_load, $ARGV_server, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Getting username and pass from Elvira props file

my $user;
my $pass;

if (open(my $props, ELVIRA_PROP_FILE)) {
    while (<$props>) {
        if (/^glk\.admin\.user\s+(\S+)\s*$/) {
            $user = $1;
        }
        elsif (/^glk\.admin\.pass\s+(\S+)\s*$/) {
            $pass = $1;
        }
    }
    close($props);
    
    unless (defined($user) && defined($pass)) {
        $logger->logdie("Impossible to find the username and password to connect to the database.");
    }
}
else {
    $logger->logdie("Impossible to oper the file \"" . ELVIRA_PROP_FILE . "\" for reading.");
}

## Initializing various stuff...
my ($jdb, $glk);

unless ($ARGV_dont_load) {
    $jdb = JCVI::DB_Connection::VGD_Connect->new(db => START_DB, server => $ARGV_server, user => $user, pass => $pass);
    $glk  = TIGR::GLKLib->new($jdb->dbh());
}

## Loading in memory the rasults and all the mapping...

open(my $jira_map,  $ARGV_jira_map)        || $logger->logdie ("Impossible to open the file \"$ARGV_jira_map\" for reading.");
open(my $glk_table, $ARGV_jira_attr_table) || $logger->logdie ("Impossible to open the file \"$ARGV_jira_attr_table\" for reading.");
open(my $summary,   ">$ARGV_summary")      || $logger->logdie ("Impossible to open the file \"$ARGV_summary\" for writing.");

my %status = ();
my %update = ();
my %insert = ();
my %all_used_dbs = ();

while (<$jira_map>) {
    chomp();
    my ($jira_id, $jira_status) = split /,/;
    $status{$jira_id} = $jira_status;
}
close($jira_map);
my $good = 1;

while (<$glk_table>) {
    chomp();
    my ($db, $eid, $jira_id, $jira_status) = split /\t/;
    
    unless (exists($status{$jira_id})) {
        $logger->error("Missing information from Jira for sample: $_");
        $good = 0;
        next;
    }
    if (defined($jira_status) && $jira_status =~ /\S/) {
        if ($jira_status ne $status{$jira_id}) {
            $update{$db}{$eid} = [$status{$jira_id}, $jira_status];
            undef($all_used_dbs{$db});
        }
    }
    else {
        $insert{$db}{$eid} = $status{$jira_id};
        undef($all_used_dbs{$db});
    }
}
close($glk_table);

unless ($good) {
    $logger->logdie("It appears that the input files have too many errors. Impossible to continue.");
}

## parsing the output and loading into the database...

## loading/Simulating the load of all the updates

foreach my $db (keys(%all_used_dbs)) {
    unless ($ARGV_dont_load) {
        if ($glk->isVgdDb($db)) {
            $glk->changeDb($db);
        }
        else { ## This is a script run in a cronjob and it should simply ignore JRA samples in retired VGD databases, without sending any warning.
            next
        }
    }
    if (exists($update{$db})) {
        while (my ($eid, $data) = each(%{$update{$db}})) {
            my ($new_value, $old_value) = @{$data};
            
            if ($ARGV_dont_load) {
                $logger->info("Database \"$db\" - Simulating the update of jira_status for sample $eid from \"$old_value\" to \"$new_value\".");
            }
            else {
                $glk->setExtentAttribute($eid, 'jira_status', $new_value);
                
                if ($new_value eq JIRA_DEPRECATED) {
                    unless ($glk->isDeprecated($eid)){
                        $glk->setDeprecated($eid);
                    }
                }
                elsif ($old_value eq JIRA_DEPRECATED) {
                    $glk->unsetDeprecated($eid);
                }
            }
            print {$summary} "$db\t$eid\tjira_status\t'$old_value' -> '$new_value'\n"
        }
    }
    if (exists($insert{$db})) {
        while (my ($eid, $value) = each(%{$insert{$db}})) {
            if ($ARGV_dont_load) {
                $logger->info("Database \"$db\" - Simulating the insertion of jira_status attribute for sample $eid (\"$value\").");
            }
            else {
                $glk->addExtentAttribute($eid, 'jira_status', $value);
                
                if ($glk->isDeprecated($eid) && $value ne JIRA_DEPRECATED) {
                    $glk->unsetDeprecated($eid);
                }
                elsif ($value eq JIRA_DEPRECATED && !$glk->isDeprecated($eid)) {
                    $glk->setDeprecated($eid);
                }
            }
            print {$summary} "$db\t$eid\tjira_status\tNULL -> '$value'\n"
        }
    }
}
close($summary);