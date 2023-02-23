#!/usr/local/bin/perl

# File: Get_All_Samples_w_Jira_ID.pl
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
# Get_All_Samples_w_Jira_ID.pl Queries GLK for all samples having a 'jira_id' attribute, 
# it generates a list of jira_ids and a table with database, Extent_id, jira_id, and jira_status, 
# if present.

=head1 NAME
    
    Get_All_Samples_w_Jira_ID.pl
    
=head1 USAGE

    Get_All_Samples_w_Jira_ID.pl [-]-jira_id_list <Jira_SampleTracking_IDs_list> [-]-jira_attr_table <table_of_glk_samples> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-jira_id_list <Jira_SampleTracking_IDs_list> 

Full path of the output file containing all the jira_id values, one per line. 

=for Euclid:
    Jira_SampleTracking_IDs_list.type:  writeable

=item [-]-jira_attr_table <table_of_glk_samples> 

Full path of the output file containing the mapping between jira SampleTracking IDs, annotation database, Extent_id, and GLK's jira_status 

=for Euclid:
    table_of_glk_samples.type:  writeable

=back

=head1 OPTIONS

=over

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

Get_All_Samples_w_Jira_ID.pl Queries GLK for all samples having a 'jira_id' attribute, generates a list of jira_ids and a table with database, Extent_id, jira_id, and jira_status, if present.



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
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use TIGR::GLKLib;
## Constants declaration
#
use constant START_DB          => 'giv';
use constant ELVIRA_PROP_FILE  => '/usr/local/devel/VIRIFX/software/Elvira.Java.props';

our ($ARGV_jira_id_list, $ARGV_jira_attr_table, $ARGV_server, $ARGV_debug, $ARGV_log_file);

my %ignore_db = (genome_viral => undef,
                 givtest      => undef);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Getting username and pass from Elvira props file

my $user;
my $pass;

if (open(my $props, ELVIRA_PROP_FILE)) {
    while (<$props>) {
        if (/^projectdb.default.user\s+(\S+)\s*$/) {
            $user = $1;
        }
        elsif (/^projectdb.default.pass\s+(\S+)\s*$/) {
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

my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => START_DB, server => $ARGV_server, user => $user, pass => $pass);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

## Getting all the samples with Jira ID and creating the list...

my $all_dbs = $glk->getAllVgdDbs();
my %sample = ();
my %duplo = ();


open(my $st_list,  ">$ARGV_jira_id_list") || $logger->logdie ("Impossible to open the file \"$ARGV_jira_id_list\" for writing.");
open(my $st_table, ">$ARGV_jira_attr_table") || $logger->logdie ("Impossible to open the file \"$ARGV_jira_attr_table\" for writing.");

foreach my $db (@{$all_dbs}) {
    if (exists($ignore_db{$db})) {
        next;
    }
    $glk->changeDb($db);
    my $r_samples = $glk->getExtentsByType('SAMPLE');
    
    foreach my $eid (@{$r_samples}) {
        my $jira_id = undef;
        my $jira_status = '';
        
        if ($glk->hasExtentAttribute($eid, 'jira_id')) {
            $jira_id = $glk->getExtentAttribute($eid, 'jira_id');
        }
        else {
            next;
        } 
        if ($glk->hasExtentAttribute($eid, 'jira_status')) {
            $jira_status = $glk->getExtentAttribute($eid, 'jira_status');
        }
        if (exists($sample{$jira_id})) {
            push(@{$duplo{$jira_id}}, [$db, $eid, $jira_id, $jira_status]);
        }
        else {
            $sample{$jira_id} = [$db, $eid, $jira_id, $jira_status];
        }
    }
}    
## Removing duplicates

foreach my $jira_id (keys(%duplo)) {
    push(@{$duplo{$jira_id}}, $sample{$jira_id});
    my @not_depr = ();
    
    foreach my $data (@{$duplo{$jira_id}}) {
        my ($db, $eid) = @{$data};
        $glk->changeDb($db);
        my $deprecated = $glk->isDeprecated($eid);
        
        unless ($deprecated) {
            push(@not_depr, $eid);
        }
    }
    if (scalar(@not_depr) > 1) {
        $logger->error("There are multiple not-deprecated samples associated to jira_id \"$jira_id\" - Skipping this jira-id.");
        undef($sample{$jira_id});
        delete($sample{$jira_id});
    }
    elsif (scalar(@not_depr) == 0) {
        $logger->error("There are multiple samples associated to jira_id \"$jira_id\" and they are all deprecated. - Skipping this jira-id.");
        undef($sample{$jira_id});
        delete($sample{$jira_id});
    }
    else { ## Only one not deprecated...
        foreach my $data (@{$duplo{$jira_id}}) {
            my ($db, $eid) = @{$data}[0,1];
            
            if ($eid == $not_depr[0]) {
                $sample{$jira_id} = $data;
            }
            else {
                $logger->warn("Sample $eid (database $db) is deprecated and has the same jira_id of a non-deprecated sample. Please, remove the jira_id (and jira_status) attribute from this sample.");
            }
        }
    }
}

## Writing the output files

foreach my $jira_id (sort(keys(%sample))) {
    print {$st_list} "$jira_id\n";
    print {$st_table}  join("\t", @{$sample{$jira_id}}), "\n";
}
close($st_list);
close($st_table);
