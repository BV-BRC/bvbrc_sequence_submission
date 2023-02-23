#!/usr/local/bin/perl

# File: template.pl
# Author: 
# Created: March 11, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# Find_Samples_Without_Normalized_Host.pl - Queries all the flu databases to find all 
# the non-deprecated samples in need of a normalized_host attribute 

=head1 NAME
    
    Find_Samples_Without_Normalized_Host.pl
    
=head1 USAGE

    Find_Samples_Without_Normalized_Host.pl [-]-out[put_file] <table_with_id_and_hosts> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-out[put_file] <table_with_id_and_hosts>

Tab-delimited file with database_name, Extent_id, host, host_species, and host_common_name

=for Euclid:
    table_with_id_and_hosts.type:   writeable

=back

=head1 OPTIONS

=over

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

Queries all the flu databases to find all the non-deprecated samples in need of a normalized_host attribute

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
## Commonly used modules (remove whatever doesn't apply):
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
use constant ELVIRA_PROP_FILE  => '/usr/local/devel/VIRIFX/software/Elvira.Java.props';

my @flu_db = (qw(giv giv2 giv3 piv swiv));
my $db = $flu_db[0];

our ($ARGV_output_file, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Initializing various stuff...

my $jdb = defined($ARGV_password_file) ? JCVI::DB_Connection::VGD_Connect->new(db => $db, server => $ARGV_server, pass_file => $ARGV_password_file) :
                                         JCVI::DB_Connection::VGD_Connect->new(db => $db, server => $ARGV_server);

my $glk  = TIGR::GLKLib->new($jdb->dbh());
$glk->setLogger($logger);
$glk->setWrittenOnlyWarnings(TRUE);
$glk->setAttrValValidation(FALSE);

open(my $out, ">$ARGV_output_file") || $logger->logdie("Impossible to open the output file \"$ARGV_output_file\" for writing.");

for (my $n = 0; $n < @flu_db; ++$n) {
    if ($n > 0) {
        $db = $flu_db[$n];
        
        if ($glk->isVgdDb($db)) {
            $glk->changeDb($db);
        }
        else {
            $logger->warn("Database \"$db\" is no longer active. Please, remove it from the hard-coded list.");
            next
        }
    }
    my $r_samples = $glk->getExtentsByType('SAMPLE');
    
    foreach my $eid (@{$r_samples}) {
        if ($glk->isDeprecated($eid)) {
            next;
        }
        my $r_attrs = $glk->getExtentAttributes($eid);
        
        if (exists($r_attrs->{normalized_host}) && $r_attrs->{normalized_host} =~ /\S/) {
            next;
        }
        my $host = exists($r_attrs->{host}) && $r_attrs->{host} =~ /\S/ ? $r_attrs->{host} : '';
        my $species = exists($r_attrs->{host_species}) && $r_attrs->{host_species} =~ /\S/ ? $r_attrs->{host_species} : '';
        my $comm_name = exists($r_attrs->{host_common_name}) && $r_attrs->{host_common_name} =~ /\S/ ? $r_attrs->{host_common_name} : '';
        my $host_str = "$host\t$species\t$comm_name";
        
        if ($host_str =~ /\S/) {
            print {$out} "$db\t$eid\t$host_str\n";
        }
    }
}
close($out);