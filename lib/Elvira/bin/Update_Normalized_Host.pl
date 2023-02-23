#!/usr/local/bin/perl

# File: Update_Normalized_Host.pl
# Author: 
# Created: March 30, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL: $
#
# Copyright 2016, J. Craig Venter Institute
#
# Update_Normalized_Host.pl Is a wrapper script intended for running as cronjob. 
# The main steps of this pipeline are:
# 
# - Compile a list of samples (and their attributes) with host, host_species, and/or host_common_name,
#   but without normalized_host.
# - Parse such list, using the known mapping between the host attributes and normalize_host, 
#   verifying the congruence among the attributes and compiling a list of samples that cannot be resolved 
#   using the existing mapping.
# - Load the new found normalized_host into the databases.


=head1 NAME
    
    Update_Normalized_Host.pl
    
=head1 USAGE

    Update_Normalized_Host.pl [-]-work_dir <temp_file_location> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-work_dir <temp_file_location>

Directory where to write the temporary files.

=for Euclid:
    temp_file_location.type:   string

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

Update_Normalized_Host.pl Is a wrapper script intended for running as cronjob. The main steps of this pipeline are:

- Compile a list of samples (and their attributes) with host, host_species, and/or host_common_name, but without normalized_host.
- Parse such list, using the known mapping between the host attributes and normalize_host, verifying the congruence among the attributes and compiling a list of samples that cannot be resolved using the existing mapping.
- Load the new found normalized_host into the databases.

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
use DateTime;

use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant GET_SAMPLE_LIST_PRG    => "$FindBin::Bin/Find_Samples_Without_Normalized_Host.pl";
use constant PARSE_SAMPLE_LIST_PRG  => "$FindBin::Bin/Compile_NormHost_Updates_List.pl";
use constant LOAD_NORM_HOST_PRG     => "$FindBin::Bin/Update_ExtentAttribute.pl";

our ($ARGV_work_dir, $ARGV_dont_load, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $dt = DateTime->now();
my $unistamp_string = $dt->ymd() . '_' . $dt->hms('.')  . '_' . $$;
$ARGV_work_dir =~ s/\/$//;

unless (defined($ARGV_log_file)) {
    $ARGV_log_file = "$ARGV_work_dir/Update_Normalized_Host.$unistamp_string.log"
}

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

my $files_uniquifier = "$ARGV_work_dir/Update_Normalized_Host"  .'_'. $unistamp_string;

my $sample_list     = "$files_uniquifier.sample_list";
my $norm_host_attrs = "$files_uniquifier.normalized_host";

my $get_samples_cmd = GET_SAMPLE_LIST_PRG . " --output_file $sample_list --server $ARGV_server";
my $parse_hosts_cmd = PARSE_SAMPLE_LIST_PRG . " --input_file $sample_list --output_file $norm_host_attrs";
my $load_norm_h_cmd = LOAD_NORM_HOST_PRG . " --input_file $norm_host_attrs --server $ARGV_server --ignore_empty_files";

## Adding optional attributes to child commands...

if ($ARGV_dont_load) {
    $load_norm_h_cmd .= ' --dont_load';
}
if ($ARGV_password_file) {
    $get_samples_cmd .= " --password_file $ARGV_password_file";
    $load_norm_h_cmd .= " --password_file $ARGV_password_file";
}
if ($ARGV_debug ne 'NOT_SET') {
    $get_samples_cmd .= " --debug $ARGV_debug";
    $parse_hosts_cmd .= " --debug $ARGV_debug";
    $load_norm_h_cmd .= " --debug $ARGV_debug";
}
if ($ARGV_log_file) {
    $get_samples_cmd .= " --log_file $ARGV_work_dir/Find_Samples_Without_Normalized_Host.$unistamp_string.log";
    $parse_hosts_cmd .= " --log_file $ARGV_work_dir/Compile_NormHost_Updates_List.$unistamp_string.log";
    $load_norm_h_cmd .= " --log_file $ARGV_work_dir/Update_ExtentAttribute.$unistamp_string.log";
}
## Now, actually running the commands, one after the other.

$logger->info("Now running the following command:\n\"$get_samples_cmd\"");

if (system($get_samples_cmd)) {
    $logger->logdie("Problems extracting the samples in need of a normalized_host");
}
$logger->info("Now running the following command:\n\"$parse_hosts_cmd\"");

if (system($parse_hosts_cmd)) {
    $logger->logdie("Problems parsing the table with all the samples in need of a normalized_host");
}
$logger->info("Now running the following command:\n\"$load_norm_h_cmd\"");

if (system($load_norm_h_cmd)) {
    $logger->logdie("Problems loading the normalized_host attributes in the database");
}
$logger->info("The program has completed the job successfully");