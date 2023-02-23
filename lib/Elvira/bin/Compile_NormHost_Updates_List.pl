#!/usr/local/bin/perl

# File: Compile_NormHost_Updates_List.pl
# Author: 
# Created: March 22, 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# Compile_NormHost_Updates_List.pl Takes a tab-delimited file with database, Extent_id, 
# host, host_species, and host_connon_name  and it creates another tab-delimited file with 
# database, Extent_id, ExtentAttributeType (i.e. "normalized_host"), and value, that can be loaded
# with Update_ExtentAttribute.pl
# If for some of the samples no mapping is found, the list of the unmapped samples is appended to a 
# file called "UnNormalizable_Hosts.table" located in the same directory as the output file.

=head1 NAME
    
    Compile_NormHost_Updates_List.pl
    
=head1 USAGE

    Compile_NormHost_Updates_List.pl [-]-in[put_file] <host_report_file> [-]-out[put_file] <normailzed_host_attr_table> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-in[put_file] <host_report_file>

Tab-delimited file with database, Extent_id, host, host_species, and host_connon_name

=for Euclid:
    host_report_file.type: readable

=item [-]-out[put_file] <normailzed_host_attr_table>

Tab-delimited file with database, Extent_id, ExtentAttributeType ("normalized_host"), and value

=for Euclid:
    normailzed_host_attr_table.type: writeable

=back

=head1 OPTIONS

=over

=item [-]-host_map <host_map_file>

File with the mapping between known host, host_species, host_common_name values and normalized host

=for Euclid:
    host_map_file.type: readable
    
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

Compile_NormHost_Updates_List.pl Takes a tab-delimited file with database, Extent_id,  host, host_species, and host_connon_name  and it creates another tab-delimited file with  database, Extent_id, ExtentAttributeType (i.e. "normalized_host"), and value, that can be loaded with Update_ExtentAttribute.pl
If for some of the samples no mapping is found, the list of the unmapped samples is appended to a file called "UnNormalizable_Hosts.table" located in the same directory as the output file.

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
# use File::Path;
use JCVI::Logging::L4pTools;

our ($ARGV_input_file, $ARGV_output_file, $ARGV_host_map, $ARGV_debug, $ARGV_log_file);

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant HOST_MAP_FILE   => "$FindBin::Bin/Normalized_Host.map";
use constant UNMAPPABLE_FILE => 'UnNormalizable_Hosts.table';

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

my $host_map_file = defined($ARGV_host_map) ? $ARGV_host_map : HOST_MAP_FILE;
my $unmapped_file = dirname($ARGV_output_file) . '/' . UNMAPPABLE_FILE;

open(my $map, $host_map_file) || $logger->logdie("Impossible to open the file \"$host_map_file\" for reading.");
open(my $table, $ARGV_input_file) || $logger->logdie("Impossible to open the file \"$ARGV_input_file\" for reading.");
open(my $out, ">$ARGV_output_file") || $logger->logdie("Impossible to open the file \"$ARGV_output_file\" for writing.");

## Loading the map of normalized_hosts...
my %norm_host = ();

while (<$map>) {
    if (/^#/ || /^\s*$/) {
        next;
    }
    chomp();
    my ($name, $nh) = split /\t/;
    
    if (exists($norm_host{$name})) {
        $logger->warn("Mapping repetition in normalized host mapping file (\"$host_map_file\") \"$_\" vs. \"$nh\" - Ignoring the last value.");
    }
    else {
        $norm_host{$name} = $nh;
    }
}
close($map);

## Scanning through the samples requiring a normalizing host and making suer that there aren't incongruences among the various host attributes
my @unmapped = ();
my %normalized = ();

while (<$table>) {
    if (/^#/ || /^\s*$/) {
        next;
    }
    chomp();
    my ($db, $eid, $host, $species, $comm_name) = split /\t/;
    my $nh = undef;
    my $listed  = FALSE;
    my $trouble = FALSE;
    
    if (defined($host) && $host =~ /\S/) {
        if (exists($norm_host{$host})) {
            $normalized{$eid} = [$db, $norm_host{$host}];
                $logger->trace("Mapped on host: \"$_\"")
        }
        else {
            push(@unmapped, "\"$host\"\t$_");
            $listed = TRUE;
        }
    }
    if (defined($species) && $species =~ /\S/) {
        if (exists($norm_host{$species})) {
            if (exists($normalized{$eid})) {
                if ($normalized{$eid}[1] ne $norm_host{$species}) {
                    $trouble = TRUE;
                    $logger->error("Mapping mismatch between host and host_species: \"$_\" (\"$normalized{$eid}[1]\" vs. \"$norm_host{$species}\".");
                }
            }
            else {
                $normalized{$eid} = [$db, $norm_host{$species}];
                $logger->trace("Mapped on species: \"$_\"")
            }
        }
        elsif (! $listed) {
            push(@unmapped, "\"$species\"\t$_");
            $listed = TRUE;
        } 
    }
    if (defined($comm_name) && $comm_name =~ /\S/) {
        if (exists($norm_host{$comm_name})) {
            if (exists($normalized{$eid})) {
                if ($normalized{$eid}[1] ne $norm_host{$comm_name}) {
                    $trouble = TRUE;
                    $logger->error("Mapping mismatch between host or host_species and host_common_name: \"$_\" (\"$normalized{$eid}[1]\" vs. \"$norm_host{$comm_name}\".");
                }
            }
            else {
                $normalized{$eid} = [$db, $norm_host{$comm_name}];
                $logger->trace("Mapped on common_name: \"$_\"")
            }
        }
        elsif (! $listed) {
            push(@unmapped, "\"$comm_name\"\t$_");
            $listed = TRUE;
        } 
    }
    if ($trouble) {
        undef($normalized{$eid});
        delete($normalized{$eid});
    }
}
close($table);

if (scalar(@unmapped)) {
    open(my $unmap, ">$unmapped_file") || $logger->logdie("Problem opening the file \"$unmapped_file\" for appending.");
    
    print {$unmap} join("\n", @unmapped), "\n";
    close($unmap);
    print STDERR basename($0), " - Found unmappable flu hosts ($unmapped_file)\n";
}

## Writing the output file...

foreach my $eid (sort({$normalized{$a}[0] cmp $normalized{$b}[0]} keys(%normalized))) {
    my ($db, $val) = @{$normalized{$eid}};
    print {$out} "$db\t$eid\tnormalized_host\t$val\n";
} 
close($out);

