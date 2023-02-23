#!/usr/local/bin/perl

# File: Extract_Low_Coverage_Areas.pl
# Author: 
# Created: July 15, 2010
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2018, J. Craig Venter Institute
#
# Extract_Low_Coverage_Areas.pl parses AutoTasker2 output to find areas with low coverage and 
# generates a fasta file containing all such regions plus a padding of X nucleotides as defined 
# by the --padding option.

=head1 NAME
    
    Extract_Low_Coverage_Areas.pl
    
=head1 USAGE

    Extract_Low_Coverage_Areas.pl [-]-d[atabase] <annotation_database> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-a[utotasker_file] <autotasker_txt>

    autoTasker2 report file (generally named "autoTasker2.*.ace.N.txt") 

=for Euclid:
    autotasker_txt.type: readable

=item [-]-f[asta_file] <assembly_fasta_file>

    Fasta file containing all the assembled sequences referred to in the autoTasker2 report
    
=for Euclid:
    assembly_fasta_file.type: readable

=item [-]-o[utput_fasta] <fasta_w_low_coverage_regions>

    Output fasta file with all the regions with low/unilateral coverage.

=for Euclid:
    fasta_w_low_coverage_regions.type: writeable
    
=back

=head1 OPTIONS

=over

=item [-]-padding <padding_size>

    Number of nucleotides to include before and after the low-coverage areas (default: 50)..
    Note: this option determins the behavior of the script in merging together multiple low coverage areas, if they are closer than 2x padding

=for Euclid:
    padding_size.type:    int >= 0
    padding_size.default: 50

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

Extract_Low_Coverage_Areas.pl parses AutoTasker2 output to find areas with low coverage and generates a fasta file containing all such regions plus a padding of X nucleotides as defined by the --padding option.

=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
use lib ("$FindBin::Bin/../lib");#, "/usr/local/devel/VIRIFX/software/Elvira/perllib/");
use Getopt::Euclid 0.2.4 (qw(:vars));
#use Data::Dumper;
use File::Basename;
use File::Path;
use JCVI::Logging::L4pTools;

## Constants declaration
#
use constant SUCCESS => 1;
use constant FAILURE => 0;
use constant TRUE    => 1;
use constant FALSE   => 0;
use constant OS_SUCC => 0;
use constant OS_FAIL => 1;

my %loco_tag = (seqcoverage    => undef,
                uniDirCoverage => undef);

## Regex to remove (most) ANSI Escape codes: s/\x1b\[[0-9;]*m//g

our ($ARGV_autotasker_file, $ARGV_fasta_file, $ARGV_output_fasta, $ARGV_padding, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

my %locov = ();

open(my $at, $ARGV_autotasker_file) || $logger->logdie("Impossible to open the autoTasker report file \$ARGV_autotasker_file\" for reading.");
open(my $fsa, $ARGV_fasta_file) || $logger->logdie("Impossible to open the assembly fasta file \$ARGV_fasta_file\" for reading.");
open(my $out, ">$ARGV_output_fasta") || $logger->logdie("Impossible to open the output low-coverage regions fasta file \$ARGV_output_fasta\" for writing.");

## Parsing the autoTasker2 report searching for low-coverage/unidirectional coverage areas
my $bad = 0;

while (<$at>) {
    s/\x1b\[[0-9;]*m//g;
    chomp();
    next unless s/^\t//; ## we are interested only in the individual lines under each segment.
    my ($region, $issue) = (split /\s*:\s*/)[0,1];
    next unless exists($loco_tag{$issue});
    
    if ($region =~ /^\[\s*\d+\s*-\s*\d+\s*\]\s+(\S+)\s+\[\s*(\d+)\s*-\s*(\d+)\s*\]$/) {
        my ($seg, $start, $end) = ($1, $2, $3);
        push(@{$locov{$seg}}, [$start, $end]);
    }
    else {
        ++$bad;
        $logger->error("File \"$ARGV_autotasker_file\" line $. - Unable to parse the following line: \"$_\"");
    }
}
close($at);

if ($bad) {
    $logger->logdie("Too many problems parsing autoTasker2 report.")
}

## Consolidating the regions with bad coverage

foreach my $seg (keys(%locov)) {
    my %remove = ();
    @{$locov{$seg}} = sort({$a->[0] <=> $b->[0]} @{$locov{$seg}});

    for (my $n = 0; $n < $#{$locov{$seg}}; ++$n) {
        next if exists($remove{$n});
        
        for (my $i = $n + 1; $i < @{$locov{$seg}}; ++$i) {
            next if exists($remove{$i});
            
            if (abs($locov{$seg}[$i][0] - $locov{$seg}[$n][1]) <= 2 * $ARGV_padding) {
                $locov{$seg}[$n][1] = $locov{$seg}[$i][1];
                undef($remove{$i});
            }
            else {
                last
            }
        }
    }
    foreach my $i (sort({$b <=> $a} keys(%remove))) {
        splice(@{$locov{$seg}}, $i, 1);
    }
}

## Parsing the input fasta file and creating the output file
my %matched = ();
my $written = 0;
{
    local $/ = "\n>";
    
    while (<$fsa>) {
        chomp();
        s/^>//;
        s/[\r\n]+$//;
        my ($hdr, @tmp) = split /[\r\n]+/;
        my ($seg, @others) = (split(/\s+/, $hdr));
        
        if (exists($locov{$seg})) {
            undef($matched{$seg});
            my $seq = join('', @tmp);
            
            foreach my $chunk (@{$locov{$seg}}) {
               my ($start, $end) = @{$chunk};
               my $seq_ln = length($seq);
               $start = $start < $ARGV_padding ? 0 : $start - $ARGV_padding;
               $end += $ARGV_padding;
               $end = $seq_ln if $end > $seq_ln;
               
               my $frg = substr($seq, $start, $end - $start);
               $frg =~ s/(.{1,60})/$1\n/g;
               ++$start;
               my $header = "$seg.$start..$end " . join(' ', @others);
               print {$out} ">$header\n$frg"; 
               ++$written;
            }
        }
    }
    close($fsa);
    close($out);
}
unless (scalar(keys(%locov)) == scalar(keys(%matched))) {
    my @missing = ();
    
    foreach my $seg (keys(%locov)) {
        push(@missing, $seg) unless exists($matched{$seg});
    }
    $logger->logdie("Unable to find the following fragment in the supplied fasta file (\"$ARGV_fasta_file\"):\n\"" . join('", "', @missing) . '"')
}
my $what = $written == 1 ? 'sequence' : 'sequences';
print "\n\nDone. Written $written $what into output file \"$ARGV_output_fasta\".\n\n";