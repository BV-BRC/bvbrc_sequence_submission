#!/usr/local/bin/perl

# File: dearchiveSamples.pl
# Author: 
# Created: October 31, 2016
#
# $Author: $
# $Date:$
# $Revision: $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# dearchiveSamples.pl takes a tuple file and, if the sample is in Archive, it expands the files back in the standard location

=head1 NAME
    
    dearchiveSamples.pl
    
=head1 USAGE

    dearchiveSamples.pl [-]-t[uple_file] <tuple_file> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-t[uple_file] <tuple_file>

    Comma-separated file with database, collection, and BAC ID

=for Euclid:
    tuple_file.type:   readable

=back

=head1 OPTIONS

=over

=item [-]-target_root_dir <target_root>

    Root portion of the path where the data of the archived samples should be extracted.
    (Default: /usr/local/projdata/700010/projects/VHTNGS/sample_data_new)

=for Euclid:
    target_root.type:    string
    target_root.default: "/usr/local/projdata/700010/projects/VHTNGS/sample_data_new"

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

dearchiveSamples.pl takes a tuple file and, if the sample is in Archive, it expands the files back in the standard location (or a different destination supplied with the --target_root_dir parameter).

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
use ProcessingObjects::SafeIO;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant ARCHIVE_ROOT => '/usr/local/archdata/700010/projects/VHTNGS/sample_data_new';
use constant DIR_PERMISSIONS => 0775;

our ($ARGV_tuple_file, $ARGV_target_root_dir, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");
$ARGV_target_root_dir =~ s/\/$//;

unless (-d $ARGV_target_root_dir) {
    mk_tree_safe($ARGV_target_root_dir, DIR_PERMISSIONS);
}
my @samples = ();
open(my $tf, $ARGV_tuple_file) || $logger->logdie("Impossible to open tuple file \"$ARGV_tuple_file\" for reading.");

while (<$tf>) {
    next if /^\s*$/ || /^#/;
    chomp();
    s/\s+//g;
    my ($db, $coll, $bid) = split /,/;
    
    unless (defined($db) && $db =~ /\S/ && defined($coll) && $coll =~ /\S/ && defined($bid) && $bid =~ /\S/) {
        $logger->error("Problems parsing file \"$ARGV_tuple_file\" - Unrecognized line: \n\"$_\" at line $. - Skipping it.");
        next;
    }
    push(@samples, [$db, $coll, $bid]);
}
close($tf);
## Processing the samples
my ($processed, $missing, $already_there, $errors) = (0) x 4;

foreach my $tuple (@samples) {
    my ($db, $coll, $bid) = @{$tuple};
    my $tarball = ARCHIVE_ROOT . "/$db/$coll/$bid/$bid.tgz";
    my $target_path = "$ARGV_target_root_dir/$db/$coll/$bid";
    
    if (-f $tarball) {
        if (-e $target_path) {
            if (-l $target_path) {
                unlink($target_path);
            }
            elsif (-d $target_path) {
                $logger->info("Sample $bid ($db, $coll) - In the destination area exists already a directory for this sample. Assuming it is still there and skipping it");
                ++$already_there;
                next
            }
            else {
                $logger->error("Sample $bid ($db, $coll) - In the file system exists already an entity called \"$target_path\" but it is neither a link nor a directory.");
                ++$errors;
                next
            }
        }
        ## Creating the target and extracting the tarball
        $logger->info("Creating the destination directory (\"$target_path\")...");
        mk_tree_safe($target_path, DIR_PERMISSIONS);
        my $cmd = "cd $target_path; tar -xzvf $tarball";
        
        if (system($cmd)) {
            $logger->error("Problems running the following command: \"$cmd\" (\"$!\" - \"$?\").");
            ++$errors;
        }
        else {
            ++$processed;
            if ($ARGV_target_root_dir =~ /scratch/) { ## If extracting on scratch - we better touch all files and dirs
                my $cmd = "find $target_path -exec touch {} \\;";
                system($cmd) && $logger->warn("problems with touching files and directrories. CMD: \"$cmd\" (\"$!\", \"$?\").");
            }
        }
    }
    else {
        if (-d $target_path) {
            $logger->info("Sample $bid ($db, $coll) is still in production and it was never archived properly.");
            ++$already_there;
        }
        else {
            $logger->error("Sample $bid ($db, $coll) - Impossible to fine neither a tarball nor a directory in production.");
            ++$missing;
        }
        next
    }
}
#my ($processed, $missing, $already_there, $errors) = (0) x 4;
my $tot = scalar(@samples);
my $msg = "\n\nDone.\n\n";

if ($tot == $processed) {
    $msg .= "Successfully extracted all $tot samples.\n";
}
else {
    $msg .= "Extracted $processed samples out of $tot.\n";
    $msg .= "$already_there samples had already a directory in the destination area (\"$ARGV_target_root_dir\").\n" if $already_there;
    $msg .= "Unable to find the tarball for $missing samples.\n" if $missing;
    $msg .= "Encountered errors on $errors samples.\n" if $errors;
}
print "$msg\n";