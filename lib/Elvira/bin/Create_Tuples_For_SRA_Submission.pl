#!/usr/local/bin/perl

# File: Create_Tuples_For_SRA_Submission.pl
# Author: pamedeo
# Created: June 29, 2018
#
# $Author:  $
# $Date: $
# $Revision: 2073 $
# $HeadURL:  $
#
# Copyright 2018, J. Craig Venter Institute
#
# Create_Tuples_For_SRA_Submission.pl Given a database and a collection, it writes tuple files for all the samples in need of SRA submission.
# It discriminates between samples still in production and samples that need to be restored from archive, and it partitions the output files
# (except for samples requiring BioSample ID) not to exceed MAX_SAMPLES_IN_FILE constant.

=head1 NAME
    
    Create_Tuples_For_SRA_Submission.pl
    
=head1 USAGE

    Create_Tuples_For_SRA_Submission.pl [-]-d[atabase] <annotation_database> [-]-c[ollection] <collection_id> [-]-o[utput] <root_for_output_names> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <annotation_database> | -D <annotation_database>

    VGD-schema annotation database

=for Euclid:
    annotation_database.type: string

=item [-]-c[ollection] <collection_id> 

    Name of the collection.
    
=for Euclid:
    collection_id.type: string
    
=item [-]-o[utput] <root_for_output_names>

    First part of the name for output tuple files.
    
=for Euclid:
    root_for_output_names.type: string

=back

=head1 OPTIONS

=over

=item [-]-only_published
    
    Do include only published samples (default: include anything after sequence validation step having a BioSample ID)

=item [-]-funding_source <funding_id>

    Restrict the search to samples having the given ID as funding source.
    
=for Euclid:
    funding_id.type: string

=item [-]-include_pending_sra

    By default this script ignores all the samples having the attribute "sra_study_id". By using this attribute, it will include the samples having "SRA Submission Pending" as value of that attribute.

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

Create_Tuples_For_SRA_Submission.pl Given a database and a collection, it writes tuple files for all the samples in need of SRA submission.
It discriminates between samples still in production and samples that need to be restored from archive, and it partitions the output files (except for samples requiring BioSample ID) not to exceed MAX_SAMPLES_IN_FILE constant.
=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
use lib ($FindBin::Bin, "$FindBin::Bin/../lib",  "$FindBin::Bin/../perllib");#, "/usr/local/devel/VIRIFX/software/Elvira/perllib/");
use Getopt::Euclid 0.2.4 (qw(:vars));
#use Data::Dumper;
use File::Basename;
use File::Path;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
#use ProcessingObjects::SafeIO;
use TIGR::GLKLib;

## Constants declaration
#
use constant SUCCESS => 1;
use constant FAILURE => 0;
use constant TRUE    => 1;
use constant FALSE   => 0;
use constant OS_SUCC => 0;
use constant OS_FAIL => 1;
use constant PUBLISHED => 'Sample Published';
use constant PROD_ROOT => '/usr/local/projdata/700010/projects/VHTNGS/sample_data_new/';
use constant ARCH_ROOT => '/usr/local/archdata/700010/projects/VHTNGS/sample_data_new/';
use constant MAX_SAMPLES_IN_FILE => 100;

our ($ARGV_database, $ARGV_collection, $ARGV_output, $ARGV_only_published, $ARGV_funding_source, $ARGV_include_pending_sra, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

my %okay_status = ('Submitted to GenBank' => undef,
                   'Sample Published'     => undef,
                   'Annotate'             => undef,
                   'Collaborator Review'  => undef);

## Initializing various stuff...

my $jdb = defined($ARGV_password_file) ? JCVI::DB_Connection::VGD_Connect->new(db => $ARGV_database, server => $ARGV_server, pass_file => $ARGV_password_file) :
                                         JCVI::DB_Connection::VGD_Connect->new(db => $ARGV_database, server => $ARGV_server);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

## Getting all the lots and the samples in the given collection...

my @prod_sam = ();
my @arch_sam = ();
my $coll_eid = $glk->getExtentByTypeRef('COLLECTION', $ARGV_collection);

unless (defined($coll_eid)) {
    $logger->logdie("Unable to find collection \"$ARGV_collection\" in database \"$ARGV_database\".")
}
my $r_lots = $glk->getExtentChildren($coll_eid);

unless (scalar(@{$r_lots})) {
    $logger->logdie("It looks like that collection \"$ARGV_collection\" in database \"$ARGV_database\" does not have any Lot.")
}
my $bad = 0;
my $not_good = 0;
my @no_bios = ();

foreach my $lot_eid (@{$r_lots}) {
    my $r_samples = $glk->getExtentChildren($lot_eid);
    
    foreach my $eid (@{$r_samples}) {
        my $r_attrs = $glk->getExtentAttributes($eid);
        my $r_info = $glk->getExtentInfo($eid);
        my $jstatus;
        my $bid = $r_info->{'ref'};
        
        if (exists($r_attrs->{jira_status})) {
            if ($ARGV_only_published && $r_attrs->{jira_status} ne PUBLISHED || !exists($okay_status{$r_attrs->{jira_status}})) {
                next
            }
        }
        else {
            $logger->error("Sample $bid (Extent_id $eid) does not have a jira_status attribute.");
            ++$bad;
            next
        }
        if (exists($r_attrs->{sra_study_id})) { 
            next unless $ARGV_include_pending_sra;
        }
        unless (exists($r_attrs->{funding_source})) {
            $logger->error("Sample $bid (Extent_id $eid) does not have a required funding_source attribute.");
            ++$bad;
            next
        }
        if ($ARGV_funding_source && $r_attrs->{funding_source} ne $ARGV_funding_source) {
            next
        } 
        unless (exists($r_attrs->{biosample_id})) {
            push(@no_bios, $bid);
            next
        }
        
        ## Checking if the sample is in production or in Archive.
        my $prod_path = PROD_ROOT . "$ARGV_database/$ARGV_collection/$bid";  
        my $arch_path = ARCH_ROOT . "$ARGV_database/$ARGV_collection/$bid";  
        
        if (-d $prod_path) {
            push(@prod_sam, $bid);
        }
        elsif (-d $arch_path) {
            push(@arch_sam, $bid);
        }
        else {
            ++$not_good;
            $logger->error("Unable to find sample $bid (Extent_id $eid) neither in production, nor in archive.");
        }
    }
}
if ($bad) {
    $logger->logdie("Found $bad serious errors with the samples of this collection. Fix the database first.")
}
my $tot_w = 0;

if (scalar(@arch_sam)) {
    if (scalar(@arch_sam) > MAX_SAMPLES_IN_FILE) {
        my $afh = undef;
        my $fino = 1;
        
        for (my $n = 0; $n < @arch_sam; ++$n) {
            unless ($n % MAX_SAMPLES_IN_FILE) {
                if (defined($afh)) {
                    close($afh);
                }
                my $arch_file = sprintf("%s.Archive.%02d.tuples", $ARGV_output, $fino++);
                open($afh, ">$arch_file") || $logger->logdie("Impossible to open the file $arch_file for writing.");
            }
            print {$afh} "$ARGV_database,$ARGV_collection,$arch_sam[$n]\n";
        }
        close($afh);
    }
    else {
        my $arch_tuples = "$ARGV_output.Archive.tuples";
        open(my $archfh, ">$arch_tuples") || $logger->logdie("Impossible to open the file $arch_tuples for writing.");
        
        foreach my $bid (@arch_sam) {
            print {$archfh} "$ARGV_database,$ARGV_collection,$bid\n";
        }
        close($archfh);
    }
    $tot_w += scalar(@arch_sam);
}
if (scalar(@prod_sam)) {
    if (scalar(@prod_sam) > MAX_SAMPLES_IN_FILE) {
        my $pfh = undef;
        my $fino = 1;
        
        for (my $n = 0; $n < @prod_sam; ++$n) {
            unless ($n % MAX_SAMPLES_IN_FILE) {
                if (defined($pfh)) {
                    close($pfh);
                }
                my $prod_file = sprintf("%s.Production.%02d.tuples", $ARGV_output, $fino++);
                open($pfh, ">$prod_file") || $logger->logdie("Impossible to open the file $prod_file for writing.");
            }
            print {$pfh} "$ARGV_database,$ARGV_collection,$prod_sam[$n]\n";
        }
        close($pfh);
    }
    else {
        my $prod_tuples = "$ARGV_output.Production.tuples";
        open(my $prodfh, ">$prod_tuples") || $logger->logdie("Impossible to open the file $prod_tuples for writing.");
        
        foreach my $bid (@prod_sam) {
            print {$prodfh} "$ARGV_database,$ARGV_collection,$bid\n";
        }
        close($prodfh);
    }
    $tot_w += scalar(@prod_sam);
}
if (scalar(@no_bios)) {
    my $no_bs_file = "$ARGV_output.Missing_BioSample.tuples";
    open(my $bsfh, ">$no_bs_file") || $logger->logdie("Impossible to open the file \"$no_bs_file\" for writing.");
    
    foreach my $bid (@no_bios) {
        print {$bsfh} "$ARGV_database,$ARGV_collection,$bid\n";
    }
    close($bsfh);
    print "\nFound ", scalar(@no_bios) , " samples missing BioSample ID. Saving their tuples in the following file:\n\n$no_bs_file";
}
unless ($tot_w) {
    $logger->logdie("No samples to submit found.");
}

my $msg = "Done. Written $tot_w tuples, (" . scalar(@arch_sam) . " of archived samples and " . scalar(@prod_sam) . " of samples still in production).";

if ($not_good) {
    $msg .= "\n\nImpossible to find sequencing files for $not_good samples."
}
print "\n\n$msg\n\n";
