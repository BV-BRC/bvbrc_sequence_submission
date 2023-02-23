#!/usr/local/bin/perl

# File: Prepare_SRA_Submission_Template.pl
# Author: Paolo Amedeo
# Created: May 17, 2017
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL: $
#
# Copyright 2017, J. Craig Venter Institute
#
# Prepare_SRA_Submission_Template.pl creates the tab-delimited template 
# to be used by /usr/local/devel/BCIS/NCBI_UI-less_XML_submission/make_UIL_SRA.pl
# The default behavior is for the script to act on all the published samples in
# the given database not having yet any of the SRA-related attributes.
# In the case a sample has been archived already, the script will throw a warning
# and will ignore it.
# The behavior of the script could be restricted by specifying a collection ID, 
# batch_id, a single  sample ID (BAC ID), or a file with one sample ID per line 
# and will act only on the samples so identified.
# It is also possible to force the script to act on samples that have jira_status value
# other than "Sample Published" by specifying option --force_unpublished in combination 
# with either a single BAC ID or a file. 

=head1 NAME
    
    Prepare_SRA_Submission_Template.pl
    
=head1 USAGE

    Prepare_SRA_Submission_Template.pl [-]-d[atabase] <annotation_database> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <annotation_database> | -D <annotation_database>

    VGD-schema annotation database

=for Euclid:
    annotation_database.type:   string
    
=item [-]-template <output_CSV_template>

    Name for the output CSV file (to be used as input file for /usr/local/devel/BCIS/NCBI_UI-less_XML_submission/make_UIL_SRA.pl)
    
=for Euclid:
    output_CSV_template.type: writeable
    
=item [-]-f[irst_n[ame]] <submitter_first_name>

First name of the person submitting these samples to SRA

=for Euclid:
    submitter_first_name.type: string
    
=item [-]-l[ast_n[ame]] <submitter_last_name>

Last name of the person submitting these samples to SRA

=for Euclid:
    submitter_last_name.type: string

=back

=head1 OPTIONS

=over

=item [-]-batch_id <batch_id>

    Batch ID corresponding to the sample to be submitted.
    Note: You need to specify only one among --batch_id --bac_id, --collection_id, --tuple_list, or --bac_id_file options. These options are mutually exclusive.

=for Euclid:
    batch_id.type: string

=item [-]-bac_id <BAC_ID>

    BAC ID (=Sample ID) corresponding to the sample to be submitted.
    Note: You need to specify only one among --batch_id --bac_id, --collection_id, --tuple_list, or --bac_id_file options. These options are mutually exclusive.

=for Euclid:
    BAC_ID.type: int > 0

=item [-]-collection_id <collection>

    Collection ID.
    Note: You need to specify only one among --batch_id --bac_id, --collection_id, --tuple_list, or --bac_id_file options. These options are mutually exclusive.

=for Euclid:
    collection.type:  string

=item [-]-t[uple_file] <tuple_list>

    File with tuples ("database,collection,bac_id").
    Note: You need to specify only one among --batch_id --bac_id, --collection_id, --tuple_list, or --bac_id_file options. These options are mutually exclusive.

=for Euclid:
    tuple_list.type:  readable

=item [-]-b[ac_id_file] <bac_id_list>

    File with a list of BAC IDs, one per line.
    Note: You need to specify only one among --batch_id --bac_id, --collection_id, --tuple_list, or --bac_id_file options. These options are mutually exclusive.

=for Euclid:
    bac_id_list.type:  readable

=item [-]-sample_root <sample_root_dir>

    Root directory where the sequence data is stored under $db/$collection/$bac_id subdirectories (default: /usr/local/projdata/700010/projects/VHTNGS/sample_data_new) 
    
=for Euclid:
    sample_root_dir.type:    string
    sample_root_dir.default: "/usr/local/projdata/700010/projects/VHTNGS/sample_data_new"

=item [-]-output_file_path <out_seq_files_dir>

    Directory where to write the files to submit to SRA (default: /usr/local/scratch/VIRAL/SRA_Submissions) 
    
=for Euclid:
    out_seq_files_dir.type:    string
    out_seq_files_dir.default: "/usr/local/scratch/VIRAL/SRA_Submissions"

=item [-]-release_date <date_of_release>

    Date when the traces should be released to the public. Date format: "yyyy-mm-dd"
    Note: If the dataset contains clinical samples or viruses grown on human cell cultures and require therefore filtering out human reads, the date of release should be set at least one week in the future. 
   
=for Euclid:
    date_of_release.type: string

=item [-]-funding_source <funding_src_id>

    Process only samples that have the specified funding source (e.g. GCID GSC MSC).

=for Euclid:
     funding_src_id.type: string   

=item [-]-dont_update_status

    By default, the script updates the database inserting/updating the attribute "sra_study_id" to "SRA Submission Pending".
    If you specify this option, no update is made in the database.

=item [-]-include_pending_sra

    By default this script ignores all the samples having the attribute "sra_study_id". By using this attribute, it will include the samples having "SRA Submission Pending" as value of that attribute.

=item [-]-force_unpublished

    It is used only in conjunction with one among --batch_id --bac_id, --collection_id or --input_file options and it results in including all the samples in the list regardless of the value of jira_status attribute.

=item [-]-library_strategy <lib_strategy>

    Value for the "library_strategy" column of the template (default: "WGS").

=for Euclid:
    lib_strategy.type:    string
    lib_strategy.default: "WGS"

=item [-]-library_source <lib_src>

    Value for the "library_source" column of the template (default: "VIRAL_RNA").

=for Euclid:
    lib_src.type:    string
    lib_src.default: "VIRAL_RNA"

=item [-]-library_selection <lib_select>

    Value for the "library_selection" column of the template (default: "RANDOM_PCR").

=for Euclid:
    lib_select.type:    string
    lib_select.default: "RANDOM_PCR"

=item [-]-simulate_only

    Simulate only the splitting of fastq files and the loading (of SRA status in "sra_study_id") but do not insert anything into the database

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

Prepare_SRA_Submission_Template.pl creates the tab-delimited template to be used by /usr/local/devel/BCIS/NCBI_UI-less_XML_submission/make_UIL_SRA.pl
The default behavior is for the script to act on all the published samples in the given database not having yet any of the SRA-related attributes.
In the case a sample has been archived already, the script will throw a warning and will ignore it.
By default, the script inserts the "sra_study_id" attribute with value "SRA Submission Pending" on each sample included in the output template. It is possible to change this behavior by useing the --dont_update_status attribute.
it is possible to extend the reach of the script by using --include_pending_sra (it doesn't skip samples having "sra_study_id" set to "SRA Submission Pending") and/or --force_unpublished (it doesn't restrict the script to act only on samples with JIRA Status to "Sample Published").
The behavior of the script could be restricted by specifying a collection ID,  batch_id, a single  sample ID (BAC ID), or a file with one sample ID per line  and will act only on the samples so identified.


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
use Data::Dumper;
use File::Basename;
use File::Path;
use File::Copy;
use ProcessingObjects::SafeIO;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use TIGR::GLKLib;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant RELEVANT    =>1;
use constant IRRELEVANT => 0;
use constant CONTACT_EMAIL     => 'Gbgenomics@jcvi.org';
#use constant FASTQ_SPLITTER    => '/usr/local/devel/VIRIFX/software/Elvira/bin/splitByReadDirectionFastq';
use constant FASTQ_SPLITTER    => '/usr/local/devel/VIRIFX/software/Staging/Elvira/bin/splitByReadDirectionFastq'; ## Pointing to Staging
use constant SPLITTER_SUCC_EXT => 'splitbydirection-completed';
use constant SRA_STUDY_ATTR    => 'sra_study_id';
use constant JIRA_STATUS_ATTR  => 'jira_status';
use constant BIOSAMPLE_ATTR    => 'biosample_id';
use constant BIOPROJECT_ATTR   => 'bioproject_id';
use constant JIRA_STATUS_PUBL  => 'Sample Published';
use constant FUNDING_SRC_ATTR  => 'funding_source';
use constant UNKNOWN_SEQUENCER => 'unspecified';
use constant BAD_SEQUENCER     => 'Sequencer Mismatch'; 
use constant PAIRED_LAYOUT     => 'paired';
use constant NONDIR_LAYOUT     => 'single';
use constant TEMP_SRA_VAL      => 'SRA_Submission_Pending';

my @fastq_dirs = (qw(solexa iontorrent));
my @sff_dirs   = (qw(sff));
my %solexa_map = ( SOLEXA4   => '454_GS_FLX_Titanium',
                   SOLEXA5   => 'Illumina_MiSeq',
                   SOLEXA6   => 'Illumina_MiSeq',
                   SOLEXA7   => 'unspecified',
                   SOLEXA8   => 'IlluminaNextSeq',
                  '1IONJCVI' => 'Ion_Torrent_PGM');

my @default_headers  = (qw(spuid first_name last_name email sra_title bioproject_id biosample_id library_name library_strategy library_source library_selection library_layout instrument_model));
my @paired_headers   = (qw(seqfile_pair1 seqfile_pair2));
my @nondir_headers   = (qw(seqfile_frag));
my @rel_date_headers = (qw(release_date));
my $has_unidir = 0;
my $has_paired = 0;

my @instruments = keys(%solexa_map);

our ($ARGV_database, $ARGV_template, $ARGV_first_name, $ARGV_last_name, $ARGV_batch_id, $ARGV_bac_id, $ARGV_collection_id, $ARGV_tuple_file, $ARGV_bac_id_file, $ARGV_sample_root, $ARGV_output_file_path, $ARGV_release_date, $ARGV_funding_source, $ARGV_dont_update_status, $ARGV_include_pending_sra, $ARGV_force_unpublished, $ARGV_library_strategy, $ARGV_library_source, $ARGV_library_selection, $ARGV_simulate_only, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Verifying that the user specified at most one of the input methods
my $in_prarams = 0;

if (defined($ARGV_bac_id)) {
    ++$in_prarams;
}
if (defined($ARGV_collection_id)) {
    ++$in_prarams;
}
if (defined($ARGV_batch_id)) {
    ++$in_prarams;
}
if (defined($ARGV_tuple_file)) {
    ++$in_prarams;
}
if (defined($ARGV_bac_id_file)) {
    ++$in_prarams;
}
if ($in_prarams > 1) {
    $logger->logdie("You must specify at most one among --bac_id, --batch_id, --collection_id, --tuple_file and --bac_id_file parameters.")
}
elsif ($ARGV_force_unpublished && $in_prarams == 0) {
    $logger->logdie("In order to use --force_unpublished, you need to specify also one among the following options: --bac_id, --batch_id, --collection_id, --tuple_file and --bac_id_file.")
}
## Initializing various stuff...

my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => $ARGV_database, server => $ARGV_server, pass_file => $ARGV_password_file);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

## Getting the list of BAC IDs...
my @bids = ();
my %sample_info = ();
my %bp_seen = ();

if ($in_prarams == 0){
    &getAllSamplesInDb();
}
elsif (defined($ARGV_bac_id)) {
    &getSample($ARGV_bac_id);
}
elsif (defined($ARGV_batch_id)) {
    &getSamplesByBatchId($ARGV_batch_id);
}
elsif (defined($ARGV_collection_id)) {
    &getSamplesByCollection($ARGV_collection_id);
}
elsif ($ARGV_tuple_file) {
    &getSamplesFromTupleFile($ARGV_tuple_file); 
}
else { 
    &getSamplesFromBacFile($ARGV_bac_id_file);
}
unless (scalar(@bids)) {
    $logger->logdie("No valid samples to be processed from the input set.");
}
## Going through all the samples checking which one needs to be processed for SRA submission.
my @srable = ();
&gatherAttributes();

unless (scalar(@srable)) {
    $logger->logdie("No processable samples left")
}
## Now almost doing the actual work. (first we need to process all the samples, then we can start writing the output, since some of the columns will be present/absent depending upon the data just processed)
print "\nNow processing ", scalar(@srable), " samples.\n\n";
&gatherReadsDirs();

## Now we are ready to write the records to the template file
if (scalar(keys(%sample_info))) {
    ## Assessing what optional columns we need to add
    &adjustHeaders();
    
    unless ($ARGV_dont_update_status) {
        &updateSraStatus();
    }
    &writeTSV();
    print "\n\nDone\n\n";
}
else {
    $logger->logdie("No sample left to process.");
}
##############################################################################################################
##---------------------------------------------- Subroutines -----------------------------------------------##
##############################################################################################################

sub getAllSamplesInDb {
    my $r_seids = $glk->getExtentsByType('SAMPLE');
    
    foreach my $eid (@{$r_seids}) {
        next if $glk->isDeprecated($eid);
        my $r_info = $glk->getExtentInfo($eid);
        my $bid = $r_info->{'ref'};
        push(@bids, $bid);
        $sample_info{$bid}{Extent_id} = $eid;
    }
        
}
sub getSample {
    my $bid = shift();
    my $eid = $glk->getExtentByTypeRef('SAMPLE', $bid);
    
    if (defined($eid)) {
        if ($glk->isDeprecated($eid)) {
            $logger->logdie("Sample $bid is flagged as \"deprecated\".")
        }
        push(@bids, $bid);
        $sample_info{$bid}{Extent_id} = $eid;
    }
    else {
        $logger->logdie("The supplied Sample (BAC) ID ($bid) was not found in the current database ($ARGV_database.")
    }
}
sub getSamplesByBatchId {
    my $batch_id = shift();
    $logger->trace("Retrieving the BAC IDs of batch \"$batch_id\" from the database.");
    my $r_bids = $glk->getBacIdByBatchId($batch_id);
    
    foreach my $bid (@{$r_bids}) {
        my $eid = $glk->getExtentByTypeRef('SAMPLE', $bid);
        next if $glk->isDeprecated($eid);
        push(@bids, $bid);
        $sample_info{$bid}{Extent_id} = $eid;
    }
}
sub getSamplesByCollection {
    my $collection_id = shift();
    my $ceid = $glk->getExtentByTypeRef('COLLECTION', $collection_id);
    
    unless (defined($ceid)) {
        $logger->logdie("Unable to locate collection \"$ARGV_collection_id\" in database $ARGV_database.")
    }
    my $r_lots = $glk->getExtentChildren($ceid);
    
    foreach my $leid (@{$r_lots}) {
        my $s_eids = $glk->getExtentChildren($leid);
        
        foreach my $eid (@{$s_eids}) {
            next if $glk->isDeprecated($eid);
            my $r_info = $glk->getExtentInfo($eid);
            my $bid = $r_info->{'ref'};
            $sample_info{$bid} = {Extent_id  => $eid,
                                  collection => $collection_id};
            push(@bids, $bid);                      
        }
    }
}
sub getSamplesFromTupleFile {
    my $tuple_file = shift();
    open (my $list, "$tuple_file") || $logger->logdie("cannot open $tuple_file. \"$!\"");
    
    while (<$list>) {
        chomp();
        next if /^#/ || /^\s*$/;
        my ($db, $coll, $bid) = split /,/;
        
        unless ($db eq $ARGV_database) {
            $logger->warn("Tuple \"$_\" does not belong to the current database (\"$ARGV_database\"). Skipping it.");
            next
        }
        if ($bid =~ /^\d+$/) {
            my $eid = $glk->getExtentByTypeRef('SAMPLE', $bid);
            
            if (defined($eid)) {
                if ($glk->isDeprecated($eid)) {
                    $logger->warn("Sample $bid (Tuple: \"$_\") is flagged as deprecated. - Skipping it");
                }
                else {
                    push(@bids, $bid);
                    $sample_info{$bid} = {collection => $coll,
                                          Extent_id  => $eid};
                }
            }
            else {
                $logger->warn("Unable to find sample $bid (Tuple \"$_\") in the current database ($ARGV_database). - Skipping it.");
            }
        }
        else {
            $logger->warn("This program deals only with tuples containing only actual BAC (sample) IDs, not modified ones, like in the current tuple (\"$_\"). - Skipping it");
            next
        }
    }
    close($list);
    
    unless (scalar(@bids)) {
        $logger->logdie("Unable to find any Sample (BAC) ID in the tuple file $tuple_file.")
    }
}
sub getSamplesFromBacFile {
    my $bid_file = shift();
    open (my $list, "bid_file") || $logger->logdie("cannot open $bid_file. \"$!\"");
    
    while (<$list>) {
        chomp();
        next if /^#/ || /^\s*$/;
        
        if (/^(\d+)\s*$/) {
            my $bid = $1;
            my $eid = $glk->getExtentByTypeRef('SAMPLE', $bid);

            if (defined($eid)) {
                if ($glk->isDeprecated($eid)) {
                    $logger->warn("Sample $bid  is flagged as deprecated. - Skipping it");
                }
                else {
                    push(@bids, $bid);
                    $sample_info{$bid}{Extent_id} = $eid;
                }
            }
            else {
                $logger->warn("Unable to find sample $bid in the current database ($ARGV_database). - Skipping it.");
            }
        }
        else {
            $logger->error("Unable to parse the following line in the input list \"$bid_file\":\n\"$_\"");
            next;
        }
    }
    close($list);
    
    unless (scalar(@bids)) {
        $logger->logdie("Unable to find any Sample (BAC) ID in the input file $bid_file.")
    }
}
sub gatherAttributes {
    foreach my $bid (@bids) {
        my $eid = $sample_info{$bid}{Extent_id};
        $logger->debug("Now checking Extent $eid.");
        
        if ($glk->hasExtentAttribute($eid, SRA_STUDY_ATTR)) {
            if ($ARGV_include_pending_sra) {
                my $sra_study = $glk->getExtentAttribute($eid, SRA_STUDY_ATTR);
                
                if ($sra_study eq TEMP_SRA_VAL) {
                    $logger->debug("This sample if flagged as being already submitted to SRA and in pending state. - Keeping it (--include_pending_sra).");
                    $sample_info{$bid}{SRA_Study} = TRUE;
                }
                else {
                    $logger->debug("This sample has been already submitted to SRA. - Skipping it.");
                    &delistSample($bid);
                    next
                }
            }
            else {
                $logger->debug("This sample has been already submitted to SRA or has been already processed through this pipeline.");
                &delistSample($bid);
                next
            }
        }
        else {
            $sample_info{$bid}{SRA_Study} = FALSE;
        }
        if (!$ARGV_force_unpublished) {
            if ($glk->hasExtentAttribute($eid, JIRA_STATUS_ATTR)) {
                my $status = $glk->getExtentAttribute($eid, JIRA_STATUS_ATTR);
                
                unless ($status eq JIRA_STATUS_PUBL) {
                    $logger->debug("This sample has not been published (JIRA Status: \"$status\" - Skipping it (option --force_unpublished not used)");
                    &delistSample($bid);
                    next
                }
            }
            else {
                $logger->error("Sample Extent $eid does not have the required " . JIRA_STATUS_ATTR . " attribute. - Skipping it.");
                &delistSample($bid);
                next
            }
        }
        my $bpid = &processBioProjects($bid);
        next unless defined($bpid);
        my $bsid = &processBioSample($bid);
        next unless defined($bsid);
        
        if ($ARGV_funding_source) {
            next unless &hasFundingSource;
        }
        unless (defined($sample_info{$bid}{collection})) {
            $sample_info{$bid}{collection} = $glk->getCollection($eid);
        }
        ## Checking if a directory actually exists for that sample...
        my $expected_dir = &checkSampleDir($bid);
        
        if (defined($expected_dir)) {
            push(@srable, [$bid, $expected_dir]);
        }
    }
}
sub processBioProjects {
    my $bid = shift();
    my $eid = $sample_info{$bid}{Extent_id};
    my $r_bpids = $glk->getBioprojectsList($eid);
    
    unless (scalar(@{$r_bpids})) {
        $logger->warn("Sample $bid does not have any BioProject ID assigned to it. - Skipping it.");
        &delistSample($bid);
        return undef
    }
    my @bp_candidates = ();
    
    foreach my $bpid (@{$r_bpids}) {
        if (exists($bp_seen{$bpid})) {
            if ($bp_seen{$bpid} == IRRELEVANT) {
                next
            }
            else {
                push(@bp_candidates, $bpid);
            }
        }
        else {
            my $lt_pfix = $glk->getBioProjectLocusTagPfix($bpid);
    
            if (defined($lt_pfix) && $lt_pfix =~ /^\S+$/) {
                push(@bp_candidates, $bpid);
                $bp_seen{$bpid} = RELEVANT;
            }
            else {
                $bp_seen{$bpid} = IRRELEVANT;
            }
        }
    }
    if (scalar(@bp_candidates) > 1) {
        $logger->error("Sample $eid has more than one BioProject ID associated with a locus_tag prefix: \"" . join('", ', @bp_candidates) . "\" This issue needs to be solved before proceeding with the submission of this sample.");
        &delistSample($bid);
        return undef
    }
    elsif (scalar(@bp_candidates) == 0) {
        $logger->error("Sample $eid does not have any BioProject ID associated with it. This issue needs to be solved before proceeding with the submission of this sample.");
        &delistSample($bid);
        return undef
    }
    $sample_info{$bid}{bioproject_id} = $bp_candidates[0];
    return $bp_candidates[0]
}
sub processBioSample {
    my $bid = shift();
    my $eid = $sample_info{$bid}{Extent_id};
    
    if ($glk->hasExtentAttribute($eid, BIOSAMPLE_ATTR)) {
        my $bsid = $glk->getExtentAttribute($eid, BIOSAMPLE_ATTR);
        $logger->debug("Sample $bid BioSample ID: \"$bsid\"");
        $sample_info{$bid}{biosample_id} = $bsid;
        return $bsid
    }
    else {
        $logger->error("Sample $bid - Extent $eid does not have the required " . BIOSAMPLE_ATTR . " attribute. - Skipping it.");
        &delistSample($bid);
        return undef
    }
}
sub hasFundingSource {
    my ($bid, $exp_fs) = @_;
    my $eid = $sample_info{$bid}{Extent_id};
    my $fs = $glk->getCombinedExtentAttribute($eid, FUNDING_SRC_ATTR);
    my $processable = 0;
        
    if (defined($fs)) {
        my @fss = split(/\s*;\s*/, $fs);
            
        foreach my $src (@fss) {
            next unless $src eq $exp_fs;
            ++$processable;
            last
        }
        if ($processable) {
            return TRUE
        }
        else {
            &delistSample($bid);
            return FALSE
        }
    }
    else {
        $logger->error("Missing \"" . FUNDING_SRC_ATTR . "\" attribute. Unable to establish if it was sequenced under $ARGV_funding_source. - Skipping it.");
        &delistSample($bid);
        return FALSE
    }
}
sub checkSampleDir {
    my $bid = shift();
        my $expected_dir = "$ARGV_sample_root/$ARGV_database/$sample_info{$bid}{collection}/$bid";
    
    if (-e $expected_dir) {
        if (-d $expected_dir) {
            return $expected_dir
        }
        elsif (-l $expected_dir) {
            $logger->warn("Sample $bid - Found a link instead of directory \"$expected_dir\". Likely it has been archived - Skipping it.");
            &delistSample($bid);
        }
        else {
            $logger->error("Found filesystem element \"$expected_dir\", but it is neither a directory or a symbolic link. - Skipping it.");
            &delistSample($bid);
        }
    } 
    else {
        $logger->warn("unable to find neither the sample directory \"$expected_dir\", nor a symbolic link to Archive. Possibly, sequencing data for this sample no longer exists. - Skipping it");
        &delistSample($bid);
    }
    return undef
}
sub gatherReadsDirs {
    foreach my $sdata (@srable) {
        my ($bid, $sample_dir) = @{$sdata};
        my @sff_files = ();
        my %file_register = ();

        ## Searching for fastq files
        &findFasqFiles($sdata, \%file_register);
        ## Searching for sff files (454 sequences)
        &findSffFiles($sdata, \@sff_files);
        
        ## Given the messy way the 454 files are organized and the possible presence of multiple variations of the same .sff file with 
        ## alterations in the middle part of the filename, samples having 454 sequences should be manually revised before proceeding 
        #  with the submission pipeline.
        if (scalar(@sff_files)) {
            $logger->warn("Sample $bid has one or more SFF files. Please, edit manually the entries for this sample to remove possible duplicate files.");
        }
        my $has_something = 0;
        
        if (scalar(keys(%file_register))) {
            $sample_info{$bid}{fastq} = \%file_register;
            ++$has_something;
        }
        elsif (!$ARGV_simulate_only) {
            $logger->warn("Sample $bid does not have any FASTQ file in the sample directory \"$sample_dir\".");
        }
        if (scalar(@sff_files)) {
            $sample_info{$bid}{sff} = \@sff_files;
            ++$has_unidir;
            ++$has_something;
        }
        unless ($has_something) {
            &delistSample($bid);
        }
    }
}
sub findFasqFiles {
    my ($s_data, $r_freg) = @_;
    my @fq_files = ();
    my ($bid, $sample_dir) = @{$s_data};
    
    foreach my $dir (@fastq_dirs){
        my $fqdir = "$sample_dir/$dir";
        $logger->trace("Probing FASTQ directory \"$fqdir\".");
        
        if (-d $fqdir) {
            my $fqdh;
            $logger->debug("Found directory \"$fqdir\".");
            unless (opendir($fqdh, $fqdir)) {
                $logger->error("Impossible to access directory \"$fqdir\" for reading. - Skipping it.");
                next
            }
            foreach my $element (readdir($fqdh)) {
                if (-f "$fqdir/$element" && $element =~ /\.fastq$/) {
                    $logger->trace("Found FASTQ file \"$fqdir/$element\"");
                    push(@fq_files, "$fqdir/$element");
                }
            }
        }
    }
    ## Now processing all the FASTQ files
    foreach my $fq (@fq_files) {
        unless (-s $fq) {
            $logger->warn("FASTQ file \"$fq\" is empty. - skipping it.");
            next
        }
        ## Opening the file to detect with what machine they were sequenced on...
        my $sequencer = &gatherSequencer($fq);
        next unless defined($sequencer);
        
        ## Splitting fastq file for direction or, anyhow, verify its integrity.
        next unless &splitFastq($bid, $fq, $sequencer, $r_freg);
    }
    ## Going through the files in orderly manner further verifying that there is consistency between read orientation, etc.
    &purgeMispairedFastq($bid, $r_freg);
}
sub gatherSequencer {
    my $fq_file = shift();
    
    if (open(my $fqh, $fq_file)) {
        chomp(my $first_head = <$fqh>);
        close($fqh);
        
        if ($first_head =~ /^\@(\S+)/) {
            my $seq_id = $1;
            my $sequencer = UNKNOWN_SEQUENCER;
            
            foreach my $instrument (@instruments) {
                next unless $seq_id =~ /$instrument/;
                $sequencer = $solexa_map{$instrument};
                last
            }
            if ($sequencer eq UNKNOWN_SEQUENCER) {
                $logger->error("Unable to identify the instrument from the read ID (\"$seq_id\") - Setting the instrument to \"$sequencer\"");
            }
            return $sequencer
        } 
        else {
            $logger->error("Corrupted FASTQ file (\"$fq_file\") Expecting a \@ at the beginning of the first line (\"$first_head\") - Skipping it.");
        }
    }
    else {
        $logger->error("Unable to open the FASTQ file \"$fq_file\" for reading. - Skipping it.");
    }
    return undef
} 
sub splitFastq {
    my ($bid, $fq_file, $sequencer, $r_freg) = @_;
    my $out_root = $ARGV_database . '_' . $sample_info{$bid}{collection} . '_' . $bid . '_' . basename($fq_file);
    $out_root =~ s/\.fastq$//;
    $out_root =~ s/\.[Rr]?[12]$//; ## Removing possible directionality infor from the file name
    my $fwd_fq = "$out_root.R1.fastq";
    my $rev_fq = "$out_root.R2.fastq";
    my $ndir_fq = "$out_root.fastq";
    my $job_done = $ARGV_output_file_path . '/' . basename($fq_file) . '.' . SPLITTER_SUCC_EXT;
    
    my $split_fq_cmd = FASTQ_SPLITTER . " -i $fq_file -o $ARGV_output_file_path -f $fwd_fq -r $rev_fq -u $ndir_fq";
    
    if ($ARGV_simulate_only) {
        $logger->info("Simulating the splitting of fastq files.\nCMD: \"$split_fq_cmd\"");
        
        unless (-e $fwd_fq || -e $rev_fq || -e $ndir_fq) { ## we do not want to mess with the files, in the case they already exist
            my @outcome = ([$fwd_fq], [$rev_fq], [$ndir_fq], [$fwd_fq, $rev_fq], [$fwd_fq, $ndir_fq], [$rev_fq, $ndir_fq], [$fwd_fq, $rev_fq, $ndir_fq]);
            
            foreach my $file (@{$outcome[int(rand(7))]}) {
                my $cmd = "touch $ARGV_output_file_path/$file";
                system($cmd) && $logger->error("Problems running the following command: \"$cmd\"");
            }
        }
        unless (-e $job_done) {
            my $cmd = "touch $job_done";
            system($cmd) && $logger->error("Problems running the following command: \"$cmd\"");
        }
    }
    else {
        if (system($split_fq_cmd)) {
            $logger->error("Problems running the following command: \"$split_fq_cmd\" (\"$!\" - \"$?\")");
            return FAILURE
        }
    }
    unless (-e $job_done) {
        $logger->error("Command \"$split_fq_cmd\" exited without any error code, but completion file \"$job_done\" has not been created.");
        return FAILURE
    }
    ## If we have non-empty forward & reverse files
    if (-e "$ARGV_output_file_path/$fwd_fq" && -s "$ARGV_output_file_path/$fwd_fq" && -e "$ARGV_output_file_path/$rev_fq" && -s "$ARGV_output_file_path/$rev_fq") {
        if (-e "$ARGV_output_file_path/$ndir_fq" && -s "$ARGV_output_file_path/$ndir_fq") {
            $logger->warn("FASTQ file \"$fq_file\" contains records with both directional and non-directional reads or badly-parsed IDs - This is highly unusual and would require manual verification. - Skipping this file");
            return FAILURE
        }
        $r_freg->{$out_root} = {forward   => "$ARGV_output_file_path/$fwd_fq",
                                'reverse' => "$ARGV_output_file_path/$rev_fq",
                                sequencer => $sequencer};
        return SUCCESS
    }
    elsif (-e "$ARGV_output_file_path/$fwd_fq" && -s "$ARGV_output_file_path/$fwd_fq" || -e "$ARGV_output_file_path/$rev_fq" && -s "$ARGV_output_file_path/$rev_fq") {
        if (-e "$ARGV_output_file_path/$ndir_fq" && -s "$ARGV_output_file_path/$ndir_fq") {
            $logger->warn("FASTQ file \"$fq_file\" contains records with both directional (only one direction) and non-directional reads or badly-parsed IDs - This is highly unusual and would require manual verification. - Skipping this file");
            return FAILURE
        }
        ## This is a legitimate case: the input file has not been merged with the input file of the opposite read.
        my ($direction, $file) = (-e "$ARGV_output_file_path/$fwd_fq" && -s "$ARGV_output_file_path/$fwd_fq") ? ('forward', "$ARGV_output_file_path/$fwd_fq") : ('reverse', "$ARGV_output_file_path/$rev_fq"); ##

        if (exists($r_freg->{$out_root}{sequencer}) && $r_freg->{$out_root}{sequencer} ne $sequencer) {
            $logger->error("Mismatch between information in previously parsed files and current file (\"$fq_file\") regarding the sequencer: \"$sequencer\" vs. \"$r_freg->{$out_root}{sequencer}\" - Skipping the current file and flagging the previous as bad.\n" . Dumper($r_freg->{$out_root}));
            $r_freg->{$out_root}{sequencer} = BAD_SEQUENCER;
        }
        else {
            $r_freg->{$out_root} = {$direction => $file,
                                    sequencer  => $sequencer};
            return SUCCESS
        }
    }
    elsif (-e "$ARGV_output_file_path/$ndir_fq" && -s "$ARGV_output_file_path/$ndir_fq") {
        $r_freg->{$out_root}{nondir} = "$ARGV_output_file_path/$ndir_fq";
        return SUCCESS
    }
    elsif ($ARGV_simulate_only) {
        return SUCCESS
    }
    else {
        $logger->warn("FASTQ file \"$fq_file\" does not appear to contain any valid FASTQ record. - Ignoring it.")
    }
    return FAILURE
}

sub fixReadNames {
    my ($filename) = @_;
    my $tmp = $filename . '.tmp';
    move($filename, $tmp);
    open(my $tfh, $tmp) || $logger->logdie("Impossible to open the fastq file \"$tmp\" for reading.");
    open(my $cfh, ">$filename") || $logger->logdie("Impossible to open the fastq file \"$filename\" for writing.");
    my $line = 0;
    
    while (<$tfh>) {
        unless ($line++ % 4) { ## Assuming that each fastq record is exactly 4 lines
            unless (/^\@/) {
                $logger->error("Unable to parse correctly fastq file \"$tmp\" at line $line.");
                &closeAll($tfh, $cfh);
                return FAILURE
            }
            s/(?:#[ACGT]+)?\/1\s*$/\n/;
        }
        print {$cfh} $_;
    }
    return SUCCESS
}

sub purgeMispairedFastq {
    my ($bid, $r_freg) = @_;
    my %delendi = ();
    
    foreach my $out_root (keys(%{$r_freg})) {
        my @files = ();
        my $last_key = '';
            
        while (my ($key, $file) = each(%{$r_freg->{$out_root}})) {
            next if $key eq 'sequencer';
            push(@files, $file);
            $last_key = $key;
        }
        unless (exists($r_freg->{$out_root}{sequencer})) {
            $jlt->die("Unexpected situration: Sample $bid - Found set of files without any sequencer defined:\n" . join("\n", @files))
        }
        if ($r_freg->{$out_root}{sequencer} eq BAD_SEQUENCER) {
            $logger->warn("Sample $bid - Skipping the following file(s) because of mismatched sequencer information:\n" . join("\n", @files));
            undef($delendi{$out_root});
            next
        }
        if (exists($r_freg->{$out_root}{nondir})) {
            if (scalar(@files) > 1) {
                $logger->warn("Sample $bid - Found non-directional and directional reads for the same sequencing run. - Skipping the following files:\n" . join("\n", @files));
                undef($delendi{$out_root});
            }
            else {
                ++$has_unidir;
            }
        }
        elsif (scalar(@files) < 2) { # This means that we have only forward or reverse, but not both. If we have only the forward read, we interpret it as bad renaming of the reads
            if ($last_key eq 'forward') {
                if (&fixReadNames($files[0])) {
                    $r_freg->{$out_root}{nondir} = $files[0];
                    undef($r_freg->{$out_root}{forward});
                    delete($r_freg->{$out_root}{forward});
                    ++$has_unidir;
                    --$has_paired;
                }
                else {
                    $logger->warn("Sample $bid - Found only a corrupted forward reads for this sequencing run. - Skipping the following file:\"$files[0]\".");
                    undef($delendi{$out_root});
                }
            }
            else {
                $logger->warn("Sample $bid - Found only forward or reverse reads for this sequencing run. - Skipping the following file:\n" . join("\n", @files));
                undef($delendi{$out_root});
            }
        }
        elsif (scalar(@files) == 2) {
            ++$has_paired;
        }
        else {
            $jlt->die("Unexpected situation encountered: Sample $bid has more than two files from the same run and none of them is non-directional:\n" . join("\n", @files))
        }
    }
    ## Purging the mispaired files
    foreach my $out_root (keys(%delendi)) {
        undef($r_freg->{$out_root});
        delete($r_freg->{$out_root});
    }
}
sub findSffFiles {
    my ($r_data, $r_sfff) = @_;
    my ($bid, $sample_dir) = @{$r_data};
    
    foreach my $dir (@sff_dirs){
        my $sffdir = "$sample_dir/$dir";
        
        if (-d $sffdir) {
            
            if (opendir(my $sffdh, $sffdir)) {
                foreach my $element (readdir($sffdh)) {
                    if (-f "$sffdir/$element" && $element =~ /\.sff$/) {
                        push(@{$r_sfff}, "$sffdir/$element");
                    }
                }
                closedir($sffdh);
            }
            else {
                $logger->error("Impossible to access directory \"$sffdir\" for reading. - Skipping it.");
                next
            }
        }
    }
}
sub adjustHeaders {
    if ($has_paired) {
        push(@default_headers, @paired_headers);
    }
    if ($has_unidir) {
        push(@default_headers, @nondir_headers);
    }
    if (defined($ARGV_release_date)) {
        push(@default_headers, @rel_date_headers);
    }
}
sub writeTSV {
    ## Creating a map of positions of all the headers
    my $n = 0;
    my %hdr_col = map({$_ => $n++} @default_headers);
    open(my $out, ">$ARGV_template") || $logger->logdie("Impossible to open the output CSV file \"$ARGV_template\" for writing.");
    print {$out} join("\t", @default_headers), "\n";
    
    foreach my $bid (sort(keys(%sample_info))) { ## Sorting the samples just for convenience
        my $bsid = $sample_info{$bid}{biosample_id};
        my $bpid = $sample_info{$bid}{bioproject_id};
        
        foreach my $out_root (sort(keys(%{$sample_info{$bid}{fastq}}))) {
            my $sra_title = "$bsid SRA $out_root";
            my @row = ($out_root, $ARGV_first_name, $ARGV_last_name, CONTACT_EMAIL, $sra_title, $bpid, $bsid, $out_root, $ARGV_library_strategy, $ARGV_library_source, $ARGV_library_selection);
            
            if (defined($sample_info{$bid}{fastq}{$out_root}{nondir})) {
                $row[$hdr_col{library_layout}] = NONDIR_LAYOUT;
                $row[$hdr_col{seqfile_frag}] = $sample_info{$bid}{fastq}{$out_root}{nondir};
                
                if ($has_paired) {
                     $row[$hdr_col{seqfile_pair1}] = '';
                     $row[$hdr_col{seqfile_pair2}] = '';
                }
            }
            else {
                $row[$hdr_col{library_layout}] = PAIRED_LAYOUT;
                $row[$hdr_col{seqfile_pair1}] = $sample_info{$bid}{fastq}{$out_root}{forward};
                $row[$hdr_col{seqfile_pair2}] = $sample_info{$bid}{fastq}{$out_root}{'reverse'};
                $row[$hdr_col{seqfile_frag}] = '' if $has_unidir;
            }
            $row[$hdr_col{instrument_model}] = $sample_info{$bid}{fastq}{$out_root}{sequencer};
            
            if (defined($ARGV_release_date)) {
                $row[$hdr_col{release_date}] = $ARGV_release_date;
            }
            print {$out} join("\t", @row), "\n";
        }
        foreach my $sff (@{$sample_info{$bid}{sff}}) {
            (my $out_root = $sff) =~ s/\.sff$//;
            my $sra_title = "$bsid SRA $out_root";
            my @row = ($out_root, $ARGV_first_name, $ARGV_last_name, CONTACT_EMAIL, $sra_title, $bpid, $bsid, $out_root, $ARGV_library_strategy, $ARGV_library_source, $ARGV_library_selection, NONDIR_LAYOUT, UNKNOWN_SEQUENCER, $sff);
            push(@row, $ARGV_release_date) if defined($ARGV_release_date);
            print {$out} join("\t", @row), "\n";
        }
    }
    close($out);
}
sub updateSraStatus {
    my $troubles = 0;
    
    foreach my $bid (keys(%sample_info)) {
        if ($sample_info{$bid}{SRA_Study}) {
            $logger->trace("Sample $bid has already the correct value (\"" . TEMP_SRA_VAL . '") in attribute "' . SRA_STUDY_ATTR . '" - Nothing to do.');
        }
        else {
            my $eid = $sample_info{$bid}{Extent_id};
            
            if ($ARGV_simulate_only) {
                $logger->info("Sample $bid (Extent_id $eid) - Simulating the insertion of attribute \"" . SRA_STUDY_ATTR . '" with value "' . TEMP_SRA_VAL . '.');
            }
            else {
                $glk->addExtentAttribute($eid, SRA_STUDY_ATTR, TEMP_SRA_VAL) || ++$troubles;
            }
        }
    }
    if ($troubles) {
        $logger->logdie('Problems inserting the attribute "' . SRA_STUDY_ATTR . '" with value "' . TEMP_SRA_VAL . '" in one or more samples.')
    }
}

sub delistSample {
    my $bid = shift();
    undef($sample_info{$bid});
    delete($sample_info{$bid});
}

sub closeAll {
    my @fhs = @_;
    
    foreach my $fh (@fhs) {
        close($fh);
    }
}
