#!/usr/local/bin/perl

# File: runDeepSequencinganalysis.pl
# Author: Paolo Amedeo
# Created: August 23, 2016
#
# $Author:  $
# $Date: $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# runDeepSequencinganalysis.pl is the replacement of Tim Stockwell's "editable text file with command line commands"
# used for running deep sequencing variant analysis.

=head1 NAME
    
    runDeepSequencinganalysis.pl
    
=head1 USAGE

    runDeepSequencinganalysis.pl [-]-t[uple_file] <tuple_file> [-]-analysis_type <type_of_analysis> [-]-output_dir <output_dir> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-t[uple_file] <tuple_file>

Tuple file ($database,$collection,$bac_id)

=for Euclid:
    tuple_file.type:   readable

=item [-]-analysis_type <type_of_analysis>

    Type of analysis to be performed.
    List of accepted values:

    sample_consensus
    master_reference
    multiseq_alignment

=for Euclid:
    type_of_analysis.type: string
    
=item [-]-output_dir <output_dir>

Directory where to store the summary files

=for Euclid:
    output_dir.type: string
    
=back


=head1 OPTIONS

=over

=item [-]-vigor_db <vigor_database>
    
    Name of the Vigor database to be used for the annotation (by default it uses the same value as the database specified in the tuple).

=item [-]-min_depth_snps <min_depth>

    Minimum sequencing depth usable to determine a SNP (default 10)

=for Euclid:
    min_depth.type:    int > 0
    min_depth.default: 10
    
=item [-]-min_freq_snps <min_percent>

    Minimum percentage of base call in order to call a minor variant (default: 3)
    
=for Euclid:
    min_percent.type:    number, min_percent > 0 && min_percent < 100
    min_percent.default: 3
    
=item [-]-min_depth_differences <min_variant_depth>

    Minimum sequencing depth for a given variant to be called (default: 10)

=for Euclid:
    min_variant_depth.type:    int > 0
    min_variant_depth.default: 10

=item [-]-reference_seq <external_reference_fasta>

    Fasta file containing the sequence to use as reference for either master_reference or multiseq_alignment type of analysis.
    This option is not required (and actually ignored) for sample_consensus analysis.
    Note: The defline of each sequence should have as identifier (i.e. the first word after the ">" sign) the segment name ("MAIN" for unsegmented viruses)
    
=for Euclid:
     external_reference_fasta.type: readable

=item [-]-work_dir <workingDir>

    Directory where to write the intermediate files files.

=for Euclid:
    workingDir.type: string

=item [-]-sample_data_root_dir <sample_data_root>

    Name of the directory under which the info (including traces, etc.) is stored in a structure $db_name/$collection_name/$bac_id/
    Default: /usr/local/projdata/700010/projects/VHTNGS/sample_data_new

=for Euclid:
    sample_data_root.type:    string
    sample_data_root.default: '/usr/local/projdata/700010/projects/VHTNGS/sample_data_new'

=item [-]-summary_prefix <summary_files_pfix>

    Prefix for the name of the summary files. (Default: "DSA")
    Note: if you intend to store the results of multiple analysis in the same output directory, you should provide a value.

=for Euclid:
    summary_files_pfix.type:    string
    summary_files_pfix.default: 'DSA'

=item [-]-keep_work_files

    Do not delete the work files at the end of the analysis. The default behavior of the script is to keep (of course) the files with the result (stored in the output directory) and delete the working_dir with all the files inside.

=item [-]-project_code <prj_code>

    Project code (required for grid jobs)

=for Euclid:
    prj_code.type: string

=item [-]-grid_queue <queue_name>

    Grid queue to be used (Default: himem)
    Available queues:
    himem
    fast
    medium
    default
    

=for Euclid:
    queue_name.type:    string
    queue_name.default: "himem"
    
=item [-]-max_grid_jobs <job_number>

    Maximum number of jobs to launch on the grid at once (Default: 40)

=for Euclid:
    job_number.type:    int > 0
    job_number.default: 40

=item [-]-run_locally

    Do not run the jobs on the grid, run them sequentially on the local machine, instead.

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

    runDeepSequencinganalysis.pl is the replacement of Tim Stockwell's "editable text file with command line commands" used for running deep sequencing variant analysis.

=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
use lib ("$FindBin::Bin");
use Getopt::Euclid 0.2.4 (qw(:vars));
#use Data::Dumper;
use File::Basename;
use File::Path;
use JCVI::Logging::L4pTools;
use TIGR::GLKLib;
use JCVI::DB_Connection::VGD_Connect;
use ProcessingObjects::SafeIO;
use DSA_Tools;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;

use constant ACCESSIONS  => 0;
use constant COLLECTION  => 1;
use constant EXTENT_ID   => 2;
use constant BLINDED_NO  => 3;
use constant VIRUS_NAME  => 4;
use constant STRAIN_CODE => 5;
use constant RESULTS_DIR => 6;

use constant A_COUNT    => 0;
use constant C_COUNT    => 1;
use constant G_COUNT    => 2;
use constant T_COUNT    => 3;
use constant MAJMIN     => 4;
use constant MINOR      => 5;
use constant MIN_PERC   => 6;

use constant POLY_REF   => 0;
use constant REGION     => 1;
use constant REGION_SUM => 2;
use constant EFFECT     => 3;
use constant AA_POS     => 4;

use constant DEFAULT_MISSING_REGION  => 'INTERGENIC';
use constant DEFAULT_SEQ_ROOT        => '/usr/local/projdata/700010/projects/VHTNGS/sample_data_new';
use constant FIND_VARS_EXE           => "$FindBin::Bin/findVariants.pl";
use constant FINISHED_JOBS_DELAY     => 20; ## seconds to wait after we don't find anything on the grid queue
use constant GRID_WORKING_DIR        => '/usr/local/scratch/VIRAL/DSA';
use constant GRID_WD_PERMS           => 0777;
use constant JOB_CHECK_INTERVAL      => 30; # seconds
use constant JOB_NAME                => 'ViralDeepSeqAnalysis';
use constant JOB_SUBMIT_INTERVAL     => 5; # Wait time between consecutive jobs submissions to reduce issues with scheduler
use constant LOST_JOBS_ALARM_CYCLE   => 3; # Number of job checking intervals with no qstat report to trigger lost jobs error
use constant NO_STRAIN_CODE          => 'N/A';
use constant OUT_DIR_PERMS           => 0777;
use constant REF_SEQ_NAME            => 'reference.fasta';
use constant SEG_POS_COUNTS          => '_bac_seg_pos_counts_maj_min_f';
use constant SEG_POS_SUMMARY         => '_summary_bac_seg_pos_counts_maj_min_f';
use constant SORTMAPDIR              => '/usr/local/scratch/VIRAL/sorttmp';

use constant START_DB                => 'genome_viral';
use constant TEMP_DIR_PATH           => '/usr/local/scratch/VIRAL/clc';
use constant QSTAT_EXE               => '/usr/local/sge_current/bin/lx-amd64/qstat';
use constant QSTAT_JOB_NAME_LN       => 10; ## length of the job name string reported by qstat
use constant QSUB_EXE                => '/usr/local/sge_current/bin/lx-amd64/qsub';
use constant VAR_CLA_FINAL           => '_final_summary_var_cla_results_reformat_f';
use constant VAR_CLA_RESULTS         => '_variant_classifier_results_f';
use constant VAR_CLA_SUMMARY         => '_summary_var_cla_results_reformat_f';
#

## Setting the Environment

$ENV{PATH}            = "/usr/local/packages/clc-ngs-cell:/usr/local/packages/clc-bfx-cell:/usr/local/bin:/usr/local/packages/seq454-2.6/bin:$ENV{PATH}";
$ENV{LD_LIBRARY_PATH} = "/usr/local/packages/python/lib:/usr/local/packages/atlas/lib:/usr/local/packages/boost/lib:/usr/local/packages/gcc/lib64:$ENV{LD_LIBRARY_PATH}";
$ENV{TMP}             = TEMP_DIR_PATH;
$ENV{TMPDIR}          = $ENV{TMP};
$ENV{SORTMAPDIR}      = SORTMAPDIR;

our ($ARGV_tuple_file, $ARGV_analysis_type, $ARGV_output_dir, $ARGV_vigor_db, $ARGV_min_depth_snps, $ARGV_min_freq_snps, $ARGV_min_depth_differences, $ARGV_reference_seq,  $ARGV_work_dir, $ARGV_sample_data_root_dir, $ARGV_summary_prefix, $ARGV_keep_work_files, $ARGV_project_code, $ARGV_grid_queue, $ARGV_max_grid_jobs, $ARGV_run_locally, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Initializing various stuff...

my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => START_DB, server => $ARGV_server, pass_file => $ARGV_password_file);
my $glk  = TIGR::GLKLib->new($jdb->dbh());
$glk->setAttrValValidation(FALSE);
my $dsa = DSA_Tools->new(min_freq_snps => $ARGV_min_freq_snps, min_depth_snps => $ARGV_min_depth_snps, min_depth_diffs => $ARGV_min_depth_differences);
my $job_root_name = JOB_NAME . "_$$";
my $grid_op_dir = GRID_WORKING_DIR . "/$$";


my $data_root_dir = defined($ARGV_sample_data_root_dir) ? abs_path($ARGV_sample_data_root_dir) : DEFAULT_SEQ_ROOT;

my @commands = ();
my @processed = ();
my %sample_info = ();
my @completed_jobs = ();
my %sample_db = ();
my @cleanup = ();

open(my $tfh, $ARGV_tuple_file) || $logger->logdie("Impossible to open the tuple file (\"$ARGV_tuple_file\") for reading.");

while (<$tfh>) {
    next if /^#/ || /^\s*$/;
    s/[\r\n]*//g;
    my ($db,$coll,$bid) = split /,/;
    $sample_info{$db}{$bid}[COLLECTION] = $coll;
    $sample_db{$bid} = $db;
}
close($tfh);
$ARGV_output_dir = abs_path($ARGV_output_dir);

unless (-d $ARGV_output_dir) {
    mk_tree_safe($ARGV_output_dir, OUT_DIR_PERMS) || $logger->logdie("Impossible to create the output directory (\"$ARGV_output_dir\")");
}
## Pulling info out of the database(s) and composing the command lines.
while (my($db, $data) = each(%sample_info)) {
    if ($glk->isVgdDb($db)) {
        $glk->changeDb($db);
    }
    else {
        $logger->warn("Database \"$db\" is not a valid VGD database or is off-line.");
        next
    }
    
    foreach my $fs_bid (keys(%{$data})) {
        my $bid = $fs_bid;
        $bid = $1 if $fs_bid =~ /(\d+)[^\d\s]+/;
        my $eid = $glk->getExtentByTypeRef('SAMPLE', $bid, 1);
        
        if (defined($eid)) {
            $sample_info{$db}{$fs_bid}[EXTENT_ID] = $eid;
            my $info = $glk->getExtentAttributes($eid);
            
            if (exists($info->{blinded_number})) {
                $sample_info{$db}{$fs_bid}[BLINDED_NO] = $info->{blinded_number};
            }
            else {
                $logger->error("Missing blinded_number for sample $bid (Database $db, Collection \"$sample_info{$db}{$fs_bid}[COLLECTION]\") - skipping it");
                next;
            }
            if (exists($info->{species_code})) {
                $sample_info{$db}{$fs_bid}[VIRUS_NAME] = $info->{species_code};
            }
            elsif (exists($info->{virus_name})) {
                $sample_info{$db}{$fs_bid}[VIRUS_NAME] = $info->{virus_name};
            }
            else {
                $logger->error("Missing virus_name and species_code for sample $bid (Database $db, Collection \"$sample_info{$db}{$fs_bid}[COLLECTION]\") - skipping it");
                next;
            }
            if (exists($info->{strain_code})) {
                $sample_info{$db}{$fs_bid}[STRAIN_CODE] = $info->{strain_code};
            }
            else {
                $logger->warn("Missing strain_code for sample $bid (Database $db, Collection \"$sample_info{$db}{$fs_bid}[COLLECTION]\") - Setting it to \"" . NO_STRAIN_CODE . '"');
                $sample_info{$db}{$fs_bid}[STRAIN_CODE] = NO_STRAIN_CODE;
            }
            ## Fishing for accessions in the database
            my $r_children = $glk->getExtentChildren($eid);
            
            foreach my $child (@{$r_children}) {
                my $usable = $glk->getExtentAttribute($child, 'submit_sequence');
                
                if (defined($usable)) {
                    my $seg_name = $glk->getExtentAttribute($child, 'segment_name');
                    
                    unless (defined($seg_name) && $seg_name =~ /\S/) {
                        $logger->logdie("Sample $bid - Segment Extent $child - Missing/empty segment_name attribute.")
                    }
                    $sample_info{$db}{$fs_bid}[ACCESSIONS]{$seg_name} = $glk->hasExtentAttribute($child, 'ncbi_accession') ? $glk->getExtentAttribute($child, 'ncbi_accession') : '';
                }
            }
        }
        else {
            $logger->error("Impossible to find sample $bid in database $db (Collection \"$sample_info{$db}{$fs_bid}[COLLECTION]\") - skipping it.");
            next;
        }
        my $vigor_db = defined($ARGV_vigor_db) ? $ARGV_vigor_db : $db;
        my $work_dir;
        my $sample_data_dir = defined($ARGV_sample_data_root_dir) ? "$ARGV_sample_data_root_dir/$vigor_db/$sample_info{$db}{$fs_bid}[COLLECTION]/$fs_bid" : DEFAULT_SEQ_ROOT . "/$vigor_db/$sample_info{$db}{$fs_bid}[COLLECTION]/$fs_bid";
        
        if (defined($ARGV_work_dir)) {
            $work_dir = abs_path($ARGV_work_dir) . "/$fs_bid/" . $dsa->getAnalysisDir($ARGV_analysis_type);
        }
        else {
            $work_dir = $sample_data_dir . '/' . $dsa->getAnalysisDir($ARGV_analysis_type); ## to be fixed
        }
        mk_tree_safe($work_dir, OUT_DIR_PERMS) unless -d $work_dir;
        my $cmd = FIND_VARS_EXE . " --database $db --collection $sample_info{$db}{$fs_bid}[COLLECTION] --bac_id $fs_bid --analysis_type $ARGV_analysis_type --vigor_db $vigor_db --work_dir $work_dir --min_depth_snps $ARGV_min_depth_snps --min_freq_snps $ARGV_min_freq_snps --min_depth_differences $ARGV_min_depth_differences --debug $ARGV_debug";

        if (defined($ARGV_sample_data_root_dir)) {
            $cmd .= " --sample_data_root_dir $ARGV_sample_data_root_dir";
        }
        if ($ARGV_analysis_type eq 'master_reference' || $ARGV_analysis_type eq 'multiseq_alignment') {
            if (defined($ARGV_reference_seq)) {
                $cmd .= ' --reference_seq '. abs_path($ARGV_reference_seq);
            }
            else {
                $logger->logdie("Missing parameter --reference_seq, which is required with \"$ARGV_analysis_type\" type of analysis");
            }
        }
        elsif ($ARGV_analysis_type ne 'sample_consensus') {
            $logger->logdie("unrecognized type of analysis (\"$ARGV_analysis_type\")")
        }
        $sample_info{$db}{$fs_bid}[RESULTS_DIR] = $work_dir;
        push(@commands, [$fs_bid, $cmd]);
    }
}
my $tot_samples = scalar(@commands);
print "Start processing $tot_samples samples.\n";

## Now running the jobs on the grid or locally.

if ($ARGV_run_locally) {
    &runLocally();
}
else {
    &manageGridJobs();
}

## Post-processing (creating the summary files)
$ARGV_summary_prefix =~ s/_+$//;
my $seg_p_sum_file  = $ARGV_output_dir . '/'. $ARGV_summary_prefix . SEG_POS_SUMMARY  . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps .'x.txt';
#my $seg_p_cnt_file  = $ARGV_output_dir . '/'. $ARGV_summary_prefix . SEG_POS_COUNTS   . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps .'x.csv';
my $var_cl_sum_file = $ARGV_output_dir . '/'. $ARGV_summary_prefix . VAR_CLA_SUMMARY  . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps .'x.txt';
#my $var_cl_res_file = $ARGV_output_dir . '/'. $ARGV_summary_prefix . VAR_CLA_RESULTS  . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps .'x.csv';
my $var_cl_final    = $ARGV_output_dir . '/'. $ARGV_summary_prefix . VAR_CLA_FINAL    . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps .'x.csv';

open(my $spsfh, ">$seg_p_sum_file")  || $logger->logdie("Impossible to open the Segment Position Variations summary file \"$seg_p_sum_file\" for writing.");
#open(my $spcfh, ">$seg_p_cnt_file")  || $logger->logdie("Impossible to open the Segment Position Variations summary file \"$seg_p_cnt_file\" for writing.");
open(my $vcsfh, ">$var_cl_sum_file") || $logger->logdie("Impossible to open the Variant Classifier summary file \"$var_cl_sum_file\" for writing.");
#open(my $vcrfh, ">$var_cl_res_file") || $logger->logdie("Impossible to open the Variant Classifier summary file \"$var_cl_res_file\" for writing.");
open(my $vcffh, ">$var_cl_final")    || $logger->logdie("Impossible to open the Segment Position Variations summary file \"$var_cl_final\" for writing.");

# print {$spcfh} "organism_name,blinded_number,bac_id,segment,segment_position,a_count,c_count,g_count,t_count,major/minor alleles\n";
# print {$vcrfh} "organism_name,blinded_number,bac_id,segment,segment_position,major/minor alleles,transcript,region,impact,aa_position\n";
print {$vcffh} "organism_name,blinded_number,bac_id,Accessions,segment,transcript,AA change,Genome position,AA position,region (SP= structural polyprotein; NSP = nonstructural polyprotein; downstream = 3' UTR; promoter = 5' UTR),A,C,G,T,major/minor alleles,% minor variant,Type of change\n";
my $past_db = START_DB;

foreach my $bid (sort({$a <=> $b}@completed_jobs)) {
    my $db = $sample_db{$bid};
    my $sample_dsa_dir = $sample_info{$db}{$bid}[RESULTS_DIR];
    my $blino = $sample_info{$db}{$bid}[BLINDED_NO];
    my $virus = $sample_info{$db}{$bid}[VIRUS_NAME];
    
    unless (-d $sample_dsa_dir) {
        $logger->error("Unable to find expected working directory $sample_dsa_dir");
        next
    }
    my $var_cla_results = "$sample_dsa_dir/" . $dsa->getVarClassfResultFileName();
    my $st_si_var_pos   = "$sample_dsa_dir/" . $dsa->getStatSignVarPosFileName();
    my $clc_var_pos     = "$sample_dsa_dir/" . $dsa->getClcVarPosFileName();
    
    my %gene_pol = ();
    my %polymorf = ();
    
    foreach my $file ($st_si_var_pos, $clc_var_pos) {
        open(my $posh, $file) || $logger->logdie("Impossible to open position file \"$file\" for reading.");
        
        while (<$posh>) {
            chomp();
            next if /^\s*$/ || /^#/;
            my $min_count;
            my @fields = split /\t/;
            $dsa->cleanupVals(\@fields);
            my ($bacid, $seg_name, $position, $a_count, $c_count, $g_count, $t_count, $majmin) = @fields;
            
            if (defined($bacid) && $bacid =~ /\S/) {
                if ($bid ne $bacid) {
                    $logger->error("Sample $bid - File \"$file\" Line $. (\"$_\") - Mismatch in sample ID: Expected \"$bid\", found \"$bacid\" instead. - Skipping this line.");
                    next
                }
            }
            else {
                $logger->error("Sample $bid - File \"$file\" Line $. (\"$_\") - Missing required field sample ID: (Expected \"$bid\"). - Forcing sample Id to the expected value.");
                $bacid = $bid;
            }
            unless (defined($seg_name) && $seg_name =~ /\S/) {
                $logger->error("Sample $bid - File \"$file\" Line $. (\"$_\") - Missing segment name. - Skipping it");
                next
            }
            my ($major, $minor) = split(/\//, $majmin);
            
            if ($minor eq 'A') {
                $min_count = $a_count;
            }
            elsif ($minor eq 'C') {
                $min_count = $c_count;
            }
            elsif ($minor eq 'G') {
                $min_count = $g_count;
            }
            elsif ($minor eq 'T') {
                $min_count = $t_count;
            }
            else {
                $logger->error("Unable to parse the major/minor variant string (\"$majmin\" - \"$_\") - Skipping this line");
                next
            }
            my $min_perc = sprintf("%5.2f", 100 * $min_count / ($a_count + $c_count + $g_count + $t_count));
            
            if (exists($polymorf{$seg_name}{$position})) {
                my $identical = 1;
                my $msg = '';
                
                if ($polymorf{$seg_name}{$position}[A_COUNT] != $a_count) {
                    $msg .= "Mismatch in the counts of As ($polymorf{$seg_name}{$position}[A_COUNT] vs. $a_count\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[C_COUNT] != $c_count) {
                    $msg .= "Mismatch in the counts of Cs ($polymorf{$seg_name}{$position}[C_COUNT] vs. $c_count\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[G_COUNT] != $g_count) {
                    $msg .= "Mismatch in the counts of Gs ($polymorf{$seg_name}{$position}[G_COUNT] vs. $g_count\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[T_COUNT] != $t_count) {
                    $msg .= "Mismatch in the counts of Ts ($polymorf{$seg_name}{$position}[T_COUNT] vs. $t_count\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[MAJMIN] ne $majmin) {
                    $msg .= "Mismatch in the major/minor allele string ($polymorf{$seg_name}{$position}[MAJMIN] vs. $majmin\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[MINOR] ne $minor) {
                    $msg .= "Mismatch in the identification of the minor allele ($polymorf{$seg_name}{$position}[MINOR] vs. $minor\n";
                    $identical = 0;
                }
                if ($polymorf{$seg_name}{$position}[MIN_PERC] ne $min_perc) {
                    $msg .= "Mismatch in the percentage of the minor allele ($polymorf{$seg_name}{$position}[MIN_PERC] vs. $min_perc\n";
                    $identical = 0;
                }
                unless ($identical) {
                    $logger->error("Significant discrepancies between the two analysis files for sampel $bid, segment \"$seg_name\" at position $position: $msg");
                }
            }
            else {
                $polymorf{$seg_name}{$position} = [$a_count, $c_count, $g_count, $t_count, $majmin, $minor, $min_perc];
            }
        }
        close($posh);
    }
    ## Writing the positions summary file
    foreach my $seg_name (sort(keys(%polymorf))) {
        foreach my $position (sort({$a <=> $b} keys(%{$polymorf{$seg_name}}))) {
#            print {$spcfh} "$virus,$blino,$bid,$seg_name,$position,$polymorf{$seg_name}{$position}[A_COUNT],$polymorf{$seg_name}{$position}[C_COUNT],$polymorf{$seg_name}{$position}[G_COUNT],$polymorf{$seg_name}{$position}[T_COUNT],$polymorf{$seg_name}{$position}[MAJMIN]\n";
            print {$spsfh} "$bid,$seg_name,$position,$polymorf{$seg_name}{$position}[A_COUNT],$polymorf{$seg_name}{$position}[C_COUNT],$polymorf{$seg_name}{$position}[G_COUNT],$polymorf{$seg_name}{$position}[T_COUNT],$polymorf{$seg_name}{$position}[MAJMIN]\n";
        }
    }
    ## Parsing the Variant Classifier files
    open(my $vcrh, $var_cla_results) || $logger->logdie("Impossible to open the CLC Variant Classifier Results file (\"$var_cla_results\") for sample $bid");
    
    while (<$vcrh>) {
        next if /^\s*$/ || /^#/;
        chomp();
        my @pieces = split(/,/, $_, -1); ## A negative limit for split forces it to keep all the trailing empty fields and is interpreted as an arbitrary high limit on the number of fields
        $dsa->cleanupVals(\@pieces);
        my ($bacid, $seg_name, $position, $maj_min, $gene, $region, $effect, $aa_pos) = @pieces;
        
        if (defined($bacid) && $bacid =~ /\S/) {
            if ($bid ne $bacid) {
                $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - Mismatch in sample ID: Expected \"$bid\", found \"$bacid\" instead. - Skipping this line.");
                next
            }
        }
        else {
            $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - Missing required field sample ID: (Expected \"$bid\"). - Forcing sample Id to the expected value.");
            $bacid = $bid;
        }
        if (defined($seg_name) && $seg_name =~ /\S/) {
            unless (exists($polymorf{$seg_name})) {
                $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - The segment name (\"$seg_name\") is not matching the value present in the other results. - Skipping this line.");
                next
            }
        }
        else {
            if (scalar(keys(%polymorf)) == 1) {
                $seg_name = (keys(%polymorf))[0];
                $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - Missing the segment name. Assuming \"$seg_name\".");
            }
            else {
                $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - Missing the segment name. Since the sample has multiple segments, this line will be skept.");
                next
            }
        }
        unless (defined($position) && $position =~ /^\d+$/) {
            no warnings;
            $logger->error("Sample $bid - File \"$var_cla_results\" Line $. (\"$_\") - Missing or invalid sequence position (\"$position\"). - Skipping this line.");
            next
        }
        unless (exists($polymorf{$seg_name}{$position})) {
            $logger->error("File \"$var_cla_results\", Line $. - Unable to find any nucleotide distribution at position $position on segment \"$seg_name\" of sample $bacid: \"$_\" - skipping this line");
            next
        }
        unless ($maj_min eq $polymorf{$seg_name}{$position}[MAJMIN]) {
            $logger->error("File \"$var_cla_results\", Line $. - Mismatch in the string describing the major/minor allele (\"$maj_min\") Using the one present in the nucleotide frequencies files (\"$polymorf{$seg_name}{$position}[MAJMIN]\").");
            $maj_min = $polymorf{$seg_name}{$position}[MAJMIN];
       }
        my $region_summary;
        
        if ($region =~ /EXON.+/){
            $region_summary = "$gene CDS";
        }
        elsif($region =~ /DOWNSTREAM.+/){
            $region_summary = $region;
        }
        elsif($region =~ /PROMOTER.+/){
            $region_summary = $region;
        }
        elsif($region =~ /INTRON.+/){
            $region_summary = $region;
        }
        elsif ($region =~ /\S/) {
            $logger->warn("File \"$var_cla_results\", Line $. (\"$_\") - Unrecognized region type \"$region\" - Skipping it.");
            next
        }
        else {
            $region_summary = DEFAULT_MISSING_REGION;
            $region = DEFAULT_MISSING_REGION;
            $logger->info("File \"$var_cla_results\", Line $. (\"$_\") - Unable to find the region type - Assuming \"$region_summary\".");
        }
        $gene_pol{$seg_name}{$gene}{$position} = [$polymorf{$seg_name}{$position}, $region, $region_summary, $effect, $aa_pos];
    }
    ## Writing the Variant Classifier Results files
    foreach my $seg_name (sort(keys(%gene_pol))) {
        my $accessions = '';
        
        if (exists($sample_info{$db}{$bid}[ACCESSIONS]{$seg_name})){
            $accessions = $sample_info{$db}{$bid}[ACCESSIONS]{$seg_name};
        }
        elsif ($seg_name =~ /^(\S+)_\d+_\d+$/ && exists($sample_info{$db}{$bid}[ACCESSIONS]{$1})) {
            $accessions = $sample_info{$db}{$bid}[ACCESSIONS]{$1};
        }
        
        foreach my $gene (sort(keys(%{$gene_pol{$seg_name}}))) {
            foreach my $position (sort({$a <=> $b} keys(%{$gene_pol{$seg_name}{$gene}}))) {
                my $r_gp = $gene_pol{$seg_name}{$gene}{$position};
                my $r_poly = $r_gp->[POLY_REF];
                my $aa_cng = $r_gp->[EFFECT] =~ /NONSYNONYMOUS\[\w{3}:([A-Z])\s+\w{3}:([A-Z])\s/ ? "$1$r_gp->[AA_POS]$2" : '';
                
                print {$vcsfh} "$bid,$seg_name,$position,$r_poly->[MAJMIN],$gene,$r_gp->[REGION],$r_gp->[EFFECT],$r_gp->[AA_POS]\n";
#                print {$vcrfh} "$virus,$blino,$bid,$seg_name,$position,$r_poly->[MAJMIN],$gene,$r_gp->[REGION],$r_gp->[EFFECT],$r_gp->[AA_POS]\n";
                print {$vcffh} "$virus,$blino,$bid,$accessions,$seg_name,$gene,$aa_cng,$position,$r_gp->[AA_POS],$r_gp->[REGION_SUM],$r_poly->[A_COUNT],$r_poly->[C_COUNT],$r_poly->[G_COUNT],$r_poly->[T_COUNT],$r_poly->[MAJMIN],$r_poly->[MIN_PERC],$r_gp->[EFFECT]\n";
            }
        }
    }
    ## Cleaning up the temporary files...
    unless ($ARGV_keep_work_files) {
        push(@cleanup, [$sample_dsa_dir, $bid]);
    }
}
close($spsfh);
#close($spcfh);
close($vcsfh);
#close($vcrfh);
close($vcffh);

foreach my $location (@cleanup) {
    my ($sample_dsa_dir, $bid) = @{$location};
    $logger->debug("Cleaning up files in sample directory $sample_dsa_dir");
    &cleanupTempFiles($sample_dsa_dir, $bid);
}

my $tot_success = scalar(@completed_jobs);

my $tot_fail = $tot_samples - $tot_success;

if ($tot_fail) {
    print "\n\nWork completed. However, $tot_fail out of $tot_samples jobs errored out.\nFollowing is a list of failed searches\n\n";
    
    my %completed = map({$_ => undef} @completed_jobs);
    my @failed = ();
    
    foreach my $bid (@processed) {
        next if exists($completed{$bid});
        push(@failed, $bid);
    }
    if ($ARGV_run_locally) {
        print "BAC ID\n", join("\n", @failed), "\n\n"; 
    }
    else {
        my @grid_err = ();
        opendir(my $gd, $grid_op_dir) || $logger->logdie("Impossible to access to the grid working directory (\"$grid_op_dir\")");
        
        foreach my $file (readdir($gd)) {
            push(@grid_err, $file) if $file =~ /\.e\d+$/;
        }
        closedir($gd);
        
        foreach my $bid (@failed) {
            my $err_file_root = $job_root_name . "_$bid";
            
            foreach my $err (@grid_err) {
                next unless $err =~ /$err_file_root/;
                print "$bid\t$grid_op_dir/$err\n";
                $logger->error("Failed grid job \"$err_file_root\" Error file: \"$grid_op_dir/$err\"");
                last
            }
        }
        print "\n\n";
    }
}
else {
    print "\n\nDone. Processed successfully all " . scalar(@completed_jobs) . " samples.\n\n";
}

####################################################################################################################################
##------------------------- Subroutines -----------------------------
####################################################################################################################################

sub runLocally {
    foreach my $data (@commands) {
        my ($bid, $cmd) = @{$data};
        $logger->debug("Local Comand: \"$cmd\"");
        
        if (system($cmd)) {
            $logger->error("Problems running the following command: \"$cmd\"");
        }
        else {
            push(@completed_jobs, $bid);
        }
        push(@processed, $bid);
    }
}

sub manageGridJobs {
    unless (defined($ARGV_project_code) && $ARGV_project_code =~ /^\S+$/) {
        $logger->logdie("Argument --project_code is required for launching grid jobs and must not contain spaces.")
    }
    $logger->info("Grid working directory: $grid_op_dir");
    
    unless (-d $grid_op_dir) {
        mk_tree_safe($grid_op_dir, GRID_WD_PERMS);
    }
    
    my %submitted = ();
    
    ## This loop will run until all the jobs have been submitted.
    while (scalar(@commands)) {
        print STDERR ':';
        my $still_running = &checkRunning(\%submitted);
        
        ## We limit the number of concurrent jobs on the grid to what has been defined in the options.
        for (my $n = $still_running; $n < $ARGV_max_grid_jobs; ++$n) {
            my $data = shift(@commands);
            my ($bid, $cmd) = @{$data};
            push(@processed, $bid);
            my $job_name = $job_root_name . "_$bid";
            my $flag_file = $grid_op_dir . "/$$.$bid.job_done";
            my $queue_opt = $ARGV_grid_queue eq 'default' ? '' : " -l $ARGV_grid_queue";
            
           my $qcmd = QSUB_EXE . ' -N ' . $job_name . ' -wd ' . $grid_op_dir . $queue_opt . ' -P ' . $ARGV_project_code . " '$cmd --flag_success $flag_file'";
            $logger->debug("CMD:\"$qcmd\"");
            
            if (system($qcmd)) {
                $logger->error("Problems launching the following grid job:\n\"$qcmd\".");
            }
            else {
                $submitted{$bid} = $flag_file;
                sleep(JOB_SUBMIT_INTERVAL);
            }
            last unless scalar(@commands);
        }
        sleep(JOB_CHECK_INTERVAL);
        $still_running = &checkRunning(\%submitted);
    }
    my $check_cycle = 0;
    my $lost_jobs_alarm_lvl = 0; ## It looks like that sometime qstat doesn't report a job when it is in a transitional state and we need
                            ## also account for any delay for the output file to be visible.
    my $short_job_name = substr($job_root_name, 0, QSTAT_JOB_NAME_LN);
    
    my $qstat_cmd = QSTAT_EXE . " | grep '$short_job_name' | grep -v Eqw";
    
    while (&checkRunning(\%submitted)) {
        print STDERR ".";
        my $running_stuff = `$qstat_cmd`;
        
        if ($running_stuff =~ /\S+/) {
            $lost_jobs_alarm_lvl = 0;
        }
        else {
            ++$lost_jobs_alarm_lvl;
            sleep(FINISHED_JOBS_DELAY);
            my $still_running = &checkRunning(\%submitted);
            
            if ($still_running && $lost_jobs_alarm_lvl > LOST_JOBS_ALARM_CYCLE) { ## grid jobs could have terminated between the check at the beginning of the loop and at this line
                $logger->logdie("Jobs have disappeared from the grid, but didn't terminate correctly (see files in the grid work directory ($grid_op_dir)");
            }
            else {
                last
            }
        }
        sleep(JOB_CHECK_INTERVAL);
    }
    print STDERR "\n";
    
    sub checkRunning {
        my $r_submitted = shift();
        my @just_done = ();
        
        while (my ($bid, $done_file) = each(%{$r_submitted})) {
            if (-e $done_file) {
                push(@just_done, $bid);
            }
        }
        foreach my $bid (@just_done) {
            undef($r_submitted->{$bid});
            delete($r_submitted->{$bid});
        }
        push(@completed_jobs, @just_done);
        return(scalar(keys(%{$r_submitted})));
    }
}

sub cleanupTempFiles {
    my ($sample_dsa_dir, $bid) = @_;

    my @files = ();
    my @dirs = ();
    
    opendir(my $wdh, $sample_dsa_dir) || $logger->logdie("Unable to access the analysis directory for sample $bid \"$sample_dsa_dir\"");
        
    foreach my $file (readdir($wdh)) {
        if (-d "$sample_dsa_dir/$file") {
            push(@dirs, "$sample_dsa_dir/$file") unless $file =~ /^\.{1,2}$/;
        }
        else {
            push(@files, "$sample_dsa_dir/$file");
        }
    }
    closedir($wdh);
    
    foreach my $dir (@dirs) {
        &cleanupTempFiles($dir, $bid);
    }
    foreach my $file (@files) {
        unlink($file) || $logger->error("Unable to delete the following file: \"$file\"");
    }
    unless (rmdir($sample_dsa_dir)) {
        $logger->error("Impossible to delete the followinmg directory: \"$sample_dsa_dir\"." );
        print STDERR `ls -alh $sample_dsa_dir`, "\n\n";
    }
    system("rm -fr $sample_dsa_dir") && $logger->error("Impossible to delete the followinmg directory: \"$sample_dsa_dir\"." );
}
