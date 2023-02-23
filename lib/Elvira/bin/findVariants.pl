#!/usr/local/bin/perl

# File: findVariants.pl
# Author: 
# Created: May 26, 2016
#
# $Author:  $
# $Date: $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# findVariants.pl is the replacement (and, hopefully, the improvement) of Tim Stockwell's "editable shell script"
# used for running deep sequencing variant analysis.

=head1 NAME
    
    findVariants.pl
    
=head1 USAGE

    findVariants.pl [-]-d[atabase] <annotation_database> [-]-collection <collection_name> [-]-bac_id <BAC_ID> [-]-analysis_type <type_of_analysis> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <annotation_database> | -D <annotation_database>

VGD-schema annotation database

=for Euclid:
    annotation_database.type:   string

=item [-]-collection <collection_name>

    Name of the sample's collection.

=for Euclid:
    collection_name.type: string

=item [-]-bac_id <BAC_ID> 

    BAC ID (= Sample ID) of the sample to be analyzed.

=for Euclid:
    BAC_ID.type: string
    
=item [-]-analysis_type <type_of_analysis>

    Type of analysis to be performed.
    List of accepted values:

    sample_consensus
    master_reference
    multiseq_alignment

=for Euclid:
    type_of_analysis.type: string
    
    
=back


=head1 OPTIONS

=over

=item [-]-vigor_db <vigor_database>
    
    Name of the Vigor database to be used for the annotation (by default it uses the same value as --database).

=item [-]-min_depth_snps <min_depth>

    Minimum sequencing depth usable to determine a SNP (default 10)

=for Euclid:
    min_depth.type:    int > 0
    min_depth.default: 10
    
=item [-]-min_freq_snps <min_percent>

    Minimum percentage of base call in order to call a minor variant
    
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

=item [-]-flag_success <success_file_name>

    If this option is specified, the script will create a file with te given name upon successful termination.
    
=for Euclid:
    success_file_name.type: writeable

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

findVariants.pl is the replacement (and, hopefully, the improvement) of Tim Stockwell's "editable shell script" used for running deep sequencing variant analysis.

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
## Commonly used modules (remove whatever doesn't apply):
use Data::Dumper;
use File::Basename;
use File::Path;
use JCVI::Logging::L4pTools;
use ProcessingObjects::SafeIO;

use DSA_Tools;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;

use constant ASMBL_CAS_EXT      => 'hybrid_mapped_to_reference_cutadapt.cas';
use constant ASMBL_COV_MAP_EXT  => 'hybrid_mapped_to_reference_cutadapt_coverage_map.txt';
use constant ASMBL_INFO_EXT     => 'hybrid_mapped_to_reference_cutadapt_assembly_info.txt';
use constant ASSEMBLY_SUBDIR    => '/mapping/consed_with_sanger';
use constant CLASSIFY_SNPS_EXE  => '/usr/local/devel/VIRIFX/software/SNPClassifier/Classify_SNPs.pl';
use constant CLC_ASMBL_EXE      => '/usr/local/bin/clc_ref_assemble_long';
use constant CLC_ASMBL_INFO_EXE => '/usr/local/bin/assembly_info';
use constant CLC_ASMBL_SIMILTY  => .95;
use constant CLC_LICENSE        => '/usr/local/packages/clc-bfx-cell/license.properties';
use constant CLC_VARIANTS_EXT   => 'hybrid_mapped_to_reference_cutadapt_find_variations_f';
use constant CODING_INFO_FILE   => 'coding_info.file';
use constant CUTADAPT_COV_EXT   => 'hybrid_mapped_to_reference_cutadapt_coverage';
use constant CUTADAPT_FQ_EXT    => 'final_merged_cutadapt.fastq';
use constant DEFAULT_DIR_PERM   => 0775;
use constant DEFAULT_LD_LIB     => '/usr/local/packages/python/lib:/usr/local/packages/atlas/lib:/usr/local/packages/boost/lib:/usr/local/packages/gcc/lib64';
use constant DEFAULT_SEARCH_P   => '/usr/local/packages/clc-ngs-cell:/usr/local/packages/clc-bfx-cell:/usr/local/bin:/usr/local/packages/seq454-2.6/bin';
use constant DEFAULT_SEQ_ROOT   => '/usr/local/projdata/700010/projects/VHTNGS/sample_data_new';
use constant FIND_VARIANTS_EXE  => 'FindStatisticallySignificantVariants';
use constant FIND_CLC_VAR_EXE   => '/usr/local/bin/find_variations';
use constant IONTORRENT_SUBDIR  => '/iontorrent';
use constant PARSD_CLC_FV_PFIX  => 'Differences_snps_f';
use constant REF_SEQ_NAME       => 'reference.fasta';
use constant RUBY_TOOLS_PATH    => '/usr/local/devel/VIRIFX/software/Ruby/Tools/Bio';
use constant SOLEXA_SUBDIR      => '/solexa';
use constant SORTTMPDIR         => '/usr/local/scratch/VIRAL/sorttmp';
use constant STAT_SIGN_VAR_EXT  => 'hybrid_mapped_to_reference_cutadapt_FindStatisticallySignificantVariants_f';
use constant STAT_VAR_SNPS_PFIX => 'FindStatisticallySignificantVariants_snps_f';
use constant TEMP_DIR_PATH      => '/usr/local/scratch/VIRAL/clc';
use constant VARIANT_CLASSIFIER => '/usr/local/devel/VIRIFX/software/SNPClassifier/Classify_SNPs.pl';
use constant VIGOR_EXE          => '/usr/local/devel/VIRIFX/software/VIGOR3/prod3/VIGOR3.pl';
use constant VIGOR_OUT          => 'vigor_out';
use constant VIGOR_OUT_CDS      => 'vigor_out.cds';


## Setting the Environment

$ENV{PATH}            = defined($ENV{PATH}) ? DEFAULT_SEARCH_P . ":$ENV{PATH}" : DEFAULT_SEARCH_P;
$ENV{RUBYLIB}         = RUBY_TOOLS_PATH; ## Old path: '/usr/local/devel/DAS/users/tstockwe/Ruby/Tools/Bio';
$ENV{LD_LIBRARY_PATH} = defined($ENV{LD_LIBRARY_PATH}) ? DEFAULT_LD_LIB . ":$ENV{LD_LIBRARY_PATH}" : DEFAULT_LD_LIB;
$ENV{TMP}             = TEMP_DIR_PATH;
$ENV{TMPDIR}          = $ENV{TMP};
$ENV{SORTTMPDIR}      = SORTTMPDIR;

our ($ARGV_database, $ARGV_collection, $ARGV_bac_id, $ARGV_analysis_type, $ARGV_vigor_db, $ARGV_min_depth_snps, $ARGV_min_freq_snps, $ARGV_min_depth_differences, $ARGV_reference_seq,  $ARGV_work_dir, $ARGV_sample_data_root_dir, $ARGV_flag_success, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));
my $dsa = DSA_Tools->new(min_freq_snps => $ARGV_min_freq_snps, min_depth_snps => $ARGV_min_depth_snps, min_depth_diffs => $ARGV_min_depth_differences);

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Initializing various stuff...

my $sample_data_dir;

unless ($ARGV_bac_id =~ /^\d{5,}/) {
    $logger->logdie("Invalid string for --bac_id argument (\"$ARGV_bac_id\")");
}

if (defined($ARGV_sample_data_root_dir)) {
    $sample_data_dir = "$ARGV_sample_data_root_dir/$ARGV_database/$ARGV_collection/$ARGV_bac_id";
}
else {
    $sample_data_dir = DEFAULT_SEQ_ROOT . "/$ARGV_database/$ARGV_collection/$ARGV_bac_id";
}
my $solexa_dir = $sample_data_dir . SOLEXA_SUBDIR;
my $iontor_dir = $sample_data_dir . IONTORRENT_SUBDIR;
my $mapping_dir; 

if (defined($ARGV_work_dir)) {
    unless (-d $ARGV_work_dir) {
        mk_tree_safe($ARGV_work_dir, DEFAULT_DIR_PERM);
    }
    $mapping_dir = $ARGV_work_dir;
}
else {
    $mapping_dir = "$sample_data_dir/" . $dsa->getAnalysisDir($ARGV_analysis_type);
}
$logger->debug("Working directory (\$mapping_dir): \"$mapping_dir\"");

my %segments = ();

my @flu_a = (['PB2', 1],
             ['PB1', 2],
             ['PA',  3],
             ['HA',  4],
             ['NP',  5],
             ['NA',  6],
             ['MP',  7],
             ['NS',  8]);

my @flu_b = (['PB1', 1],
             ['PB2', 2],
             ['PA',  3],
             ['HA',  4],
             ['NP',  5],
             ['NA',  6],
             ['MP',  7],
             ['NS',  8]);

my @rota  = (['VP1',   1],
             ['VP2',   2],
             ['VP3',   3],
             ['VP4',   4],
             ['NSP1',  5],
             ['VP6',   6],
             ['NSP3',  7],
             ['NSP2',  8],
             ['VP7',   9],
             ['NSP4', 10],
             ['NSP5', 11]);
my @arbo  = (['L', 1],
             ['M', 2],
             ['S', 3]);           

## A couple of viral databases are for "general usage" and are not linked specifically to a particular type of virus.
## The following mapping reflect the current situation, an analysis were required on samples in one of these databases.

my $vda_ref  = \@arbo;
my $vffs_ref = \@flu_a;
             
my @unseg = (['MAIN', 1]);

my %db_segs_map = (barda    => \@flu_a,
                   chikv    => \@unseg,
                   dhs      => \@unseg,
                   ebola    => \@unseg,
                   eeev     => \@unseg,
                   entero   => \@unseg,
                   flumb    => \@flu_a,
                   fluutr   => \@flu_a,
                   gcv      => \@unseg,
                   giv      => \@flu_a,
                   giv2     => \@flu_b,
                   giv3     => \@flu_a,
                   givtest  => \@flu_a,
                   hadv     => \@unseg,
                   hpiv1    => \@unseg,
                   hpiv3    => \@unseg,
                   jev      => \@unseg,
                   marburg  => \@unseg,
                   mmp      => \@unseg,
                   mpv      => \@unseg,
                   msl      => \@unseg,
                   norv     => \@unseg,
                   piv      => \@flu_a,
                   rbl      => \@unseg,
                   rsv      => \@unseg,
                   rtv      => \@rota,
                   sapo     => \@unseg,
                   swiv     => \@flu_a,
                   synflu   => \@flu_a,
                   vda      => $vda_ref,
                   veev     => \@unseg,
                   vffs     => $vffs_ref,
                   vzv      => \@unseg,
                   wnv      => \@unseg,
                   yfv      => \@unseg,
                   zikv     => \@unseg
);

unless (exists($db_segs_map{$ARGV_database})) {
    $logger->logdie("Unable to find segment number/name mapping for database \"$ARGV_database\".");
}

## Defining the name of the mapping directory
unless($dsa->isValidAnalysys($ARGV_analysis_type)) {
    $logger->logdie("Invalid type of analysis (\"$ARGV_analysis_type\")")
}

if ($dsa->isExternalRefAnalysis($ARGV_analysis_type) && !defined($ARGV_reference_seq)) {
    $logger->logdie("Missing external reference sequence, required for '$ARGV_analysis_type' analysis type.");
}

$logger->debug("Mapping Dir (\$mapping_dir): \"$mapping_dir\"");

## End of defining the name of mapping directory

my $sample_prefix   = join('_', $ARGV_database, $ARGV_collection, $ARGV_bac_id);

my $ref_seq_path  = $mapping_dir .'/' . REF_SEQ_NAME;

my $clc_lic_copy    = "$mapping_dir/license.properties";
my $cutadapt_fastq  = "$mapping_dir/${sample_prefix}_" . CUTADAPT_FQ_EXT;
my $clc_asmbl_cas   = $sample_prefix . '_' . ASMBL_CAS_EXT;
my $cov_file        = $sample_prefix . '_' . CUTADAPT_COV_EXT;
my $asmbl_info_file = $sample_prefix . '_' . ASMBL_INFO_EXT;
my $cov_map         = $sample_prefix . '_' . ASMBL_COV_MAP_EXT;
my $stat_sign_varnt = $sample_prefix . '_' . STAT_SIGN_VAR_EXT . $ARGV_min_freq_snps;
my $clc_vars_root   = $sample_prefix . '_' . CLC_VARIANTS_EXT . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences;
my $clc_vars_out    = "$clc_vars_root.new_contigs";
my $clc_vars_log    = "$clc_vars_root.log";
my $clc_var_report  = PARSD_CLC_FV_PFIX . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '.output';
my $clc_var_pos     = $dsa->getClcVarPosFileName();
my $st_si_var_out   = "$stat_sign_varnt.variations";
my $st_si_var_pos   = $dsa->getStatSignVarPosFileName();
my $st_si_var_snps  = STAT_VAR_SNPS_PFIX . $ARGV_min_freq_snps . '_' . $ARGV_min_depth_snps . 'x.output';
my $var_cla_results = $dsa->getVarClassfResultFileName();

unless (defined($ARGV_vigor_db)) {
    $ARGV_vigor_db = $ARGV_database;
}

## Creating required directories

unless (-d SORTTMPDIR) {
    mk_tree_safe(SORTTMPDIR, 0777);
    $logger->trace("Created SORTTMPDIR (\"" . SORTTMPDIR . '"');
}
unless (-d $mapping_dir) {
    mk_tree_safe($mapping_dir, 0777);
    $logger->trace("Created mapping dir (\"$mapping_dir\")")
}
unless (-e $clc_lic_copy) {
    my $cmd = 'ln -s ' . CLC_LICENSE . " $clc_lic_copy";
    system($cmd) && $logger->logdie("Impossible to create the symbolic link to the CLC license file (\"$cmd\").");
    $logger->debug("Sym-linked CLC licemse (\"$clc_lic_copy\")");
}

###################################################################################################################
##
## Preparing input files for Variant analysis
##
###################################################################################################################

## Copying the mapping reference to the mapping directory

if ($ARGV_analysis_type eq 'sample_consensus') {
    &copyLastAceConsensus();
}
else {
    copy_safe($ARGV_reference_seq, $ref_seq_path);
    $logger->debug("Copied external consensus sequence ($ARGV_reference_seq) to: \"$ref_seq_path\"");
}
$logger->debug("Creating segment-specific files");
&processSegments($ref_seq_path);

## Changing directory to $mapping_dir
$logger->debug("Changing directory to the mapping directory (\"$mapping_dir\")");
chdir($mapping_dir) || $jlt->logdie("Impossible to change directory to the mapping dir (\"$mapping_dir\")");

## Running Vigor

my $vigor_cmd = VIGOR_EXE . " -d $ARGV_vigor_db -i " . REF_SEQ_NAME . ' -o ' . VIGOR_OUT;
$logger->debug("Running Vigor (\"$vigor_cmd\")");

system($vigor_cmd) && $jlt->die("Problems running Vigor (\"$vigor_cmd\") - \"$!\" - \"$?\"");

## Parsing Vigor output
$logger->debug("Parsing VIGOR output");

&parseVigorOutput();

## Creating the fastq file with all Solexa and IonTorrent reads.
$logger->debug("Creating the fastq file with all Solexa and IonTorrent reads. (\"$cutadapt_fastq\")");

&createGlobalFastq($cutadapt_fastq);

## Running CLC-Bio Mapping assembly
$logger->info("Running Mapping Assembly for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");
my $clc_ass_cmd =  CLC_ASMBL_EXE . ' -s ' . CLC_ASMBL_SIMILTY . " -o $clc_asmbl_cas -q " . basename($cutadapt_fastq) . ' -d ' . REF_SEQ_NAME;
$logger->debug("CMD: \"$clc_ass_cmd\"");
system($clc_ass_cmd) && $logger->logdie("Problems running CLC Reference Assembly (\"$clc_ass_cmd\") - \"$!\" = \"$?\"");

$logger->info("Calculating coverag maps for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");
my $clc_asmbl_info = CLC_ASMBL_INFO_EXE . " -c -n -d $cov_file $clc_asmbl_cas >& $asmbl_info_file";

$logger->debug("Assembly info command: \"$clc_asmbl_info\"");

system($clc_asmbl_info) && $logger->logdie("Problems running CLC Assembly Info (\"$clc_asmbl_info\") - \"$!\" = \"$?\"");

$logger->info("Creating the coverage map for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&createCoverageMap();

## FindStatisticallySignificantVariants
$logger->info("Finding statistically-significant variants for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");
my $find_stat_var_cmd = $FindBin::Bin . '/' . FIND_VARIANTS_EXE . " -cas $clc_asmbl_cas -o $mapping_dir -prefix $stat_sign_varnt -threshold $ARGV_min_freq_snps";
$logger->debug("CMD:\"$find_stat_var_cmd\"");
system($find_stat_var_cmd) && $logger->logdie("Problems running the script for finding statistically-significant variants. (CMD: \"$find_stat_var_cmd\")");

## Find Variation with CLC
$logger->info("Finding variations using CLC find_variations for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");
my $clc_freq = 0.01 * $ARGV_min_freq_snps;
my $find_clc_var_cmd = FIND_CLC_VAR_EXE . " -f $clc_freq -c $ARGV_min_depth_differences -a $clc_asmbl_cas -o $clc_vars_out -v >& $clc_vars_log";
$logger->debug("CMD:\"$find_clc_var_cmd\"");
system($find_clc_var_cmd) && $logger->logdie("Problems running the script for finding statistically-significant variants. (CMD: \"$find_clc_var_cmd\")");

## Parsing CLC find_variation log
$logger->info("Parsing CLC find_variations log for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&parseClcVariations($clc_vars_log);

$logger->info("Parsing FindStatisticallySignificantVariants output for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&parseStatSignVariations($st_si_var_out);

$logger->info("Creating segment-specific merged variant files for sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&uniqueMergeVariationsSplitSegments($clc_var_report, $st_si_var_snps);

$logger->info("Running SNPClassifier on all the segments (with SNPs and coding information) of sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&runSnpClassifier();

$logger->info("Parsing SNPClassifier results of sample $ARGV_database - $ARGV_collection - $ARGV_bac_id.");

&parseAndMergeClassifier();

## Creating the file that signals the successful run (used when launched on the grid)
if (defined($ARGV_flag_success)) {
    open(my $sfh, ">$ARGV_flag_success") || $logger->logdie("Impossible to open the file \"$ARGV_flag_success\" for writing\".");
    print {$sfh} "$$\n";
    close($sfh);
}

###################################################################################################################
##
##          Subroutines
##
###################################################################################################################

sub copyLastAceConsensus {
    my $assembly_dir = $sample_data_dir . ASSEMBLY_SUBDIR;
    opendir(my $assdir, $assembly_dir) || $logger->logdie("Impossible to open the directory containing the assembly consensus (\"$assembly_dir\")");
    my $max_ace = 0;
    my $consensus_file = undef;
    
    foreach my $file (readdir($assdir)) {
        if ($file =~ /\.ace\.(\d+).consensus.fasta$/) {
            my $ace_no = $1;
            
            if ($ace_no >= $max_ace) {
                $max_ace = $ace_no;
                $consensus_file = "$assembly_dir/$file";
            }
        }
    }
    if (defined($consensus_file)) {
        $logger->trace("SRC Consensus (from latest ACE file): \"$consensus_file\"\nTarget File: \"$ref_seq_path\"\n");
        copy_safe($consensus_file, $ref_seq_path);
    }
    else {
        $jlt->die("Unable to find any assembly sequence consensus in the assembly directory (\"$assembly_dir\")");
    }
    closedir($assdir);
}

sub parseVigorOutput {
   open(my $vo, VIGOR_OUT_CDS) || $jlt->die("Impossible to open Vigor output file \"" . VIGOR_OUT_CDS . "\" for reading.");
    open(my $ci, '>' . CODING_INFO_FILE) || $jlt->die("Impossible to open file \"" . CODING_INFO_FILE . "\" for writing.");
    
    my %annot = (); 
    
    while (<$vo>) {
        next unless /^>([^\s\.]+)\.\d+\s/; ## all the mature peptides and possible other elements should have a second '.\d+'
        my $seg_name = $1;
        my $loc_string = undef;
        my $gene_symbl = undef;
        
        if (/\blocation=(\S*)/) {
            $loc_string = $1;
        }
        if (/\bgene="(\S*)"/) {
            $gene_symbl = $1;
        }
        unless (defined($loc_string) && defined($gene_symbl)) {
            chomp();
            $jlt->die("Unable to gather the necessary gene info from Vigor output \"" . VIGOR_OUT_CDS . "\" (\"$_\")");
        }
        my @locations = split(/,/, $loc_string);
        my @exons = ();
        
        foreach my $chunk (@locations) {
            push(@exons, [split(/\.\./, $chunk)]);
        }
        my $gene_start = $exons[0][0] - 1;
        my $gene_end  = $exons[-1][1];
        push(@{$annot{CDS}}, [$seg_name, $gene_symbl, $gene_start, $gene_end]);
        push(@{$annot{SEG}{$seg_name}{CDS}}, [$gene_symbl, $gene_start, $gene_end]);
        
        if (scalar(@exons) > 1) {
            for (my $i = 0; $i < @exons; ++$i) {
                my $ei = $i + 1;
                my $exo_start = $exons[$i][0] - 1;
                push(@{$annot{EXON}}, [$seg_name, $gene_symbl, "$gene_symbl.$ei", $exo_start, $exons[$i][1],1]);
                push(@{$annot{SEG}{$seg_name}{EXON}}, [$gene_symbl, "$gene_symbl.$ei", $exo_start, $exons[$i][1],1]);
            }
        }
        else {
            push(@{$annot{EXON}}, [$seg_name, $gene_symbl, $gene_symbl, $gene_start, $gene_end,1]);
            push(@{$annot{SEG}{$seg_name}{EXON}}, [$gene_symbl, $gene_symbl, $gene_start, $gene_end,1]);
        }
    }
    close($vo);
    
    foreach my $element (qw(EXON CDS)) {
        foreach my $chunk (@{$annot{$element}}) {
            print {$ci} "$element\t", join("\t", @{$chunk}), "\n";
        }
    }
    close($ci);
    $logger->debug("Written the coding info file (\"" . CODING_INFO_FILE . "\").");
    
    ## Writing the segment-specific files...
    
    while (my ($seg_name, $seg_data) = each(%{$annot{SEG}})) {
        my $seg_file = $seg_name . '_' . CODING_INFO_FILE;
        open(my $sfh, ">$seg_file") || $jlt->die("Impossible to open the file \"$seg_file\" for writing.");
        
        foreach my $element (qw(EXON CDS)) {
            foreach my $chunk (@{$seg_data->{$element}}) {
               print {$sfh} "$element\t$seg_name\t", join("\t", @{$chunk}), "\n";
            }
        }
        close($sfh);
        $segments{$seg_name}{CODINFO} =  $seg_file;
        $logger->debug("Written the segment-specific coding info file (\"$seg_file\").");
    }
}

sub createGlobalFastq {
    my $cutadapt_fastq = shift();
    open(my $fq, ">$cutadapt_fastq") || $jlt->die("Impossible to open the fastq file \"$cutadapt_fastq\" for writing.");
    
    ## The following should be validated by Nadia: do we need the constraint for IonTorrent file names? If not, we could wrap everything in a single lop
    
    if (-d $solexa_dir) {
        opendir(my $sold, $solexa_dir) || $jlt->die("Impossible to open the directory containing Solexa sequences (\"$solexa_dir\").");
        
        foreach my $file (readdir($sold)) {
            if ($file =~ /\.fastq$/) {
                open(my $solq, "$solexa_dir/$file") || $jlt->die("Problems opening Solexa fastq file \"$solexa_dir/$file\" for reading.");
                
                while (<$solq>) {
                    print {$fq} $_;
                }
                close($solq);
            }
        }
        closedir($sold);
    }
    if (-d $iontor_dir) {
        opendir(my $itd, $iontor_dir) || $jlt->die("Impossible to open the directory containing IonTorrent sequences (\"$iontor_dir\").");
        
        foreach my $file (readdir($itd)) { ## The following should be validated by Nadia: do we need the constraint for IonTorrent file names?
            if ($file =~ /^1IONJCVI\S+\.fastq$/) {
                open(my $itq, "$iontor_dir/$file") || $jlt->die("Problems opening IonTorrent fastq file \"$iontor_dir/$file\" for reading.");
                
                while (<$itq>) {
                    print {$fq} $_;
                }
                close($itq);
            }
        }
        closedir($itd);
    }
    close($fq);
    unless (-s $cutadapt_fastq) {
        $jlt->die("The fastq file containing all the reads for this sample (\"$cutadapt_fastq\") is empty.");
    }
    $logger->debug("Created the fastq file (\"$cutadapt_fastq\").");
}

sub createCoverageMap {
    open(my $cov, ">$cov_map") || $jlt->die("Impossible to open the coverage map file \"$cov_map\" for writing.");
    
    foreach my $data (@{$db_segs_map{$ARGV_database}}) {
        my $seg_no = sprintf("%03d", $data->[1]);
        my $seg_name = $data->[0];
        my $seg_cov_file = $sample_prefix . '_' . CUTADAPT_COV_EXT . '.' . $seg_no . '.dat';
        
        unless (-f $seg_cov_file) {
            $logger->warn("unable to find segment coverage file \"$seg_cov_file\" - Skipping it.");
            next;
        }
        open(my $seg, $seg_cov_file) || $jlt->die("Impossible to open segment coverage file \"$seg_cov_file\" for reading.");
        
        while (<$seg>) {
            if (/^\s*(\d+)\s+(\d+)\s*$/) {
                my ($position, $coverage) = ($1, $2);
                print {$cov} sprintf("%s %3s %12d   %d\n", $ARGV_bac_id, $seg_name, $position + 1, $coverage);
            }
        }
        close($seg);
        $logger->debug("Created segment coverage file \"$seg_cov_file\".");
    }
    close($cov);
    $logger->debug("Created coverage map(\"$cov_map\").")
}

## Note: there are two differences from Tim's original script. First, instead of printing twice the major allele, 
## we print botrh major and minor allele in the position file and the subject/reference alleles in the variations report.

sub parseClcVariations {
    my $clc_log = shift();
    open(my $clc, $clc_log) || $jlt->die("Impossible to open CLC find_variants log file (\"$clc_log\") for reading.");
    open (my $cpos, ">$clc_var_pos") || $jlt->die("Impossible to open file \"$clc_var_pos\" for writing.");
    open (my $cvar, ">$clc_var_report") || $jlt->die("Impossible to open file \"$clc_var_report\" for writing.");
    
    ## Grabbing the name of the reference sequence
    my $asmbl_ref = undef;
    
    while (<$clc>) {
        if (/^(\S+)/) {
            $asmbl_ref = $1;
        }
        elsif (/^\s+(\d+)\s+(?:Difference|Nochange)\s+([ACGTN])\s+->\s+([ACGTN])\s+A:\s+(\d+)\s+C:\s+(\d+)\s+G:\s+(\d+)\s+T:\s+(\d+)\s+N:\s+\d+/) {
            my ($pos, $ref_nt, $sbj_nt, $a_no, $c_no, $g_no, $t_no) = ($1, $2, $3, $4, $5, $6, $7);
            
            unless (defined($asmbl_ref)) {
                $logger->logdie("Unable to find the assembly reference in CLC log file \"$clc_log\"")
            }   
            ## Getting the two major variants
            my %basecall = (A => $a_no,
                            C => $c_no,
                            G => $g_no,
                            T => $t_no);
            my $n = 0;
            my $tot_cov = 0;
            my @maj_min = ();
    
            foreach my $nt (sort({$basecall{$b} <=> $basecall{$a}} keys(%basecall))) {
                if ($n < 2) {
                    push(@maj_min, [$nt, $basecall{$nt}]);
                }
                $tot_cov += $basecall{$nt};
                $n++;
            }
            if ($tot_cov < $ARGV_min_depth_snps || $maj_min[0][1] - $maj_min[1][1] < $ARGV_min_depth_differences || $ref_nt eq $sbj_nt && 100 * $maj_min[1][1] / $tot_cov < $ARGV_min_freq_snps) {
                next;
            }
            print {$cpos} "$ARGV_bac_id\t$asmbl_ref\t$pos\t$a_no\t$c_no\t$g_no\t$t_no\t$maj_min[0][0]/$maj_min[1][0]\n";
            print {$cvar} "$ARGV_bac_id\t$asmbl_ref\t", $pos - 1, "\t$pos\t1\t$maj_min[0][0]/$maj_min[1][0]\n";
       }
    }
    close($clc);
    close($cpos);
    close($cvar);
    $logger->debug("Written the CLC variation position file (\"$clc_var_pos\").");
    $logger->debug("Written the CLC variation reprot file (\"$clc_var_report\").");
}

sub parseStatSignVariations {
    my $stat_sign_variations = shift();
    open(my $stat, $stat_sign_variations) || $jlt->die("Impossible to open the FindStatisticallySignificantVariants output file (\"$stat_sign_variations\") for reading");
    open(my $pos, ">$st_si_var_pos") || $jlt->die("Impossible to open the file \"$st_si_var_pos\" for writing");
    open(my $snp, ">$st_si_var_snps")|| $jlt->die("Impossible to open the file \"$st_si_var_snps\" for writing");
    
    while (<$stat>) {
        if (/^#/) {
            next;
        }
        chomp();
        my ($asmbl_ref, $position, $a_no, $c_no, $g_no, $t_no) = split /\t/; 
        my $tot_cov = 0;
        my @top2 = ();
        my %nts = (A => $a_no,
                   C => $c_no,
                   G => $g_no,
                   T => $t_no);
        
        foreach my $nt (sort({$nts{$b} <=> $nts{$a}} keys(%nts))) {
            push(@top2, [$nt, $nts{$nt}]);
            $tot_cov += $nts{$nt};
        }
        if ($tot_cov < $ARGV_min_depth_snps) {
            next;
        }
        
        my $zero_pos = $position - 1;
        print {$pos} "$ARGV_bac_id\t$asmbl_ref\t$position\t$a_no\t$c_no\t$g_no\t$t_no\t$top2[0][0]/$top2[1][0]\n";
        print {$snp} "$ARGV_bac_id\t$asmbl_ref\t$zero_pos\t$position\t1\t$top2[0][0]/$top2[1][0]\n";
    }
    close($stat);
    close($pos);
    close($snp);
    $logger->debug("Written the Stat. Significant variation position file (\"$st_si_var_pos\").");
    $logger->debug("Written the Stat. Significant variation reprot file (\"$st_si_var_snps\").");
}

sub processSegments {
    my $fasta_file = shift();
    local $/ = "\n>";
    
    open(my $ff, $fasta_file) || $jlt->die("Unable to open the fasta file \"$fasta_file\" for reading.");

    while (<$ff>) {
        chomp();
        s/^>//;
        s/\n$//;
        my ($hdr, @seq) = split /\n/;
        next unless $hdr =~ /^(\S+)/;
        my $seg_name = $1;
        my $seg_file = "$mapping_dir/${seg_name}_" . REF_SEQ_NAME; 
        open(my $seg_fh, ">$seg_file") || $jlt->die("Impossible to create segment Fasta file \"$seg_file\".");
        print {$seg_fh} ">$seg_name\n", join("\n", @seq), "\n";
        close($seg_fh);
        $segments{$seg_name}{FSA} = $seg_file;
        $logger->debug("Segment Fasta file: \"$seg_file\"");
    }
    close($ff);
    
    unless(scalar(keys(%segments))) {
        $jlt->die("Impossible to parse correctly fasta file \"$fasta_file\" - Unable to find segment names.");
    }
}

sub uniqueMergeVariationsSplitSegments {
    my ($clc_report, $stat_s_report) = @_;
    ## Both files have the same format: BAC_ID<tab>Segment_Name<tab>position<tab>position_plus_one<tab>1<tab>major/minor_allele
    ## Since the two files are treated equally, we parse one after the other, removing any redundancy and making sure that they are consistent.
    my %seg_snps = ();
    my $good = 1;
    
    foreach my $report ($clc_report, $stat_s_report) {
        open(my $rfh, $report) || $jlt->die("Impossible to open the file \"$report\" for reading.");
        $logger->debug("Now parsing report \"$report\".");
        
        while (<$rfh>) {
            chomp();
            next if /^\s*$/ || /^#/;
            my ($bid,$seg,$pos,$alls) = (split /\t/)[0..2,5];
            
            unless (defined($bid) && defined($seg) && defined($pos) && defined($alls)) {
                print STDERR "Report: \"$report\", Line $. - Unable to parse the following line:\n\"$_\"\nBID: $bid  SEG: \"$seg\"  POS: $pos  ALLS: \"$alls\"\n\n";
                next;
            }
            
            unless ($ARGV_bac_id =~ /^$bid/) {
                $jlt->die("File \"$report\" contains SNP information of a different sample ($bid) - Expecting $ARGV_bac_id instead.")
            }
            if (exists($seg_snps{$seg}) && exists($seg_snps{$seg}{$pos})) {
                unless ($alls eq $seg_snps{$seg}{$pos}) {
                    $logger->error("Sample $bid - Segment $seg - Different allele call at position $pos between file \"$clc_report\" and \"$stat_s_report\".");
                    $good = 0;
                }
            }
            else {
                $seg_snps{$seg}{$pos} = $alls;
            }
        }
        close($rfh);
    }
    unless ($good) {
        $jlt->die("too many mismatches in processing the SNP reports. Quitting now.")
    }
    ## Writing the condensed report to the segment-specific files...
    
    foreach my $seg (keys(%seg_snps)) {
        my $seg_snp_file = $seg . '_snps_f' . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps . 'x.output';
        
        open(my $ssf, ">$seg_snp_file") || $jlt->die("Impossible to open the file \"$seg_snp_file\" for writing.");
        
        foreach my $pos (sort({$a <=> $b} keys(%{$seg_snps{$seg}}))) {
            print {$ssf} "$pos\t", $pos + 1, "\t1\t$seg_snps{$seg}{$pos}\n";
        }
        close($ssf);
        $segments{$seg}{SNP} = $seg_snp_file;
        $logger->debug("Written segment SNPs file \"$seg_snp_file\".");
    }
}

sub runSnpClassifier {
    foreach my $seg (keys(%segments)) {
        
        $logger->trace("Now processing Segment: \"$seg\".");
        
        if (!exists($segments{$seg}{SNP})) {
            $logger->warn("Sample $ARGV_bac_id - Not found any SNP info for segment \"$seg\" - Skipping it");
            next
        }
        elsif (!exists($segments{$seg}{CODINFO})) {
            $logger->warn("Sample $ARGV_bac_id - Not found any coding info for segment \"$seg\" - Skipping it");
            next
        }
        my $seg_class_file = $seg . '_variant_classifier_' . $ARGV_min_freq_snps . '_c' . $ARGV_min_depth_differences . '_' . $ARGV_min_depth_snps . 'x';
        my $cmd = CLASSIFY_SNPS_EXE . " -s $segments{$seg}{SNP} -c $segments{$seg}{CODINFO} -n $segments{$seg}{FSA} -o $seg_class_file";
        #Classify_SNPs.pl -s MAIN_3_11026_snps_f3_c10_10x.output -c MAIN_3_11026_coding_info.file -n /usr/local/scratch/VIRAL/Paolo/DeepS/Zika_63395/sample_consensus_mapping/MAIN_3_11026_reference.fasta -o MAIN_3_11026_variant_classifier_3_c10_10x
        system($cmd) && $jlt->die("Problems running the following command: \"$cmd\"");
        $segments{$seg}{CLASSIF} = $seg_class_file;
        $logger->debug("Ran SNP Classifier. Results in files \"$seg_class_file.*\".");
    }
}

sub parseAndMergeClassifier {
    open(my $vcfh, ">$var_cla_results") || $jlt->die("Impossible to open the file \"$var_cla_results\" for writing.");
    my %classified = ();
    
    foreach my $seg (keys(%segments)) {
        unless (exists($segments{$seg}{CLASSIF})) {
            $logger->warn("No classifier results for segment \"$seg\".");
            
            print STDERR Dumper($segments{$seg}), "\n\n";
            
            next
        }
        open(my $clfh, "$segments{$seg}{CLASSIF}.denormal") || $jlt->die("Impossible to open the file \"$segments{$seg}{CLASSIF}.denormal\" for reading");
        
        while (<$clfh>) {
            chomp();
            my @tmp = split /\t/;
            my ($poswhat, $seg, $gene, $snp_reg, @other_fields) = @tmp[0,4..6, 7..$#tmp];
            my ($ref_loc, $maj_min) = ($poswhat =~ /^\d+-(\d+):\d+\:([ACGT]\/[ACGT])/);
            my $cds_impact = '';
            my $aa_loc = '';
            
            unless (defined($ref_loc) && defined($maj_min)) {
                $jlt->die("Unable to parse the first field (\"$poswhat\") of the input file \"$segments{$seg}{CLASSIF}.denormal\".")
            }
            $seg =~ s/^\s+//;
            $seg =~ s/\s+$//;
            $gene =~ s/^\s+//;
            $gene =~ s/\s+$//;
            $snp_reg =~ s/^\s+//;
            $snp_reg =~ s/\s+$//;
                
            if (scalar(@other_fields)) { # It's coding sequence
                $cds_impact = $other_fields[1];
                ($aa_loc) = ($other_fields[2] =~ /\d+-(\d+)/);
                
                unless (defined($aa_loc)) {
                    $jlt->die("unable to properly parse the last field (\"$other_fields[1]\") of the input file \"$segments{$seg}{CLASSIF}.denormal\" Line $.\n" . join("\n", @other_fields))
                } 
            }
            my @fields = ($maj_min, $snp_reg, $cds_impact, $aa_loc);
            
            if (exists($classified{$seg}{$ref_loc}{$gene})) {
                $logger->warn("Sample $ARGV_bac_id - Segment: \"$seg\" - Gene: \"$gene\" - Position: $ref_loc - Duplicated record. Found first \"" . join('", "', @{$classified{$seg}{$ref_loc}{$gene}}) . "\" vs. \"" . join('", "', @fields) .'" - skipping the past record');
            }
            else {
                $classified{$seg}{$ref_loc}{$gene} = \@fields;
            }
        }
    }
    foreach my $seg (keys(%classified)) {
        foreach my $ref_loc (sort({$classified{$seg}{$a} <=> $classified{$seg}{$b}} keys(%{$classified{$seg}}))) {
            foreach my $gene (sort({$classified{$seg}{$ref_loc}{$a} cmp $classified{$seg}{$ref_loc}{$b}} keys(%{$classified{$seg}{$ref_loc}}))) {
                my ($maj_min, $snp_reg,$cds_impact, $aa_loc) = @{$classified{$seg}{$ref_loc}{$gene}};
                print {$vcfh} "$ARGV_bac_id,$seg,$ref_loc,$maj_min,$gene,$snp_reg,$cds_impact,$aa_loc\n";
            }
        }
    }
    close($vcfh);
    $logger->debug("Written all the unique variations in \"$var_cla_results\".");
}
