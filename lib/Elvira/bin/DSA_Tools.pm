# File: DSA_Tools.pm
# Author: pamedeo
# Created: August, 25 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2016, J. Craig Venter Institute
#
# DSA_Tools.pm 



package DSA_Tools;

use strict;
use warnings;
# use FindBin;

# Commonly used modules (delete whatever you don't need)
# use Data::Dumper;
#use File::Basename;
#use File::Copy;
#use File::Path;
use JCVI::Logging::L4pTools;
#use JCVI::DB_Connection::VGD_Connect;
#use JCVI::UnicodeTools::Transliterate;

#use Cwd (qw(abs_path));

## JCVI Modules
# use TIGR::GLKLib;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;

use constant ANALYSIS_DIR => 0;
use constant EXTERNAL_REF => 1;

use constant MIN_DEPTH_SNPS  => 10;
use constant MIN_FREQ_SNPS   => 3;
use constant MIN_DEPTH_DIFFS => 10;

use constant CLC_VAR_POS_PFIX   => 'Differences_bac_seg_pos_counts_maj_min_f';
use constant STAT_VAR_POS_PFIX  => 'FindStatisticallySignificantVariants_bac_seg_pos_counts_maj_min_f';
use constant VAR_CLA_RES_PFIX   => 'var_cla_results_reformat_f';


my $jlt = JCVI::Logging::L4pTools->new();

=head1  NAME DSA_Tools

This library is a collection of tools to handle different tasks in the Deep Sequencing Analysis pipeline.

=cut

my %analysis_type = (master_reference   => ['master_reference_mapping', TRUE],
                     sample_consensus   => ['sample_consensus_mapping', FALSE],
                     multiseq_alignment => ['msa_consensus_reference_mapping', TRUE]);




=head2 new();

    my $dsa_obj = DSA_Tools->new();
    my $dsa_obj = DSA_Tools->new(min_freq_snps => $min_frq, min_depth_snps => $min_depth, min_depth_diffs => $min_diffs);
    
It creates and initializes the object. It takes three optional pairs of arguments in key-value fashion:
min_freq_snps
min_depth_snps
min_depth_diffs
   
=cut

sub new {
    my $class = shift();
    my $logger = $jlt->getLogger(ref($class));
    $logger->trace("Creating a new object");
    my $self = {min_freq_snps   => MIN_FREQ_SNPS,
                min_depth_snps  => MIN_DEPTH_SNPS,
                min_depth_diffs => MIN_DEPTH_DIFFS,
                @_};
    return(bless($self, $class));  
}

=head2 isValidAnalysys()

    my $true_false = $dsa_obj->isValidAnalysys($analysis_type);
    
    Given a type of analysis, it returns TRUE if the analysis type is one among the recognized analysis. FALSE otherwise.
    
=cut

sub isValidAnalysys {
    my ($self, $anal_type) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return(exists($analysis_type{$anal_type}));
}

=head2 getAnalysisDir()

    my $analysis_dir = $dsa_obj->getAnalysisDir($analysis_type)
    
    Given a type of analysis, it return the standard name of the work directory for that type.
    
=cut

sub getAnalysisDir {
    my ($self, $anal_type) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return($analysis_type{$anal_type}[ANALYSIS_DIR])
}

=head2 isInternalRefAnalysis()

    my $true_false = $dsa_obj->isInternalRefAnalysis($analysis_type)
    
    Given a type of analysis, it returns TRUE if the analysis is performed against an external reference, FALSE otherwise.
    
=cut

sub isExternalRefAnalysis {
    my ($self, $anal_type) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return($analysis_type{$anal_type}[EXTERNAL_REF])
}

sub getVarClassfResultFileName {
    my ($self) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return(VAR_CLA_RES_PFIX . $self->{min_freq_snps} . '_c' . $self->{min_depth_diffs} . '_' . $self->{min_depth_snps} . 'x.txt')
}

sub getStatSignVarPosFileName {
    my ($self) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return(STAT_VAR_POS_PFIX . $self->{min_freq_snps} . '_' . $self->{min_depth_snps} . 'x.txt')
}

sub getClcVarPosFileName {
    my ($self) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering/Exiting');
    return(CLC_VAR_POS_PFIX  . $self->{min_freq_snps} . '_c' . $self->{min_depth_diffs} . '.txt')
}

sub cleanupVals {
    my ($self, $r_values) = @_;
    my $logger =  $jlt->getLogger(ref($self));
    $logger->trace('Entering');
    
    foreach my $piece (@{$r_values}) {
        $piece =~ s/^\s+//;
        $piece =~ s/\s+$//;
    }
    $logger->trace('Exiting');
}

1;