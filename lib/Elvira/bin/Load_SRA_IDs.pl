#!/usr/local/bin/perl

# File: Load_SRA_IDs.pl
# Author: Paolo Amedeo
# Created: July 12, 2017
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL:  $
#
# Copyright 2017, J. Craig Venter Institute
#
# Load_SRA_IDs.pl is the script used for loading in the database all SRA-associated IDs.
# It takes the tab-separated-table grouping all the samples in the "SRA Study" downloaded 
# from SRA and the name of the target database, and it produces a similar table with all the
# ignored samples, if any.
# The script requires columns with the following headers (any other column will be reproduced
# in the output file respecting the original order):
# BioSample Experiment Run SRA_Sample SRA_Study

=head1 NAME
    
    Load_SRA_IDs.pl
    
=head1 USAGE

    Load_SRA_IDs.pl [-]-d[atabase] <annotation_database> [-]-s[tudy[_file]] <tsv_SRA_report> [-]-o[ut[put_file]] <leftover_tsv>

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <annotation_database> | -D <annotation_database>

    VGD-schema annotation database

=for Euclid:
    annotation_database.type:   string

=item [-]-s[tudy[_file]] <tsv_SRA_report>

    Tab-delimited file downloaded from SRA "SRA Run Selector page" (https://www.ncbi.nlm.nih.gov/Traces/study/)
    Required column headers: BioSample Experiment Library_Name Run SRA_Sample SRA_Study
    
=for Euclid:
    tsv_SRA_report.type: readable
    
=item [-]-o[ut[put_file]] <leftover_tsv>

    Tab-delimited file with all the rows of samples that haven't been loaded into the current database.
    The file will be empty (i.e. even without the headers) if there is no leftover record.

=for Euclid:
    leftover_tsv.type: writeable

=back

=head1 OPTIONS

=over

=item [-]-overwrite

    By default the program would skip the records already present (but different) in the database. Using this option it will replace them, instead.

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

Load_SRA_IDs.pl is the script used for loading in the database all SRA-associated IDs.
It takes the tab-separated-table grouping all the samples in the "SRA Study" downloaded from SRA and the name of the target database, and it produces a similar table with all the ignored samples, if any.

The script requires columns with the following headers (any other column will be reproduced in the output file respecting the original order):
BioSample Experiment Run SRA_Sample SRA_Study
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
use File::Copy;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use ProcessingObjects::SafeIO;
use TIGR::GLKLib;

## Constants declaration
#
use constant SUCCESS   => 1;
use constant FAILURE   => 0;
use constant SYS_ERROR => 1;
use constant TRUE      => 1;
use constant FALSE     => 0;

use constant BIOSAMPLE_ATTR  => 'biosample_id';
use constant TEMP_SRA_VAL    => 'SRA_Submission_Pending';


our ($ARGV_database, $ARGV_study_file, $ARGV_output_file, $ARGV_overwrite, $ARGV_dont_load, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");

## Initializing various stuff...

my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => $ARGV_database, server => $ARGV_server, pass_file => $ARGV_password_file);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

my %field = (BioSample    => undef,
             Experiment   => undef,
             Library_Name => undef,
             Run          => undef,
             SRA_Sample   => undef,
             SRA_Study    => undef);

my $sra_study_attr   =  'sra_study_id';
my $sra_sample_attr  = 'sra_sample_id';
my $sra_library_attr = 'sra_library_ids';


## Opening all the files

open(my $in, $ARGV_study_file)      || $logger->logdie("Impossible to open the SRA Study file \"$ARGV_study_file\" for reading");

chomp(my $header_line = <$in>);

my @headers = split(/\t/, $header_line);

for (my $n = 0; $n < @headers; ++$n) {
    if (exists($field{$headers[$n]})) {
        $field{$headers[$n]} = $n;
    }
}
## Making sure that we have all the required headers
my @missing = ();

while (my ($hdr, $no) = each(%field)) {
    push(@missing, $hdr) unless defined($no);
}
if (scalar(@missing)) {
    $logger->logdie("Impossible to process file \"$ARGV_study_file\". Missing the following header(s): \"" . join('", "', @missing) . '"')
}
## We go through the file twice, first we collect all the data, then we go through again if we need to write leftover records

my %samples = ();
my %skip = ();
my %already_there = ();

while (<$in>) {
    next if /^\s*$/;
    chomp();
    my @tmp = split /\t/;
    my ($bs_id, $exp_id, $run_id, $sra_id, $lib_name, $study_id) = @tmp[$field{BioSample},$field{Experiment},$field{Run},$field{SRA_Sample},$field{Library_Name},$field{SRA_Study}];
    
    unless (defined($bs_id) && $bs_id =~ /\S/ && defined($exp_id) && $exp_id =~ /\S/ && defined($run_id) && $run_id =~ /\S/ && defined($sra_id) && $sra_id =~ /\S/ && defined($lib_name) && $lib_name =~ /\S/ && defined($study_id) && $study_id =~ /\S/) {
        $logger->error("Empty/missing required field at line $. of file $ARGV_study_file (\"$_\"). - Skipping it");
        next
    }
    my $sra_library_str = join(':', $exp_id, $lib_name, $run_id);
    
    if (exists($samples{$bs_id}{$sra_study_attr})){
        if ($samples{$bs_id}{$sra_study_attr} ne $study_id) {
            warn("unexpected multiple values of SRA_Study for sample $bs_id - Skipping this sample.");
            undef($skip{$bs_id});
            next
        }
    }
    else {
        $samples{$bs_id}{$sra_study_attr} = $study_id;
    }
    if (exists($samples{$bs_id}{$sra_sample_attr})) {
        if ($samples{$bs_id}{$sra_sample_attr} ne $sra_id) {
           warn("unexpected multiple values of SRA_Sample for sample $bs_id - Skipping this sample.");
            undef($skip{$bs_id});
            next 
        }
    }
    else {
        $samples{$bs_id}{$sra_sample_attr} = $sra_id;
    }
    push(@{$samples{$bs_id}{$sra_library_attr}}, $sra_library_str);
}
## Now searching in the database for the samples by BioSample_id and gathering any of the relevant attributes
my %sample_eid = ();

foreach my $bs_id (keys(%samples)) {
    my $eid = $glk->getExtentByAttribute(BIOSAMPLE_ATTR, $bs_id, TRUE); ## Calling it in "strict mode"
    
    if (defined($eid)) {
        $sample_eid{$bs_id} = $eid;
    }
    else {
        undef($skip{$bs_id});
    }
}
## In the case we do not find a single sample in the database...
unless (scalar(keys(%sample_eid))) {
    copy($ARGV_study_file, $ARGV_output_file);
    $logger->warn("Not a single sample in SRA file \"$ARGV_study_file\" has been found in database $ARGV_database.");
    exit(SYS_ERROR);
}

## Gathering the attributes from all the existing samples and comparing them with the values in the study file

while (my ($bs_id, $eid) = each(%sample_eid)) {
    my $r_attrs = $glk->getExtentAttributes($eid);
    
    if (exists($r_attrs->{$sra_study_attr}) && $r_attrs->{$sra_study_attr} ne TEMP_SRA_VAL) {
        unless ($ARGV_overwrite) { ## if we use --overwrite, we do not investigate what we have and what we don't, since it's likely that something went wrong during the previous loading attempt.
            ## Checking first for the existance of all three attributes
            my $ok = TRUE;
            
            unless (exists($r_attrs->{$sra_sample_attr})) {
                $logger->error("BioSample \"$bs_id\" (Extent $eid) - Data corruption: has defined \"$sra_study_attr\" attribute (\"$r_attrs->{$sra_study_attr}\") but misses \"$sra_sample_attr\" attribute.");
                $ok = FALSE;
            }
            unless (exists($r_attrs->{$sra_library_attr})) {
                $logger->error("BioSample \"$bs_id\" (Extent $eid) - Data corruption: has defined \"$sra_study_attr\" attribute (\"$r_attrs->{$sra_study_attr}\") but misses \"$sra_library_attr\" attribute.");
                $ok = FALSE;
            }
            unless ($ok) {
                $logger->error("Skipping this sample for too many errors.");
                undef($skip{$bs_id});
                next
            }
        }
    }
    elsif (exists($r_attrs->{$sra_sample_attr}) || exists($r_attrs->{$sra_library_attr})) {
        unless ($ARGV_overwrite) {
            $logger->error("BioSample \"$bs_id\" (Extent $eid) - Data corruption: it doeesn't have attribute \"$sra_study_attr\" but has \"$sra_sample_attr\" and/or \"$sra_library_attr\" - skipping it.");
            undef($skip{$bs_id});
            next
        }
    }
    else { ## If it doesn't have any of the attributes or has just the placeholder value in SRA study, we do not need any further check
        next
    }
    if ($r_attrs->{$sra_study_attr} eq $samples{$bs_id}{$sra_study_attr}) {
        $logger->info("BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_study_attr\" set to the correct value (\"$samples{$bs_id}{$sra_study_attr}\")");
        undef($already_there{$bs_id}{$sra_study_attr});
    } 
    else {
        my $msg = "BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_study_attr\", but set to a different value (DB: \"$r_attrs->{$sra_study_attr}\" v.s. RSA Study file: \"$samples{$bs_id}{$sra_study_attr}\").";
        if ($ARGV_overwrite) {
            $logger->info("$msg - Overwriting it");
        }
        else {
            $logger->warn("$msg - skipping this sample");
            undef($skip{$bs_id});
            next
        }
    }
    if ($r_attrs->{$sra_sample_attr} eq $samples{$bs_id}{$sra_sample_attr}) {
     $logger->info("BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_sample_attr\" set to the correct value (\"$samples{$bs_id}{$sra_sample_attr}\")");
        undef($already_there{$bs_id}{$sra_sample_attr});
    } 
    else {
        my $msg = "BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_sample_attr\", but set to a different value (DB: \"$r_attrs->{$sra_sample_attr}\" v.s. RSA Study file: \"$samples{$bs_id}{$sra_sample_attr}\").";
        if ($ARGV_overwrite) {
            $logger->info("$msg - Overwriting it");
        }
        else {
            $logger->warn("$msg - skipping this sample");
            undef($skip{$bs_id});
        }
    }
    my $equivalent = &compareLibs($r_attrs->{$sra_library_attr}, $samples{$bs_id}{$sra_library_attr});
    
    if ($equivalent) {
        $logger->info("BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_library_attr\" set to the correct value (\"$samples{$bs_id}{$sra_library_attr}\")");
        undef($already_there{$bs_id}{$sra_library_attr});
    } 
    else {
        my $msg = "BioSample \"$bs_id\" (Extent $eid) has already attribute \"$sra_library_attr\", but set to a different value (DB: \"$r_attrs->{$sra_library_attr}\" v.s. RSA Study file: \"$samples{$bs_id}{$sra_library_attr}\").";
        if ($ARGV_overwrite) {
            $logger->info("$msg - Overwriting it");
        }
        else {
            $logger->warn("$msg - skipping this sample");
            undef($skip{$bs_id});
        }
    }
}
## Now updating the database...
my ($tot_smpls, $tot_attrs) = (0) x 2;

while (my ($bs_id, $eid) = each(%sample_eid)) {
    next if exists($skip{$bs_id});
    my $updated = 0;
    
    unless (defined($eid)) {
        $logger->logdie("Unexpected event: unable to find the Extent_id for BioSample \"$bs_id\" after supposedly having queried the database.")
    }
    foreach my $attr_type ($sra_study_attr, $sra_sample_attr, $sra_library_attr) {
        next if exists($already_there{$bs_id}) && exists($already_there{$bs_id}{$attr_type});
        my $value = $attr_type eq $sra_library_attr ? join(';', @{$samples{$bs_id}{$attr_type}}) : $samples{$bs_id}{$attr_type};
        
        if ($ARGV_dont_load) {
            $logger->info("BioSample \"$bs_id\" (Extent_id $eid) - simulating the insertion/updating of attribute \"$attr_type\" (value: \"$value\").");
        }
        else {
            unless ($glk->setExtentAttribute($eid, $attr_type, $value)) {
                $logger->error("Problems inserting/updating attribute \"$attr_type\" for sample \"$bs_id\" (Extent_id $eid).");
                undef($skip{$bs_id});
            }
        }
        ++$updated;
        ++$tot_attrs;
    }
    ++$tot_smpls if $updated;
}

## If we have leftover samples that cannot be processed...
my $ln_written = 0;
my $skipped = scalar(keys(%skip));
my $already_loaded = scalar(keys(%already_there));

if ($skipped) { 
    open(my $out, ">$ARGV_output_file") || $logger->logdie("Impossible to open the leftovers file \"$ARGV_output_file\" for writing.");
    seek($in, 0, 0);
    my $header_line = <$in>;
    print {$out} $header_line;
    
    while (<$in>) {
        my $bs_id = (split /\t/)[$field{BioSample}];
        
        if (exists($skip{$bs_id})) {
            print {$out} $_;
            ++$ln_written;
        }
    }
    close($out);
}
else {
    my $cmd = "touch $ARGV_output_file";
    system($cmd) && $logger->error("Problems executing the following command: \"$cmd\"");
}
close($in);
    
my $summary = $ARGV_dont_load ? "Simulated the processing of $tot_smpls samples and the uploading of $tot_attrs attributes.\n\n" : 
                                "Processed $tot_smpls samples and uploaded $tot_attrs attributes.\n\n";
$summary .= "$skipped samples were ignored and $ln_written rows of data written in the output file \"$ARGV_output_file\".\n\n" if $skipped;
$summary .= "$already_loaded samples had already the proper attributes in the database, therefore they were ignored.\n\n" if $already_loaded;
print "Done\n\n$summary";

sub compareLibs {
    my ($attr_string, $r_libs) = @_;
    
    my @existing = sort(split(/;/, $attr_string));
    my @proposed = sort(@{$r_libs});
    
    if (scalar(@existing) != scalar(@proposed)) {
        return FALSE
    }
    for (my $n = 0; $n < @existing; ++$n) {
        if ($existing[$n] ne $proposed[$n]) {
            return FALSE
        }
    }
    return TRUE
}
