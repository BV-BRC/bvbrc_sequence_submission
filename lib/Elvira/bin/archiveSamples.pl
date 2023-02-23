#!/usr/local/bin/perl

# File: archiveSamples.pl
# Author: Paolo Amedeo
# Created: November, 4 2016
#
# $Author:  $
# $Date:  $
# $Revision:  $
# $HeadURL: $
#
# Copyright 2016, J. Craig Venter Institute
#
# This program archieves sequence and assembly data of samples by either scanning the VHTNGS area for published samples 
# or by acting on a list of tuples

=head1 NAME
    
    archiveSamples.pl
    
=head1 USAGE

    archiveSamples.pl [-]-leftovers <leftovers_file> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-leftovers <leftovers_file>

    Output file containing in a tab-delimited list the reason for not processing the published sample and the correspondent tuple.

=for Euclid:
    leftovers_file.type:   writeable

=back

=head1 OPTIONS

=over

=item [-]-t[uple_file] <tuple_file>

    Comma-separated file with database, collection, and BAC ID.
    If this option is specified, the script will act on the samples contained in the list regardless to their status.
    By default the script archives all the samples marked as published, unless an archive tarball is already present.

=for Euclid:
    tuple_file.type: readable

=item [-]-force_archiving

    By default, the program does nothing for the samples for which there is already a tarball in archive.
    If this option is specified, the old tarball is removed and replaced with the current data.

=item [-]-archive_alike

    If this option is specified, the program will archive also directories whose name includes the BAC ID (e.g. "63481_RNA_SISPA").

=item [-]-archive_dismissed

    Archive also samples that have jira_status values 'Deprecated' and 'Unresolved'

=item [-]-archive_lost

    Archive any sample, even if the BAC ID is not found in the database.

=item [-]-delete_only

    Do not archive, just delete from productions the directories corresponding to the tuples in the tuples file and replaces them with links to Archive.
    NOTE: This option must be specified together with [-]-t[uple_file]

=item [-]-archive_root <archive_root_dir>

    Root directory where to archive the samples (default: /usr/local/archdata/700010/projects/VHTNGS/sample_data_new)
    
=for Euclid:
    archive_root_dir.type:    string
    archive_root_dir.default: '/usr/local/archdata/700010/projects/VHTNGS/sample_data_new'

=item [-]-test 

    It simulates only.

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

    This program archieves sequence and assembly data of samples by either scanning the VHTNGS area for published samples or by acting on a list of tuples
=cut

BEGIN {
    use Cwd (qw(abs_path getcwd));
    $::cmd = join(' ', $0, @ARGV);
    $::working_dir = getcwd();
}

use strict;
use warnings;
use FindBin;
use lib (".");
use Getopt::Euclid 0.2.4 (qw(:vars));
#use Data::Dumper;
use File::Basename;
use File::Path;
use File::Copy;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use TIGR::GLKLib;
use ProcessingObjects::SafeIO;

## Constants declaration
#
use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;
use constant ARCH_ABORTED       => 'Problems in archiving - operation aborted';
use constant ARCH_PLUS_DATA     => 'Exist both tarball and "new" data';
use constant DEFAULT_DB         => 'giv';
use constant DIR_NOT_FOUND      => 'Unexpected Error - Dir missing in action';
use constant DIR_PERM           => 0775;
use constant FILE_NOT_DIR       => 'Found file instead of directory';
use constant JIRA_PUBLISHED     => 'Sample Published';
use constant JIRA_UNRESOLVED    => 'Unresolved';
use constant JIRA_DEPRECATED    => 'Deprecated';
use constant LOOKALIKE_IGNORED  => 'Ignoring "look-alike" directories';
use constant MISSING_PROD_DIR   => 'Production dir not found';
use constant MISSING_TBALL_DEL  => 'Attempting deletion of unarchived sample';
use constant MISSING_TARBALL    => 'Found broken link to tarball';
use constant NOT_FOUND_IN_DB    => 'Tuple not found in database';
use constant PROD_ROOT          => '/usr/local/projdata/700010/projects/VHTNGS/sample_data_new';
use constant SIMULATED_ARCH     => 'Simulated Archiving';

our ($ARGV_leftovers, $ARGV_tuple_file, $ARGV_force_archiving, $ARGV_archive_alike, $ARGV_archive_dismissed, $ARGV_archive_lost, $ARGV_delete_only, $ARGV_archive_root, $ARGV_test, $ARGV_server, $ARGV_password_file, $ARGV_debug, $ARGV_log_file);

my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $ARGV_log_file, ARGV_debug => $ARGV_debug);
my $logger = $jlt->getLogger(basename($0));

$logger->info("Command line: $::cmd\nInitial working directory: $::working_dir\nDebug level: \"$ARGV_debug\"");
my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => DEFAULT_DB, server => $ARGV_server, pass_file => $ARGV_password_file);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

$glk->setAttrValValidation(FALSE);

unless (-d $ARGV_archive_root) {
    mk_tree_safe($ARGV_archive_root, DIR_PERM);
}

## Getting the samples...

my %samples = ();
my $leftover = 0;
my $archived = 0;
my %smpl_eid = ();
my %ace_loc = ();
my %alike = ();
my @to_be_deleted = ();
my $ok_to_delete = TRUE;
my $last_db = DEFAULT_DB;

open(my $lh, ">$ARGV_leftovers") || $logger->logdie("Impossible to open the leftovers file (\"$ARGV_leftovers\") for writing.");

if ($ARGV_tuple_file) {
    open(my $tfh, $ARGV_tuple_file) || $logger->logdie("Impossible to open the tuple file \"$ARGV_tuple_file\" for reading.");
    $ARGV_archive_alike = TRUE;
    
    while (<$tfh>) {
        next unless /\S/;
        chomp();
        s/\s+//g;
        my ($db, $coll, $bid) = split /,/;
        
        if (defined($db) && $db =~ /\S/ && defined($coll) && $coll =~ /\S/ && defined ($bid) && $bid =~ /\S/) {
            push(@{$samples{$db}{$coll}}, $bid);
            
            unless ($bid =~ /^\d+$/) {
                undef($alike{$db}{$coll}{$bid});
            }
        }
        else {
            $logger->error("Unable to recognize the following line (File \"$ARGV_tuple_file\", line " . $. + 1 . ") as a valid tuple: \"$_\" - Skipping it.");
        }
    }
    close($tfh);
}
elsif ($ARGV_delete_only) {
    $logger->logdie("Option --delete_only requires a tuple_file as source of directories to be deleted.")
}
else {
    my $r_dbs = $glk->getAllVgdDbs();
    
    foreach my $db (@{$r_dbs}) {
        $glk->changeDb($db) unless $db eq $last_db;
        my $r_smpls = $glk->getExtentsByType('SAMPLE');
        
        foreach my $eid (@{$r_smpls}) {
            next if $glk->isDeprecated($eid) && ! $ARGV_archive_dismissed;
            my $status = $glk->getExtentAttribute($eid, 'jira_status');
            
            if (defined($status) && ($status eq JIRA_PUBLISHED || $ARGV_archive_dismissed && ($status eq JIRA_UNRESOLVED || $status eq JIRA_DEPRECATED))) {
                my $coll = $glk->getCollection($eid);
                
                if (defined($coll)) {
                    my $info = $glk->getExtentInfo($eid);
                    my $bid = $info->{'ref'};
                    push(@{$samples{$db}{$coll}}, $bid);
                    $smpl_eid{$bid} = $eid;
                }
                else {
                    $logger->error("Database $db, Sample Extent $eid - unable to find a collection for this Extent. - Skipping it.");
                }
            }
        }
        $last_db = $db;
    }
}

## At this point we have a list of samples (either all the published samples in all the databases, or the list provided with the tuple file)

## Removing from the list all the samples that have been already archived and do not have any new data


foreach my $db (keys(%samples)) {
    my $db_prod_dir = PROD_ROOT . "/$db";
    
    my $db_exists = $glk->isVgdDb($db);
    
    $glk->changeDb($db) if $db_exists && $db ne $last_db;
    
    unless (-d $db_prod_dir) {
        foreach my $coll (keys(%{$samples{$db}})) {
            foreach my $bid (@{$samples{$db}{$coll}}) {
                my $tuple = "$db,$coll,$bid";
                print {$lh} MISSING_PROD_DIR, "\t$tuple\n";
                ++$leftover;
            }
        }
        $logger->info("Database \"$db\" - Unable to find directory \"$db_prod_dir\" in the production area. - Skipping samples in this database.");
        next        
    }
    foreach my $coll (keys(%{$samples{$db}})) {
        my $prod_path = PROD_ROOT . "/$db/$coll";
        my $arch_path = "$ARGV_archive_root/$db/$coll";

        ## Gathering all the subdirs of the production path...
        my @dir_list = ();
        
        if (-d $prod_path) {
            opendir(my $pd, $prod_path) || $logger->logdie("Impossible to access the production directory \"$prod_path\".");
             
            foreach my $thing (readdir($pd)) {
                next if $thing =~ /^\.+$/ || $thing =~ /^\d+$/; ## Skipping FS placeholders and actual (=unmodified) sample dirs
                push(@dir_list, $thing) if -d "$prod_path/$thing" && ! -l "$prod_path/$thing";
            }
            closedir($pd);
        }
        else {
            foreach my $bid (@{$samples{$db}{$coll}}) {
                my $tuple = "$db,$coll,$bid";
                print {$lh} MISSING_PROD_DIR, "\t$tuple\n";
                ++$leftover;
            }
            $logger->info("Unable to find production directory \"$prod_path\". - Skipping it.");
            next
        }
        foreach my $bid (@{$samples{$db}{$coll}}) {
            my $tuple = "$db,$coll,$bid";
            my $eid;
            

            if (-e "$prod_path/$bid") {
                if (exists($smpl_eid{$bid})) {
                    $eid = $smpl_eid{$bid};
                }
                elsif ($bid =~ /^\d+$/ && $db_exists) { ## In the case of tuple file, we could have "look-alike" directories listed as well
                    $eid = $glk->getExtentByTypeRef('SAMPLE', $bid) if $db_exists;
                    
                    unless (defined($eid) || $ARGV_delete_only || $ARGV_archive_lost) {
                        $logger->error("Unable to find sample $bid in database $db - Skipping it.");
                        print {$lh} NOT_FOUND_IN_DB, "\t$tuple\n";
                        ++$leftover;
                        next
                    }
                    if (defined($eid)) {
                        $smpl_eid{$bid} = $eid;
                    
                        if ($db_exists && $glk->hasExtentAttribute($eid, 'ace_location')) {
                            my $ace = $glk->getExtentAttribute($eid, 'ace_location');
                            
                            unless ($ace =~ /^\(.+\)$/) {
                                $ace_loc{$eid} = "($ace)";
                            }
                        }
                    }
                }
                elsif (! $ARGV_tuple_file) {
                    $logger->logdie("Unexpected situation: pulled from database $db collection \"$coll\" a non-all-nuber BAC ID: \"$bid\".")
                }
                if ($ARGV_delete_only) {
                    &markForDeletion($db, $coll, $bid);
                }
                else {
                    &archiveIt($db, $coll, $bid);
                }
            }
            else {
                $logger->info("Unable to find sample directory \"$prod_path/$bid\" in the production area. - Skipping it.");
                print {$lh} MISSING_PROD_DIR, "\t$tuple\n";
                ++$leftover;
            }
            
            ## Looking for "look-alike" directories
            foreach my $thing (@dir_list) {
                next unless $thing =~ /$bid/;
                next if exists($alike{$db}{$coll}{$thing});
                
                if ($ARGV_archive_alike) {
                    &archiveIt($db, $coll, $thing);
                }
                elsif (-d "$prod_path/$thing") { ## If it is a link, it preasumably means it's already archived and we don't list it
                    $tuple = "$db,$coll,$thing";
                    print {$lh} LOOKALIKE_IGNORED, "\t$tuple\n";
                    ++$leftover;
                }
            }
            
        }
    }
    $last_db = $db;
}

if ($ARGV_delete_only) {
    if ($ok_to_delete) {
        $archived = &deleteProduction();
    }
    else {
        $logger->logdie("The tuple list provided contains one or more tuples of samples that do not have any tarball in Archive. Check the leftovers file for keyword \"" . MISSING_TBALL_DEL . '"')
    }
}

close($lh);

my $action;

if ($ARGV_test) {
    if ($ARGV_delete_only) {
        $action = 'Simulated the deletion of';
    }
    else {
        $action = 'Simulated the archiving of';
    }
}
elsif ($ARGV_delete_only) {
    $action = 'DELETED';
}
else {
    $action = 'Archived';
}

if ($leftover > 0) {
    if ($ARGV_test && !$ARGV_delete_only) {
        $leftover -= $archived; ## In the case of testing we're double-counting the archived samples because they're also listed in the leftovers
    }
    
    if ($leftover > 1) {
        print "\nDone.\n\n$action $archived samples. However $leftover samples were left untouched.\n\n";
    }
    else { 
        print "\nDone.\n\n$action $archived samples. However $leftover sample was left untouched.\n\n";
    }
}
elsif ($ARGV_tuple_file) {
    print "\nDone.\n\n$action all the samples in the tuple file ($archived).\n\n";
}
else {
    print "\nDone.\n\n$action all the published samples ($archived).\n\n";
}

sub markForDeletion {
    my ($db, $coll, $bid) = @_;
    my $prod_path = PROD_ROOT . "/$db/$coll";
    my $arch_path = "$ARGV_archive_root/$db/$coll";
    my $smpl_dir = "$prod_path/$bid";
    my $arch_smpld = "$arch_path/$bid";
    my $smpl_tgz = "$arch_smpld/$bid.tgz";
    my $tuple = "$db,$coll,$bid";
    
    if (-e $smpl_dir) { ## This check is rather useless, since we've done practically the same in the body of the program
        if (-l $smpl_dir) {
            if (-e $smpl_tgz) {
                $logger->info("Database: $db, Collection: $coll, Sample $bid is already archived and there is no file in production.");
            }
            else {
                $logger->error("Database: $db, Collection: $coll, Sample $bid Found link in production, but not corresponding tarball(\"$smpl_tgz\") in Archieve.");
                print {$lh} MISSING_TARBALL, "\t$tuple\n";
                ++$leftover;
            }
        }
        elsif (-d $smpl_dir) {
            if (-e $smpl_tgz) {
                if ($ARGV_delete_only) {
                    push(@to_be_deleted, [$smpl_dir, $prod_path, $arch_smpld, $tuple]);
                }
                else {
                    $jlt->die("Subroutine markForDeletion() called without --delete_only parameter being specified.")
                }
            }
            else {
                $ok_to_delete = FALSE;
                $logger->error("Database: $db, Collection: $coll, Sample $bid - No archive existing for this file. Halting the deletion of all samples in \"$ARGV_tuple_file\"");
                print {$lh} MISSING_TBALL_DEL, "\t$tuple\n";
            }
        }
        else {
            $logger->error("\"$smpl_dir\" is neither a link, nor a directory. - Skipping this sample.");
            print {$lh} FILE_NOT_DIR, "\t$tuple\n";
            ++$leftover;
        }
    }
}

sub archiveIt {
    my ($db, $coll, $bid) = @_;
    my $prod_path = PROD_ROOT . "/$db/$coll";
    my $arch_path = "$ARGV_archive_root/$db/$coll";
    my $smpl_dir = "$prod_path/$bid";
    my $arch_smpld = "$arch_path/$bid";
    my $smpl_tgz = "$arch_smpld/$bid.tgz";
    my $tmp_tgz = "$smpl_tgz.tmp";
    my $tuple = "$db,$coll,$bid";
    my $good = TRUE;
    
    if (-e $smpl_dir) { ## This check is rather useless, since we've done practically the same in the body of the program
        if (-l $smpl_dir) {
            if (-e $smpl_tgz) {
                $logger->debug("Database: $db, Collection: $coll, Sample $bid is already archived");
            }
            else {
                $logger->error("Database: $db, Collection: $coll, Sample $bid Found link in production, but not corresponding tarball(\"$smpl_tgz\") in Archieve.");
                print {$lh} MISSING_TARBALL, "\t$tuple\n";
                ++$leftover;
            }
        }
        elsif (-d $smpl_dir) {
            if (-e $smpl_tgz) {
                if ($ARGV_force_archiving && ! $ARGV_test) {
                    move($smpl_tgz, $tmp_tgz);
                }
                else {
                    $logger->warn("Database: $db, Collection: $coll, Sample $bid - There is already a tarball in Archive, but ther is still a directory with data in production.");
                    print {$lh} ARCH_PLUS_DATA, "\t$tuple\n";
                    ++$leftover;
                    $good = FALSE;
                }
            }
            if ($good) {
                &packItUp($db, $coll, $bid);
            }
        }
        else {
            $logger->error("\"$smpl_dir\" is neither a link, nor a directory. - Skipping this sample.");
            print {$lh} FILE_NOT_DIR, "\t$tuple\n";
            ++$leftover;
        }
    }
    else { ## If in production there is no directory corresponding to the sample
        $logger->error("Unexpected behavior: Despite upstream checks, it is now impossible to find directory or link in the production area corresponding to sample $bid (Collection \"$coll\", Database \"$db\" - Expected: \"$smpl_dir\") - Make sure that the sample was not originally loaded/sequenced under a different database or collection");
        print {$lh} DIR_NOT_FOUND, "\t$tuple\n";
        ++$leftover;
    }
}

sub packItUp {
    my ($db, $coll, $bid) = @_;
    my $prod_path = PROD_ROOT . "/$db/$coll";
    my $arch_path = "$ARGV_archive_root/$db/$coll";
    my $tuple = "$db,$coll,$bid";
    my $smpl_dir = "$prod_path/$bid";
    my $arch_smpld = "$arch_path/$bid";
    my $smpl_tgz = "$arch_smpld/$bid.tgz";
    my $tmp_tgz = "$smpl_tgz.tmp";
    
    print STDERR "\nProcessing sample DB: $db, Collection: $coll, BAC ID: $bid (path: $smpl_dir)\n";
            
    unless (-d $arch_smpld) {
        if ($ARGV_test) {
            $logger->info("Simulating the creation of the sample directory in Archive (\"$arch_smpld\").");
        }
        else {
            $logger->debug("Creating the sample directory in Archvie (\"$arch_smpld\")");
            mk_tree_safe($arch_smpld, DIR_PERM);
        }
    }
    my $tar_cmd = "cd $smpl_dir; tar -czvf $smpl_tgz *";
    my $clean_cmd = "rm -rf $smpl_dir";
    my $del_old_tgz_cmd = "rm $tmp_tgz";
    
    if ($ARGV_test) {
        $logger->info("Simulating the creation of the tarball - CMD: \"$tar_cmd\"");
        $logger->info("Simulating the deletion the pre-existing tarball - CMD: \"$del_old_tgz_cmd\"") if -f $tmp_tgz;
        $logger->info("Simulating the removal of files and directories from the production area - CMD: \"$clean_cmd\"");
        createLink($prod_path, $arch_smpld);
        ++$archived;
        print {$lh} SIMULATED_ARCH, "\t$tuple\n";
        ++$leftover;
    }
    else {
        $logger->debug("Creating the tarball - CMD: \"$tar_cmd\"");
        
        if (system($tar_cmd)) {
            $logger->error("Problems creating the tarball (CMD: \"$tar_cmd\" Errors: \"$!\", \"$?\") - skipping the other operations and proceeding with the next sample.");
            print {$lh} ARCH_ABORTED, "\t$tuple\n";
            next
        }
        if (-e $tmp_tgz) {
            $logger->debug("Deleting the pre-existing tarball - CMD: \"$del_old_tgz_cmd\"");
            system($del_old_tgz_cmd) && $logger->error("Problems with deleting the old tarball (CMD: \"$del_old_tgz_cmd\" Errors: \"$!\", \"$?\") - skipping the other operations and proceeding with the next sample.");
        }
        $logger->debug("Removing files and directories from the production area - CMD: \"$clean_cmd\"");
        
        if (system($clean_cmd)) {
            $logger->error("Problems with removing files and directories from the production area (CMD: \"$clean_cmd\" Errors: \"$!\", \"$?\") - skipping the other operations and proceeding with the next sample.");
            print {$lh} ARCH_ABORTED, "\t$tuple\n";
            ++$leftover;
            next
        }
        if (createLink($prod_path, $arch_smpld)) {
            if ($bid =~ /^\d+$/) {
                if (exists($smpl_eid{$bid})){
                    my $eid = $smpl_eid{$bid};
                    
                    if (exists($ace_loc{$eid})) {
                        &updateAceLoc($db, $eid);
                    }
                }
                elsif (!$ARGV_archive_lost) {
                    $logger->error("Unexpected condition: unable to find the Extent_id for the current sample (BAC ID: $bid, DB: $db)");
                }
            }
            ++$archived;
            $logger->info("Archived sample \"$tuple\"");
        }
        else {
            print {$lh} ARCH_ABORTED, "\t$tuple\n";
            ++$leftover;
        }
    }
}

sub updateAceLoc {
    my ($db, $eid) = @_;
    
    if ($ARGV_test) {
        $logger->info("Simulating the updating of the 'ace_location' attribute for Extent $eid (database $db): \"$ace_loc{$eid}\"");
    }
    else {
        $glk->changeDb($db) unless $db eq $last_db; ## the existance of the db is already vetted earlier in the code
        $glk->setExtentAttribute($eid, 'ace_location', $ace_loc{$eid});
        $last_db = $db;
    }
}

sub deleteProduction {
    my $deleted = 0;
    
    foreach my $smpl (@to_be_deleted) {
        my ($smpl_dir, $prod_path, $arch_smpld, $tuple) = @{$smpl};
        my $clean_cmd = "rm -rf $smpl_dir";
        
        if ($ARGV_test) {
            $logger->info("Simulating the deletion of working directory \"$smpl_dir\" (CMD: \"$clean_cmd\")"); 
            print STDERR '.';
            if (&createLink($prod_path, $arch_smpld)) {
                ++$deleted;
            }
            else {
                print {$lh} ARCH_ABORTED, "\t$tuple\n";
            }
            
        }
        else {
            if (system($clean_cmd)) {
                $logger->error("Problems running the following command (\"$!\", \"$?\"):\n\"$clean_cmd\"");
            }
            else {
                $logger->trace("CMD: \"$clean_cmd\" - Success.");
                print STDERR '.';
                
                if (&createLink($prod_path, $arch_smpld)) {
                    ++$deleted;
                }
                else {
                    print {$lh} ARCH_ABORTED, "\t$tuple\n";
                }
            }
        }
    }
    print STDERR "\n";
    return $deleted
}

sub createLink {
    my ($prod, $arch) = @_;
    
    my $link_cmd = "cd $prod; ln -s $arch .";
    
    if ($ARGV_test) {
        $logger->info("Simulating the creation of  the link to Archive in production - CMD: \"$link_cmd\"");
    }
    else {
        $logger->debug("Creating the link to Archive in production - CMD: \"$link_cmd\"");
        
        if (system($link_cmd)) {
            $logger->error("Problems with creating a link to the Archive area into production (CMD: \"$link_cmd\" Errors: \"$!\", \"$?\")");
            return FAILURE
        }
    }
    return SUCCESS
}