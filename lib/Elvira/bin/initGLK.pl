#!/usr/local/bin/perl

# File: initGLK.pl
# Author: pamedeo
# Created: March 17, 2011
#
# $Author: $
# $Date:  $
# $Revision: $
# $HeadURL: $
#
# Copyright 2011, J. Craig Venter Institute
#
# initGLK.pl creates a root Extent in a brand new VGD-schema database. It replaces setupGLK, which did a bunch of other thing now no longer necessary, since the introduction of the new schema and the migration of all the lookup tables into vir_common.

=head1 NAME
    
    initGLK.pl
    
=head1 USAGE

    initGLK.pl [-]-d[atabase] <database> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <VGD_database> | -D <VGD_database>

VGD annotation database

=for Euclid:
    VGD_database.type:   string

=back

=head1 OPTIONS

=over

=item [-]-o[utput[_file]] <out_file> | [-]-outfile <out_file>

Name for the output file

=for Euclid:
    out_file.type: writeable
    
=item [-]-force_xx_attributes

If the XX collection already exists, instead of adding only the missing attributes, by using this option will overwrite the existing values too.
    
=item [-]-quiet

Suppress output (i.e. opposit of verbose, it won't write too many messages to STDOUT and won't write to any output file.

=item [-]-server <db_server> | -S <db_server>

Database server (default: SYBPROD)

=for Euclid:
    db_server.type:    string
    db_server.default: 'SYBPROD'

=item [-]-pass[word_]file <pass_file>

File with username and password for connecting to the database.

=for Euclid:
    pass_file.type: readable

=item [-]-u[ser[name]] <user> | -U <user>

Database username

=for Euclid:
    user.type: string

=item [-]-pass[word] <password> | -P <password>

Database password

=for Euclid:
    password.type: string

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

initGLK.pl creates a root Extent in a brand new VGD-schema database. It replaces setupGLK, which did a bunch of other thing now no longer necessary, since the introduction of the new schema and the migration of all the lookup tables into vir_common.



=cut

BEGIN {
    no warnings;
    use FindBin;
    $::default_ini_file = "$FindBin::Bin/../etc/Annotate_Virus.ini"; ## Ignore warning: it is used above in POD/Getopt::Euclid instructions.
    $::cmd              = join( ' ', $0, @ARGV );
}

use strict;
use warnings;
use lib ("$FindBin::Bin/../site_perl", "$FindBin::Bin");
use File::Basename;

use DBI;
use IO::File;
use TIGR::GLKLib;
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use Getopt::Euclid qw(:vars);
use File::Basename;
use Cwd qw (abs_path);

use constant SUCCESS  => 1;
use constant FAILURE  => 0;
use constant TRUE     => 1;
use constant FALSE    => 0;

our ($ARGV_database, $ARGV_output_file, $ARGV_force_xx_attributes, $ARGV_quiet, $ARGV_server, $ARGV_password_file, $ARGV_username, $ARGV_password, $ARGV_log_file, $ARGV_debug);
my $SCRIPT = basename($0);
my $log_filename = defined($ARGV_log_file) && $ARGV_log_file =~ /\S/ ? $ARGV_log_file : "Annotate_Virus_${ARGV_database}_ps$$";
my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $log_filename, ARGV_debug    => $ARGV_debug);
my $logger = $jlt->getLogger( basename($0) );

$logger->info("Command line: $::cmd");


my %xx_attrs = (annotation_contact_address  => '9704 Medical Center Dr Rockville, MD 20850 USA',
                annotation_institution      => 'J. Craig Venter Institute (JCVI)',
                annotation_lab_address      => '9704 Medical Center Dr Rockville, MD 20850 USA',
                annotation_lab_name         => 'Viral Genomics Group',
                extraction_contact_address  => '9704 Medical Center Dr Rockville, MD 20850 USA',
                extraction_institution      => 'J. Craig Venter Institute (JCVI)',
                extraction_lab_address      => '9704 Medical Center Dr Rockville, MD 20850 USA',
                extraction_lab_name         => 'Viral Genomics Group',
                sequence_contact_address    => '9704 Medical Center Dr Rockville, MD 20850 USA',
                sequence_institution        => 'J. Craig Venter Institute (JCVI)',
                sequence_lab_address        => '9704 Medical Center Dr Rockville, MD 20850 USA',
                sequence_lab_name           => 'Viral Genomics Group',
                deprecated                  => 'Deprecated samples (old school) and master for attributes to propagate to other collections');

# #########################################################################
#
#    Version Strings
#
# #########################################################################
my $VERSION = "1.00";
my $BUILD = (qw/$Revision: 127 $/ )[1];

# #########################################################################
#
#    Global Variables
#
# #########################################################################


# Standard Database Options
#my $db_passfile_path = $ARGV_password_file;
my $db_server = $ARGV_server;
my $db_name   = $ARGV_database;
my $db_user   = $ARGV_username;
my $db_pass   = $ARGV_password;

#
#  Ouput Variables
#
my $output_filename = $ARGV_output_file;
my $quiet_mode = $ARGV_quiet;
my $output = undef;

# #########################################################################
#
#    Get a GLKLib Object
#
# #########################################################################

#
$logger->info("Connecting to Database: $db_server.$db_name as $db_user.");

my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => $db_name, server => $db_server, pass_file => $ARGV_password_file, user => $db_user, pass => $db_pass, ask4pass => 1);
my $glk  = TIGR::GLKLib->new($jdb->dbh());

$logger->logdie("Database connection failed.") unless defined $glk;
$glk->setLogger($logger);

# #########################################################################
#
#    Get a handle to the output
#
# #########################################################################

# Interpret the following as aliases for console output
if (defined $output_filename && (($output_filename eq "-") || ($output_filename eq "STDOUT"))) {
    $output_filename = undef;
}
#
#  Route output to /dev/null if we requested quiet mode
#
if ($quiet_mode) {
    $output_filename = "/dev/null";
}
if (defined $output_filename) {
    $output = new IO::File;
    $output->open($output_filename, "w");
}
else {
    $output = new IO::Handle;
    $output->fdopen(1, "w");  
}

# #########################################################################
#
#    Do Work
#
# #########################################################################

# Ensure that a root extent exists
my $root_eid = $glk->getExtentRoot();

unless (defined($root_eid)) {
    $root_eid = $glk->addExtent(undef, "GENOME", "WHOLE", "Root Extent");
}
my $xx_eid = $glk->getXXeid(TRUE); ## Suppress warnings

unless (defined($xx_eid) && $xx_eid > 0) {
    $xx_eid = $glk->addExtent($root_eid, 'COLLECTION', 'XX', 'Deprecated Samples');
    
    unless (defined($xx_eid)) {
        $logger->logdie("Impossible to create the XX collection in database $ARGV_database");
    }
}
## Populating the attributes for the XX collection
my $attrs_up = 0;

while (my($attr, $val) = each(%xx_attrs)) {
    if ($glk->hasExtentAttribute($xx_eid, $attr)) {
        if ($ARGV_force_xx_attributes) {
            $glk->setExtentAttribute($xx_eid, $attr, $val);
            ++$attrs_up;
        }
    }
    else {
        $glk->addExtentAttribute($xx_eid, $attr, $val);
        ++$attrs_up;
    }
}

if ($attrs_up) {
    print "\n\n", '-' x 38,  ' WARNING: ', '-' x 38, "\n---", ' ' x 80, "---\n",
          "--- The program has inserted new attributes with default values for COLLECTION XX  ---\n",
          "--- You should open open it in Lemur and make sure that everything is fine and add ---\n",
          "--- any other required attribute.                                                  ---\n",
          '---', ' ' x 80, "---\n", '-' x 86, "\n\n"
}

# #########################################################################
#
#    End the Script
#
# #########################################################################
END {
    $output->close() if defined $output;
}



