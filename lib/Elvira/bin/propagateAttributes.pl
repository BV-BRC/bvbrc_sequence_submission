#!/usr/local/bin/perl

# File: propagateAttributes.pl
# Author: pamedeo
# Created: March 15, 2011
#
# $Author: $
# $Date:  $
# $Revision: $
# $HeadURL: $
#
# Copyright 2011, J. Craig Venter Institute
#
# propagateAttributes.pl serves for propagating GLK sequence and/or annotation attributes from the template collection to the current collection

=head1 NAME
    
    propagateAttributes.pl
    
=head1 USAGE

    propagateAttributes.pl [-]-d[atabase] <database> [-]-target_collection <collection_code> [options]

=head1 REQUIRED ARGUMENTS

=over

=item [-]-d[atabase] <VGD_database> | -D <VGD_database>

VGD annotation database

=for Euclid:
    VGD_database.type:   string

=item [-]-target_collection <collection_code>

Code of the destination collection to which to copy the attributes

=for Euclid:
    collection_code.type: string

=back

=head1 OPTIONS

=over

=item [-]-preserve

If the target collection contains already some of the attributes to be copied, do not overwrite them.

=item [-]-o[utput[_file]] <out_file> | [-]-outfile <out_file>

Name for the output file

=for Euclid:
    out_file.type: writeable
    
=item [-]-quiet

Suppress output (i.e. opposit of verbose, it won't write too many messages to STDOUT and won't write to any output file.

=item [-]-annot_attr[ibutes]

Copy over the annotation-related attributes

=item [-]-seq_attr[ibutes]

Copy over the sequence-related attributes

=item [-]-source_collection <src_coll>

Source collection (default: 'XX')

=for Euclid:
    src_coll.type:    string
    src_coll.default: 'XX'
    
=item [-]-conf[ig] <ini_file> | -C <ini_file>

INI file containing the lists of attributes to be copied for each set of attributes
(Default: $::ini_file)
    
=for Euclid:
    ini_file.type:    readable
    ini_file.default: $::ini_file

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

=item --help

    Prints this documentation and quit
    
=back

=head1 DESCRIPTION

It propagates GLK sequence and/or annotation attributes from the template collection to the current collection. It replaces copyAttributes.pl

=cut

BEGIN {
    use FindBin;
    use File::Basename;
    (my $ini_name = basename($0)) =~ s/\.pl$/.ini/;
    $::cmd = join(' ', $0, @ARGV);
    $::ini_file = "$FindBin::Bin/$ini_name";
}

use strict;
use warnings;
use FindBin;
use lib ("$FindBin::Bin/../site_perl", "$FindBin::Bin");
use DBI;
use TIGR::GLKLib;
use Config::IniFiles;
use JCVI::DB_Connection::VGD_Connect;
use Log::Log4perl (qw(get_logger));

use Getopt::Euclid qw(:vars);

use File::Basename;
use Cwd qw (abs_path);

use constant ANNOT_ATTR      => 'Annotation_Attributes';
use constant SEQ_ATTR        => 'Sequence_Attributes';
use constant EXTRACTION_ATTR => 'Extraction_Attributes';
use constant ATTR_SECTION    => 'Attributes';

our ($ARGV_database, $ARGV_target_collection, $ARGV_annot_attributes, $ARGV_seq_attributes, $ARGV_preserve, $ARGV_output_file, $ARGV_quiet, $ARGV_source_collection, $ARGV_config, $ARGV_server, $ARGV_password_file, $ARGV_username, $ARGV_password);


# #########################################################################
#
#    Global Variables
#
# #########################################################################

my $log_filename = basename($0) . '_' . $$ . '_' . $^T . '.log';
my $jlt = JCVI::Logging::L4pTools->init(ARGV_log_file => $log_filename);
my $logger = $jlt->getLogger(basename($0));

$logger->trace("Connecting to Database: $ARGV_server.$ARGV_database.");


my $jdb = JCVI::DB_Connection::VGD_Connect->new(db => $ARGV_database, pass_file => $ARGV_password_file, ask4pass => 1);
my $glk = TIGR::GLKLib->new($jdb->dbh());
$logger->logdie("Database connection failed.") unless defined $glk;

#
#  Option Variables
# 
my $preserve = $ARGV_preserve;

#
#  Ouput Variables
#
my $output_filename;
my $quiet_mode = $ARGV_quiet;
my $output = $ARGV_output_file;

# #########################################################################
#
#    Get a handle to the output
#
# #########################################################################

# Interpret the following as aliases for console output
if (defined $output_filename)
{
    $output_filename = undef if ($output_filename eq "-");
    $output_filename = undef if ($output_filename eq "STDOUT");
}

#
#  Route output to /dev/null if we requested quiet mode
#
if ($quiet_mode)
{
    $output_filename = "/dev/null";
}

if (defined $output_filename)
{
    $output = new IO::File;
    $output->open($output_filename, "w");
}
else
{
    $output = new IO::Handle;
    $output->fdopen(1, "w");  
}

# #########################################################################
#
#    Get the source
#
# #########################################################################
my $src_eid = $glk->translateExtentName("COLLECTION:$ARGV_source_collection");

unless (defined($src_eid)) {
    if ($ARGV_source_collection eq 'XX') {
        $logger->logdie("The database has not been properly initialized and the collection 'XX' does not exist. Fix the database first.");
    }
    else {
        $logger->logdie("Impossible to find collection \"$ARGV_source_collection\" in database \"$ARGV_database\".");
    }
}

# #########################################################################
#
#    Create the destination list
#
# #########################################################################


my $eid = $glk->translateExtentName("COLLECTION:$ARGV_target_collection");
unless (defined $eid) {
    $logger->logdie("Could not find collection \"$ARGV_target_collection\" in database \"$ARGV_database\".");
}

# #########################################################################
#
#    Create the target attribute list
#
# #########################################################################

my @target_attrs = ();

my $ini = Config::IniFiles->new(-file => $ARGV_config);

if ($ARGV_annot_attributes) {
    my @tmp = $ini->val(ATTR_SECTION, ANNOT_ATTR);
    push(@target_attrs, @tmp);
}
if ($ARGV_seq_attributes) {
    my @tmp = $ini->val(ATTR_SECTION, SEQ_ATTR);
    push(@target_attrs, @tmp);
}

#### Verifying that all the attributes are valid....

for (my $n = 0; $n < @target_attrs; ++$n) {
    my $copy_attr = $target_attrs[$n];
    my $bad = 0;
    
    if (!defined($copy_attr) || $copy_attr !~ /\S/) {
        ++ $bad;
    }
    else {     
        #  Verify the attribute type is valid.
        my $copy_attr_id = $glk->translateExtentAttrType($copy_attr);
        if (defined($copy_attr_id)) {
            #  Normalize the attribute back to a name.
            my $attr_name = $glk->getExtentAttrTypeName($copy_attr_id);
            $target_attrs[$n] = $attr_name;
        }
        else {
            $logger->error("Could not translate '$copy_attr' into an Extent Attribute Type.");
            ++$bad;
        }
    }
    # Eliminating the attributes flagged as being bad.
    if ($bad) {
        splice(@target_attrs, $n--, 1);
    }
}

$logger->logdie("No valid attributes requested.") unless scalar(@target_attrs) > 0;

# #########################################################################
#
#    Do Work
#
# #########################################################################

#
#  Harvest the source attributes
#
my %attr = %{$glk->getExtentAttributes($src_eid)};

#
#  Build the target list if all attributes were requested.
#

foreach my $attr_type (@target_attrs) {
    next unless exists($attr{$attr_type}); # we don't want to create attributes that don't exist in the source collection.
    
    my $attr_val = $attr{$attr_type};
    my $result = $glk->setExtentAttribute($eid, $attr_type, $attr_val, 0, $preserve);
    
    if (!defined($result) || ! $result) {
         $logger->error("Failed to set $attr_type to '$attr_val' on Extent #$eid (Collection \"$ARGV_target_collection\"");
    }
    elsif ($result = -1) {
         $logger->error("Attribute $attr_type Extent #$eid (Collection \"$ARGV_target_collection\" is already present - left unchanged.");
    }
}

# #########################################################################
#
#    End the Script
#
# #########################################################################
END
{
    $output->close() if defined $output;
}

