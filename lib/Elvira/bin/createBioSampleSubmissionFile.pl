#!/usr/bin/env perl
umask 002;
use strict;

use File::Basename;
my $SCRIPT = basename($0);

# #########################################################################
#
#    Version Strings
#
# #########################################################################
my $VERSION = "0.90";
my $BUILD = (qw/$Revision: 1223 $/ )[1];

# #########################################################################
#
#    Load Modules
#
# #########################################################################

#
#  Use Local Modules First
#
use FindBin qw($Bin);
use lib "$Bin";
use lib "$Bin/../perllib";
#
#  Standard USE statments
#
use Getopt::Long;

#
#  Load JavaRunner
#
use Java::Runner;


# #########################################################################
#
#    Global Variables
#
# #########################################################################

# #########################################################################
#
#    Set up the JavaRunner
#
# #########################################################################

my $runner = Java::Runner->new();

$runner->useJavaPreset("8");

$runner->clearClassPath();
$runner->initialHeapSize(48);
$runner->addClassLocation("$Bin/../resources");
$runner->addJarDirectory("$Bin/../lib/log4j");

$runner->addJarDirectory("$Bin/../lib/jillion");
$runner->addJarDirectory("$Bin/../lib/elvirautilities");
$runner->addJarDirectory("$Bin/../lib/glklib");
$runner->addJarDirectory("$Bin/../lib/jlims");
$runner->addJarDirectory("$Bin/../lib/jodatime");
$runner->addJarDirectory("$Bin/../lib/apache-commons");
$runner->addJarDirectory("$Bin/../lib/hibernate");
$runner->addJarDirectory("$Bin/../lib/jdbc");



$runner->mainClass("org.jcvi.elvira.application.service.exporters.bioSample.CreateBioSampleSubmissionFile");
#pass all arguments thru as is
foreach my $arg (@ARGV){
	$runner->addParameters($arg);
}


# #########################################################################
#
#    Kick off the JavaRunner
#
# #########################################################################


    $runner->execute();


