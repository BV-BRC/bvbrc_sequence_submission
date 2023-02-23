# File: Strain_Code.pm
# Author: pamedeo
# Created: July 17, 2014
#
# $Author: $
# $Date: $
# $Revision: $
# $HeadURL: $
#
# Copyright 2014, J. Craig Venter Institute
#
# Strain_Code.pm Collects all the functions required for building and handling a NCBI strain_code string.

package Strain_Code;

use strict;
use warnings;

#use Data::Dumper;
use File::Path;
use Log::Log4perl (qw(get_logger));
use JCVI::Logging::L4pTools;
use JCVI::DB_Connection::VGD_Connect;
use Cwd (qw(abs_path));
use TIGR::GLKLib;


my $jlt = JCVI::Logging::L4pTools->new();

my %virus_behavior = ('' => '');


=head1  NAME Strain_Code

This is a collection of methods for creating and handling NCBI strain_code string.

=cut


=head2 new();

    my $template_object = template->new();
    
It creates and initializes the object. Describe here mandatory and optional arguments.
   
=cut

sub new {
    my $class = shift();
    my $logger = get_logger(ref($class));
    $logger->trace("Creating a new object");
    my $self = {@_};
    return(bless($self, $class));  
}
1;