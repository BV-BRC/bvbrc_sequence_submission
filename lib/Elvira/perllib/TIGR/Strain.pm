# File: Strain.pm
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
# Strain.pm Contains the Strain storage object and all the accessors and methods to verify and manipulate the fields.

package Strain;

use strict;
use warnings;
###---To be removed once deployed---###
use lib '../';
#######################################

#use Data::Dumper;
use File::Path;
use Log::Log4perl (qw(get_logger));
use JCVI::Logging::L4pTools;
use Cwd (qw(abs_path));
use TIGR::GLKLib;
use TIGR::Country_Code;
use Time::Piece;
use Time::ParseDate;

my $jlt = JCVI::Logging::L4pTools->new();
my $cc_obj = TIGR::Country_Code->new();

use constant EPIWEEK_FIRST_IGNORE => 3;
use constant WEEKS_YEAR => 52;

my %month = (Jan => 31,
             Feb => 29,
             Mar => 31,
             Apr => 30,
             May => 31,
             Jun => 30,
             Jul => 31,
             Aug => 31,
             Sep => 30,
             Oct => 31,
             Nov => 30,
             Dec => 31);

=head1  NAME Strain

Contains the Strain storage object and all the accessors and methods to verify and manipulate the fields.

=cut


=head2 new();

    my $strain_obj = Strain->new();
    my $strain_obj = Strain->new(virus_name => $virus_name, collection_date => $collection_date, strain => $strain, country => $country, host => $host, isolate => $isolate, ...);
    
    
It creates and initializes the object. The object can be created empty and filled with the accessors, or it can be filled directly.

The object is pre-populated with all the recognized key, associated to undef value. Initializing the object with an unknown key triggers a fatal exception.

List of recognized keys:

city
collection_date
country
country_code
epidemiologic_week
genogroup
genotype
host
host_comm_name
isolate
strain
subtype
virus_code
virus_name


   
=cut

sub new {
    my $class = shift();
    my $logger = get_logger(ref($class));
    $logger->trace("Creating a new strain object");
    my $self = {city                => undef,
                collection_date     => undef,
                country             => undef,
                country_code        => undef,
                epidemiologic_week  => undef,
                epidemiologic_year  => undef,
                genogroup           => undef,
                genotype            => undef,
                host                => undef,
                host_comm_name      => undef,
                isolate             => undef,
                strain              => undef,
                subtype             => undef,
                virus_code          => undef,
                virus_name          => undef
    };
    my $good = 1;
    my %params = @_;
    
    while (my ($attr, $val) = each(%params)) {
        if (exists($self->{$attr})) {
            $self->setAttribute($attr, $val);
        }
        else {
            $logger->error("Constructor called with unrecognized parameter (\"$attr\").");
            $good = 0;
        }
    }
    if ($good) {
        $logger->trace('Exiting');
        return(bless($self, $class));
    }
    else {
        $logger->logdie("Too many problems found. Quitting now");
    }  
}

=head2 setAttribute()

    $strain_obj->setAttribute($attr, $value);

Given the attribute name and the corresponding value, it call the correct accessor for that attribute.

=cut

sub setAttribute {
    my ($self, $attr, $val) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    if ($attr eq 'city') {
        $self->setCity($val);
    }
    elsif ($attr eq 'collection_date') {
        $self->setCollectionDate($val);
    }
    elsif ($attr eq 'country') {
        
    }
    elsif ($attr eq 'country_code') {
        
    }
    elsif ($attr eq 'epidemiologic_week') {
        
    }
    elsif ($attr eq 'genogroup') {
        
    }
    elsif ($attr eq 'genotype') {
        
    }
    elsif ($attr eq 'host') {
        
    }
    elsif ($attr eq 'host_comm_name') {
        
    }
    elsif ($attr eq 'isolate') {
        
    }
    elsif ($attr eq 'strain') {
        
    }
    elsif ($attr eq 'subtype') {
        
    }
    elsif ($attr eq 'virus_code') {
        
    }
    elsif ($attr eq 'virus_name') {
    
    
    }
    $logger->trace('Exiting');
}

=head2 setCity()

    $strain_obj->setCity($city);

Accessor to set the parameter 'city'

=cut

sub setCity {
    my ($self, $city) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    $self->{city} = $city;
    
    $logger->trace('Exiting');
}

=head2 getCity()

    my $city = $strain_obj->getCity();

Accessor to retrive the parameter 'city'

=cut

sub getCity {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{city});
}

=head2 setCollectionDate()

    $strain_obj->setCollectionDate($collection_date);

Accessor to set the parameter 'collection_date'. It also updates the 'epidemiologic_week' accordingly
The date must be in INSDC format (DD-Mmm-YYYY)

=cut

sub setCollectionDate {
    my ($self, $collection_date) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    my $epid_week = undef;
    
    if (defined($collection_date)) {
        if ($collection_date =~ /^(\d{1,2})([-\/.])([ADFJMNOS][a-z]{2})\2(\d{4})$/) {
            my ($d_candidate, $m_candidate, $y_candidate) = ($1, $3, $4);
            
            unless (exists($month{$m_candidate})) {
                $m_candidate = ucfirst(lc($m_candidate));
                
                if (exists($month{$m_candidate})) {
                    $logger->warn("Month in collection_date (\"$collection_date\") spelled with wrong case. Converting it to \"$m_candidate\".");
                }
                else {
                    $logger->logdie("Unable to recognize the month in the supplied collection_date (\"$collection_date\").");
                }
            }
            unless ($d_candidate > 0 && $d_candidate <= $month{$m_candidate}) {
                $logger->logdie("Invalid value of day ($d_candidate) in collection_date \"$collection_date\".");
            }
            $collection_date = "$d_candidate-$m_candidate-$y_candidate"; 
        }
        
        
        $epid_week = $self->setEpiWeekYear($collection_date);
    }
    $self->{collection_date}    = $collection_date;
    
    $logger->trace('Exiting');
}

=head2 getCollectionDate()

    my $collection_date = $strain_obj->getCollectionDate();

Accessor to retrive the parameter 'collection_date'

=cut

sub getCollectionDate {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{collection_date});
}

=head2 setCountry()

    $strain_obj->setCountryByName($country_name)
    
Given the country name, it verifies that it is indeed a valid country and sets both the "county" and the "country_code".    

=cut

sub setCountry {
my ($self, $name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless ($cc_obj->isValidCountry($name)) {
        my $name = $cc_obj->cleanCountry($name);
    }
    unless (defined($name)) {
        $logger->logdie("Impossible to set the country name and ISO ALPHA-3 code.");
    }
    $self->{country} = $name;
    my $c_code = $cc_obj->getIso3byCountryName($name);
    
    if (defined($c_code)) {
        $self->{country_code} = $c_code;
    }
    else {
        $logger->warn("Country \"$name\" does not have a ISO ALPHA-3 code. - leaving the field blank");
    }
    $logger->trace('Exiting');
}

=head2 getCountry()

    my $country = $strain_obj->getCountry();

Accessor to retrive the parameter 'country'

=cut

sub getCountry {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{country});
}

=head2 getCountryCode()

    my $country_code = $strain_obj->getCountryCode();

Accessor to retrive the parameter 'country_code'

=cut

sub getCountryCode {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{country_code});
}

=head2 setEpiWeekYear()

    $strain_obj->setEpiWeekYear($collection_date);
    
Given the collection date in INSDC format (i.e. DD-Mmm-YYYY), it sets the epidemiologic week and year

Definition of epidemiologic week ("epi week")    
"The first epi week of the year ends, by definition, on the first Saturday of January, as long as it falls at least four days into the month. Each epi week begins on a Sunday and ends on a Saturday."

=cut

sub setEpiWeekYear {
    my ($self, $collection_date) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless ($collection_date =~ /^\d{1,2}([-.\/])[ADFJMNOS][a-z]{2}\1(\d{4})$/) {
        $logger->logdie("Invalid format/value (\"$collection_date\"). - Expecting a date in INSDC format (i.e. DD-Mmm-YYYY).");
    }
    my $epi_year = $1;
    my $coll_obj = localtime(parsedate($collection_date));
    my $epi_brk_obj = localtime(parsedate("01-05-$epi_year"));

    my $epi_week = $coll_obj->week();
    
    if ($epi_brk_obj->day_of_week() > 0) { ## If the first Saturday of the year is before Jan 4... 
        --$epi_week;
        
        if ($epi_week < 1) { ## If this is the case, we need to check if we are in epi-week 52 or 53 of the year before...
            --$epi_year;
            ## Calculating in which epi-week Dec 31 of the previous year falls in...
            my $ny_eve_obj = localtime(parsedate("12-31-$epi_year"));
            $epi_week = $ny_eve_obj->week();
            $epi_brk_obj = localtime(parsedate("01-05-$epi_year"));
            
            if ($epi_brk_obj->day_of_week() > 0) { ## Same pinciple as the wrapping check...
                --$epi_week;
            }
            ## Now checking if the collection date and Dec 31 of the previous year are in the same week (if we're here, they can't be more than a week apart)...
            if ($coll_obj->day_of_week() - $ny_eve_obj->day_of_week() <= 0) {
                ++$epi_week;
            }
        }
    }
    $self->{epidemiologic_week} = $epi_week;
    $self->{epidemiologic_year} = $epi_year;        
    $logger->trace('Exiting');
}

=head2 getEpiYear()

    my $epi_year = $strain_obj->getEpiYear();

Accessor to retrive the parameter 'epidemiologic_year'

=cut

sub getEpiYear {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{epidemiologic_year});
}

=head2 getEpiWeek()

    my $epi_week = $strain_obj->getEpiWeek();

Accessor to retrive the parameter 'epidemiologic_week'

=cut

sub getEpiWeek {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    
    return($self->{epidemiologic_week});
}



1;