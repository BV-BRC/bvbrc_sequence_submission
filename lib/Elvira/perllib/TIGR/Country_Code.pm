# File: Country_Code.pm
# Author: pamedeo
# Created: July 24, 2014
#
# $Author: $
# $Date: $
# $Revision: $
# $HeadURL: $
#
# Copyright 2014, J. Craig Venter Institute
#
# Country_Code.pm Collects all the functions required for validating country names/codes and convert between the two.

package TIGR::Country_Code;

use strict;
use warnings;

#use Data::Dumper;
use File::Path;
use Log::Log4perl (qw(get_logger));
use JCVI::Logging::L4pTools;
use Cwd (qw(abs_path));

# use constant COUNTRY_TABLE => './Mapped_Coutry_Codes.table';
use constant ISO2          => 0;
use constant ISO3          => 1;
use constant ISO_NUM       => 2;

my $jlt = JCVI::Logging::L4pTools->new();

=head1  NAME Country_Code

This is a collection of methods for creating and handling NCBI strain_code string.

=cut

my %country = ();
my @c_code  = ();
my %alias   = (vietnam  => 'Viet Nam');


=head2 new();

    my $cc_obj = TIGR::Country_Code->new();
    
It creates and initializes the Cuntry_Code object. No parameters needed and/or accepted.
   
=cut

sub new {
    my $class = shift();
    my $logger = get_logger(ref($class));
    $logger->trace("Creating a new object");
    my $self = {};
    bless($self, $class);
    $self->_loadCountries();
    return($self);  
}

=head2 isValidCountry()

my $yes_no = $cc_obj->isValidCountry($country_name);

It returns 1 if the supplied name is a recognized country name, 0 otherwise

=cut

sub isValidCountry {
    my ($self, $name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering/Exiting');
    return(exists($country{$name}));
}

=head2 cleanCountry()

    my $clean_name = $cc_obj->cleanCountry($country_name);
    
Given the country name it checks if the all-lower-case version of it is recognized among the aliases.
It returns the correct spelling of the country, or undef and a warning message if no valid spelling/case has been found.

=cut

sub cleanCountry {
    my ($self, $dirty_name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    my $clean_name = undef;
    (my $lc_candidate = $dirty_name) =~ s/^\s+//;
    $lc_candidate =~ s/\s+$//;
    $lc_candidate = lc($lc_candidate);
    
    if (exists($alias{$lc_candidate})) {
        $clean_name = $alias{$lc_candidate};
    }
    else {
        $logger->warn("Unable to map \"$dirty_name\" to a valid country name.");
    }
    $logger->trace('Exiting');
    return($clean_name);
}

=head2 getCountryByIso3()

    my @coutries = @{$cc_obj->getCountriesByIso3($iso3_code)};
    
Given a 3-letters ISO ALPHA-3 code, it returns a list with the name of the countries/territories associated with that name.
If the code is not valid, an empty list is returned and a warning message thrown.

=cut
 
sub getCountriesByIso3 {
    my ($self, $iso3) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    if ($iso3 =~ /^[A-Za-z]{3}$/) {
        unless ($iso3 =~ /[A-Z]{3}/) {
            my $correct_iso3 = uc($iso3);
            $logger->warning("The ISO ALPHA-3 code should be all upper-case letters. Correcting it now (\"$iso3\" -> \"$correct_iso3\").");
            $iso3 = $correct_iso3;
        }
    }
    else {
        no warnings;
        $logger->logdie("Called with an invalid/malformed/empty ISO ALPHA-3 code (\"$iso3\").");
    }
    $logger->trace('Exiting');
    
    if (exists($c_code[ISO3]{$iso3})) {
        return($c_code[ISO3]{$iso3});
    }
    else {
        $logger->warn("Invalid ISO ALPHA-3 code. No country found corresponding to code \"$iso3\".");
        return([]);
    }
}

=head2 getCountryByIso2()

    my @coutries = @{$cc_obj->getCountriesByIso2($iso2_code)};
    
Given a 2-letters ISO ALPHA-2 code, it returns a list with the name of the countries/territories associated with that name.
If the code is not valid, an empty list is returned and a warning message thrown.

=cut
 
sub getCountriesByIso2 {
    my ($self, $iso2) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    if ($iso2 =~ /^[A-Za-z]{2}$/) {
        unless ($iso2 =~ /[A-Z]{2}/) {
            my $correct_iso2 = uc($iso2);
            $logger->warning("The ISO ALPHA-2 code should be all upper-case letters. Correcting it now (\"$iso2\" -> \"$correct_iso2\").");
            $iso2 = $correct_iso2;
        }
    }
    else {
        no warnings;
        $logger->logdie("Called with an invalid/malformed/empty ISO ALPHA-2 code (\"$iso2\").");
    }
    $logger->trace('Exiting');
    
    if (exists($c_code[ISO2]{$iso2})) {
        return($c_code[ISO2]{$iso2});
    }
    else {
        $logger->warn("Invalid ISO ALPHA-2 code. No country found corresponding to code \"$iso2\".");
        return([]);
    }
}

=head2 getCountryByIsoNumeric()

    my @coutries = @{$cc_obj->getCountriesByIsoNumeric($iso_numeric_code)};
    
Given a 3-digits ISO Numeric code, it returns a list with the name of the countries/territories associated with that name.
If the code is not valid, an empty list is returned and a warning message thrown.

=cut
 
sub getCountriesByIsoNumeric {
    my ($self, $iso_number) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless ($iso_number =~ /^\d{3}$/) {
        no warnings;
        $logger->logdie("Called with an invalid/malformed/empty ISO Numeric code (\"$iso_number\").");
    }
    $logger->trace('Exiting');
    
    if (exists($c_code[ISO_NUM]{$iso_number})) {
        return($c_code[ISO_NUM]{$iso_number});
    }
    else {
        $logger->warn("Invalid ISO Numeric code. No country found corresponding to code \"$iso_number\".");
        return([]);
    }
}


=head2 getIso3byCountryName()

    my $iso_3 = $cc_obj->getIso3byCountryName($country_name);
    
Given a valid country name, it returns the correspondent ISO ALPHA-3 code
If the name is not a valid country name, undef is returned and a warning message thrown.

=cut
 
sub getIso3byCountryName {
    my ($self, $name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless (exists($country{$name})) {
        $name = $self->cleanCountry($name);
        
        unless (defined($name)) {
            $logger->warn("Returning UNDEF");
            $logger->trace('Exiting');
            return(undef);
        }
    }
    $logger->trace('Exiting');
    return($country{$name}[ISO3]);
}


=head2 getIso3byCountryName()

    my $iso_2 = $cc_obj->getIso2byCountryName($country_name);
    
Given a valid country name, it returns the correspondent ISO ALPHA-2 code
If the name is not a valid country name, undef is returned and a warning message thrown.

=cut
 
sub getIso2byCountryName {
    my ($self, $name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless (exists($country{$name})) {
        $name = $self->cleanCountry($name);
        
        unless (defined($name)) {
            $logger->warn("Returning UNDEF");
            $logger->trace('Exiting');
            return(undef);
        }
    }
    $logger->trace('Exiting');
    return($country{$name}[ISO2]);
}


=head2 getIsoNumericByCountryName()

    my $iso_3 = $cc_obj->getIsoNumericByCountryName($country_name);
    
Given a valid country name, it returns the correspondent ISO Numeric 3-digit code
If the name is not a valid country name, undef is returned and a warning message thrown.

=cut
 
sub getIsoNumericByCountryName {
    my ($self, $name) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    unless (exists($country{$name})) {
        $name = $self->cleanCountry($name);
        
        unless (defined($name)) {
            $logger->warn("Returning UNDEF");
            $logger->trace('Exiting');
            return(undef);
        }
    }
    $logger->trace('Exiting');
    return($country{$name}[ISO_NUM]);
}


########################################################################################################
##                                        --- Private Methods ---                                     ##
########################################################################################################

## _loadCountries()
#
#  $self->_loadCountries();
#
# Initialization step consisting in loading into memory the table containing the names and ISO codes
# of all the countries recognized by INSDC.
# The expected format is a tab-delimited table with four columns in the following order:
# 1) INSDC Country Name
# 2) ISO ALPHA-2 Code
# 3) ISO ALPHA-3 Code
# 4) ISO Numeric Code
# Empty lines and lines starting with "#" are ignored.
# The hashes %country and @c_code will be emptied prior to loading.

sub _loadCountries {
    my ($self) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace('Entering');
    
    %country = ();
    @c_code  = ();
    
    while (<DATA>) {
        if (/^#/ || /^\s*$/) {
            next;
        }
        chomp();
        my ($name, $iso2, $iso3, $num) = split /\t/;
        
        if (defined($name)) {
            if (defined($num) && $num =~ /^\d+$/) {
                $num = sprintf("%03d", $num);
                $country{$name}[ISO_NUM] = $num;
                push(@{$c_code[ISO_NUM]{$num}}, $name);
            }
            else {
                no warnings;
                $logger->debug("Country \"$name\" does not have a valid Numeric ISO code (\"$num\").");
            }
            if (defined($iso2) && $iso2 =~ /^[A-Z]{2}$/) {
                $country{$name}[ISO2] = $iso2;
                push(@{$c_code[ISO2]{$iso2}}, $name);
            }
            else {
                no warnings;
                $logger->debug("Country \"$name\" does not have a valid ISO ALPHA-2 code (\"$iso2\").");
            }
            if (defined($iso3) && $iso3 =~ /^[A-Z]{3}$/) {
                $country{$name}[ISO3] = $iso3;
                push(@{$c_code[ISO3]{$iso3}}, $name);
            }
            else {
                no warnings;
                $logger->debug("Country \"$name\" does not have a valid ISO ALPHA-3 code (\"$iso3\").");
            }
            $alias{lc($name)} = $name;
        }
        else {
            no warnings;
            $logger->error("Impossible to parse line $. of the country codes table. Missing a valid country name: \"$_\". - Skipping it.");
        }
    }
    $logger->trace('Exiting');
}    

1;

__DATA__
#Country	ISO ALPHA-2  Code	ISO ALPHA-3 Code	ISO Numeric Code
Afghanistan	AF	AFG	4
Albania	AL	ALB	8
Algeria	DZ	DZA	12
American Samoa	AS	ASM	16
Andorra	AD	AND	20
Angola	AO	AGO	24
Anguilla	AI	AIA	660
Antarctica	AQ	ATA	10
Antigua and Barbuda	AG	ATG	28
Arctic Ocean			
Argentina	AR	ARG	32
Armenia	AM	ARM	51
Aruba	AW	ABW	533
Ashmore and Cartier Islands			
Atlantic Ocean			
Australia	AU	AUS	36
Austria	AT	AUT	40
Azerbaijan	AZ	AZE	31
Bahamas	BS	BHS	44
Bahrain	BH	BHR	48
Baker Island	UM	UMI	581
Bangladesh	BD	BGD	50
Baltic Sea			
Barbados	BB	BRB	52
Bassas da India	TF	ATF	260
Belarus	BY	BLR	112
Belgian Congo			
Belgium	BE	BEL	56
Belize	BZ	BLZ	84
Benin	BJ	BEN	204
Bermuda	BM	BMU	60
Bhutan	BT	BTN	64
Bolivia	BO	BOL	68
Borneo			
Bosnia and Herzegovina	BA	BIH	70
Botswana	BW	BWA	72
Bouvet Island	BV	BVT	74
Brazil	BR	BRA	76
British Virgin Islands	VG	VGB	92
Brunei	BN	BRN	96
Bulgaria	BG	BGR	100
Burkina Faso	BF	BFA	854
Burma	MM	MMR	104
Burundi	BI	BDI	108
Cambodia	KH	KHM	116
Cameroon	CM	CMR	120
Canada	CA	CAN	124
Cape Verde	CV	CPV	132
Cayman Islands	KY	CYM	136
Central African Republic	CF	CAF	140
Chad	TD	TCD	148
#Channel Islands			830
Chile	CL	CHL	152
China	CN	CHN	156
Christmas Island	CX	CXR	162
Clipperton Island			
Cocos Islands	CC	CCK	166
Colombia	CO	COL	170
Comoros	KM	COM	174
Cook Islands	CK	COK	184
Coral Sea Islands	AU	AUS	36
Costa Rica	CR	CRI	188
Cote d'Ivoire	CI	CIV	384
Croatia	HR	HRV	191
Cuba	CU	CUB	192
Curacao	CW	CUW	531
Cyprus	CY	CYP	196
Czech Republic	CZ	CZE	203
Czechoslovakia			
Democratic Republic of the Congo	CD	COD	180
Denmark	DK	DNK	208
Djibouti	DJ	DJI	262
Dominica	DM	DMA	212
Dominican Republic	DO	DOM	214
East Timor	TL	TLS	626
Ecuador	EC	ECU	218
Egypt	EG	EGY	818
El Salvador	SV	SLV	222
Equatorial Guinea	GQ	GNQ	226
Eritrea	ER	ERI	232
Estonia	EE	EST	233
Ethiopia	ET	ETH	231
Europa Island	TF	ATF	260
Falkland Islands (Islas Malvinas)	FK	FLK	238
Faroe Islands	FO	FRO	234
Fiji	FJ	FJI	242
Finland	FI	FIN	246
Former Yugoslav Republic of Macedonia			
France	FR	FRA	250
French Guiana	GF	GUF	254
French Polynesia	PF	PYF	258
French Southern and Antarctic Lands	TF	ATF	260
Gabon	GA	GAB	266
Gambia	GM	GMB	270
Gaza Strip	PS	PSE	275
Georgia	GE	GEO	268
Germany	DE	DEU	276
Ghana	GH	GHA	288
Gibraltar	GI	GIB	292
Glorioso Islands	TF	ATF	260
Greece	GR	GRC	300
Greenland	GL	GRL	304
Grenada	GD	GRD	308
Guadeloupe	GP	GLP	312
Guam	GU	GUM	316
Guatemala	GT	GTM	320
Guernsey	GG	GGY	
Guinea	GN	GIN	324
Guinea-Bissau	GW	GNB	624
Guyana	GY	GUY	328
Haiti	HT	HTI	332
Heard Island and McDonald Islands	HM	HMD	334
Honduras	HN	HND	340
Hong Kong	HK	HKG	344
Howland Island	UM	UMI	581
Hungary	HU	HUN	348
Iceland	IS	ISL	352
India	IN	IND	356
Indian Ocean			
Indonesia	ID	IDN	360
Iran	IR	IRN	364
Iraq	IQ	IRQ	368
Ireland	IE	IRL	372
Isle of Man	IM	IMN	833
Israel	IL	ISR	376
Italy	IT	ITA	380
Jamaica	JM	JAM	388
Jan Mayen	SJ	SJM	744
Japan	JP	JPN	392
Jarvis Island	UM	UMI	581
Jersey	JE	JEY
Johnston Atoll	UM	UMI	581
Jordan	JO	JOR	400
Juan de Nova Island	TF	ATF	260
Kazakhstan	KZ	KAZ	398
Kenya	KE	KEN	404
Kerguelen Archipelago	TF	ATF	260
Kingman Reef	UM	UMI	581
Kiribati	KI	KIR	296
Korea			
Kosovo			
Kuwait	KW	KWT	414
Kyrgyzstan	KG	KGZ	417
Laos	LA	LAO	418
Latvia	LV	LVA	428
Lebanon	LB	LBN	422
Lesotho	LS	LSO	426
Liberia	LR	LBR	430
Liechtenstein	LI	LIE	438
Line Islands	UM	UMI	581
Lithuania	LT	LTU	440
Luxembourg	LU	LUX	442
Libya	LY	LBY	434
Macedonia	MK	MKD	807
Macau	MO	MAC	446
Madagascar	MG	MDG	450
Malawi	MW	MWI	454
Malaysia	MY	MYS	458
Maldives	MV	MDV	462
Mali	ML	MLI	466
Malta	MT	MLT	470
Marshall Islands	MH	MHL	584
Martinique	MQ	MTQ	474
Mauritania	MR	MRT	478
Mauritius	MU	MUS	480
Mayotte	YT	MYT	175
Mediterranean Sea			
Mexico	MX	MEX	484
Micronesia	FM	FSM	583
Midway Islands	UM	UMI	581
Moldova	MD	MDA	498
Monaco	MC	MCO	492
Mongolia	MN	MNG	496
Montenegro	ME	MNE	499
Montserrat	MS	MSR	500
Morocco	MA	MAR	504
Mozambique	MZ	MOZ	508
Myanmar	MM	MMR	104
Namibia	NA	NAM	516
Nauru	NR	NRU	520
Navassa Island	UM	UMI	581
Nepal	NP	NPL	524
Netherlands	NL	NLD	528
Netherlands Antilles	AN	ANT	530
New Caledonia	NC	NCL	540
New Zealand	NZ	NZL	554
Nicaragua	NI	NIC	558
Niger	NE	NER	562
Nigeria	NG	NGA	566
Niue	NU	NIU	570
Norfolk Island	NF	NFK	574
North Korea	KP	PRK	408
North Sea			
Northern Mariana Islands	MP	MNP	580
Norway	NO	NOR	578
Oman	OM	OMN	512
Pacific Ocean			
Pakistan	PK	PAK	586
Palau	PW	PLW	585
Palmyra Atoll	UM	UMI	581
Panama	PA	PAN	591
Papua New Guinea	PG	PNG	598
Paracel Islands			
Paraguay	PY	PRY	600
Peru	PE	PER	604
Philippines	PH	PHL	608
Pitcairn Islands	PN	PCN	612
Poland	PL	POL	616
Portugal	PT	PRT	620
Puerto Rico	PR	PRI	630
Qatar	QA	QAT	634
Republic of the Congo	CG	COG	178
Reunion	RE	REU	638
Romania	RO	ROU	642
Ross Sea			
Russia	RU	RUS	643
Rwanda	RW	RWA	646
Saint Helena	SH	SHN	654
Saint Kitts and Nevis	KN	KNA	659
Saint Lucia	LC	LCA	662
Saint Pierre and Miquelon	PM	SPM	666
Saint Vincent and the Grenadines	VC	VCT	670
Samoa	WS	WSM	882
San Marino	SM	SMR	674
Sao Tome and Principe	ST	STP	678
Saudi Arabia	SA	SAU	682
Senegal	SN	SEN	686
Serbia	RS	SRB	688
Serbia and Montenegro			
Seychelles	SC	SYC	690
Siam			
Sierra Leone	SL	SLE	694
Singapore	SG	SGP	702
Sint Maarten	SX	SXM	534
Slovakia	SK	SVK	703
Slovenia	SI	SVN	705
Solomon Islands	SB	SLB	90
Somalia	SO	SOM	706
South Africa	ZA	ZAF	710
South Georgia and the South Sandwich Islands	GS	SGS	239
South Korea	KR	KOR	410
#South Sudan	SS	SSD	
Southern Ocean			
Spain	ES	ESP	724
Spratly Islands			
Sri Lanka	LK	LKA	144
Sudan	SD	SDN	736
Suriname	SR	SUR	740
Svalbard	SJ	SJM	744
Swaziland	SZ	SWZ	748
Sweden	SE	SWE	752
Switzerland	CH	CHE	756
Syria	SY	SYR	760
Taiwan	TW	TWN	158
Tajikistan	TJ	TJK	762
Tanzania	TZ	TZA	834
Tasman Sea			
Thailand	TH	THA	764
Togo	TG	TGO	768
Tokelau	TK	TKL	772
Tonga	TO	TON	776
Trinidad and Tobago	TT	TTO	780
Tromelin Island	TF	ATF	260
Tunisia	TN	TUN	788
Turkey	TR	TUR	792
Turkmenistan	TM	TKM	795
Turks and Caicos Islands	TC	TCA	796
Tuvalu	TV	TUV	798
Uganda	UG	UGA	800
Ukraine	UA	UKR	804
United Arab Emirates	AE	ARE	784
United Kingdom	GB	GBR	826
USA	US	USA	840
USSR			
Uruguay	UY	URY	858
Uzbekistan	UZ	UZB	860
Vanuatu	VU	VUT	548
#Vatican	VA	VAT	336
Venezuela	VE	VEN	862
Viet Nam	VN	VNM	704
Virgin Islands	VI	VIR	850
Wake Island	UM	UMI	581
Wallis and Futuna	WF	WLF	876
West Bank	PS	PSE	275
Western Sahara	EH	ESH	732
Yemen	YE	YEM	887
Yugoslavia			
Zaire			
Zambia	ZM	ZMB	894
Zimbabwe	ZW	ZWE	716
