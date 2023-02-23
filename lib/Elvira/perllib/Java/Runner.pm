# $Id: Runner.pm 6247 2012-05-04 19:39:36Z pedworth $
#
# File: JavaRunner.pm
# Authors: Jeff Sitz
#
#  Copyright @ 2008, J. Craig Venter Institute (JCVI).  All rights reserved.
#
# A Perl object for executing Java applications
#

=head1 NAME

JavaRunner - A Perl object for executing Java applications

=head1 SYNOPSIS

use Java::Runner;

=head1 DESCRIPTION

A Perl object for executing Java applications

=cut

package Java::Runner;

use strict;
use warnings;

use IO::Dir;
use Cwd qw( realpath );
use POSIX qw(uname);

## configuration management 
our $JAVARUNNER_VERSION = "1.0.0";
our $JAVARUNNER_BUILD = (qw/$Revision: 6247 $/ )[1];
our $JAVARUNNER_VERSION_STRING = "v$JAVARUNNER_VERSION Build $JAVARUNNER_BUILD";

# #########################################################################
#
#    Export Variables and Routines
#
# #########################################################################
BEGIN 
{
    use Exporter ();
    use vars qw(@EXPORT @EXPORT_OK @ISA %EXPORT_TAGS);

    @ISA         = qw(Exporter);
    @EXPORT      = qw( );
    %EXPORT_TAGS = (
                    VERSION => [qw($JAVARUNNER_VERSION $JAVARUNNER_BUILD $JAVARUNNER_VERSION_STRING)],
                    CHECKS => [],
                    UTIL => []
                   );
    @EXPORT_OK   = ();
    
    Exporter::export_tags('VERSION');
    Exporter::export_tags('CHECKS');
    Exporter::export_tags('UTIL');
}

use vars @EXPORT;
use vars @EXPORT_OK;


#
#  Constants
#
use constant PATH_SEP 			=> '/';
use constant DEFAULT_JAVA_HOME 	=> '/usr/local/java';
use constant DEFAULT_JAVA_BIN 	=> 'bin';
use constant DEFAULT_JAVA_EXE 	=> 'java';
use constant SGE_ROOT			=> '/usr/local/sge_current';
use constant JAVA_FIVE			=> '/usr/local/java/1.5.0';
use constant JAVA_SIX			=> '/usr/local/java/1.6.0';
use constant JAVA_SEVEN			=> '/usr/local/java/1.7.0';
use constant JAVA_EIGHT			=> '/usr/local/java/1.8.0';

sub firstExisting(@)
{
    for my $dir (@_) {
        return $dir if (-e $dir);
    }
    return undef;
}

#
#  Paths
#
my %JAVA_HOME = ( '5' => JAVA_FIVE,
                  '6' => JAVA_SIX,
                  '7' => JAVA_SEVEN,
                  '8' => JAVA_EIGHT,
                  '5-32b' => JAVA_FIVE,
                  '6-32b' => JAVA_SIX
                );

sub getMacJavaHomePath($$) {
	 my ($self, $version) = @_;
	 
	return `/usr/libexec/java_home -v 1.$version`;
}

sub new {
    my $pkg = shift();
    my $self = {'mainClass' => undef,
                'mainJar' => undef,
                'nativeLibPath' => [],
                'classLoc' => [],
                'params' => [],
                'properties' => {},
                'maxHeap' => undef,
                'initHeap' => undef,
                
                'javaHome' => undef,
                'javaBin' => undef,
                'javaExe' => undef,
                'useGCOverheadLimit' =>1,
                @_
               };

    bless ($self, $pkg);
    
    $self->setJVMFromEnvironment();
    $self->setClasspathFromEnvironment();
    $self->setNativeLibraryPathFromEnvironment();

    return $self;
}
sub disableGCOverheadLimit($) {
	my $self = @_;
	$self->{useGCOverheadLimit} = undef;
}
sub enableGCOverheadLimit($){
	my $self = @_;
	$self->{useGCOverheadLimit} = 1;
}
sub setJVMFromEnvironment($) {
    my ($self) = @_;
    
    my $jvmHome = ($ENV{JAVA_HOME} || $ENV{JAVA_ROOT} || $ENV{JRE_HOME} || $ENV{JDK_HOME} || $ENV{SDK_HOME});
    
    if (defined($jvmHome)) {
    	$self->javaHome($jvmHome);
    }
    if (defined($ENV{JAVA_BINDIR})) {
    	$self->javaBin($ENV{JAVA_BINDIR});
    }
    if (defined($ENV{JAVA_EXE})) {
    	$self->javaExe($ENV{JAVA_EXE});
    }
}

sub setClasspathFromEnvironment($) {
    my ($self) = @_;
    
    my %loc_set = ();
    
    if (defined $ENV{CLASSPATH}) {
        for my $loc (split(/:/, $ENV{CLASSPATH})) {
            unless (exists $loc_set{$loc}) {
                $self->addClassLocation($loc);
                $loc_set{$loc} = 1;
            }
        }      
    }
}

sub setNativeLibraryPathFromEnvironment($) {
    my ($self) = @_;
    
    my %path_set = ();
    
    if (defined $ENV{LD_LIBRARY_PATH}) {
        for my $path (split(/:/, $ENV{LD_LIBRARY_PATH})) {
            unless (exists $path_set{$path}) {
                $self->addNativeLibraryPath($path);
                $path_set{$path} = 1;
            }
        }      
    }
}

sub addNativeLibraryPath($$) {
    my ($self, $path) = @_;
    
    push(@{$self->{nativeLibPath}}, $path);
}

sub clearNativeLibraryPath($) {
    my ($self) = @_;
    
    $self->{nativeLibPath} = [];
}

sub useJavaPreset($$) {
    my ($self, $preset) = @_;
    my $home;

    # check if an existing java install is okay
    $home = $self->checkEnvironmentVersion($preset);

    if (! $home ) {
	#dkatzel July 2014
	#JCVI Macs have different path to java than Linux
	my @uname = uname();
	
	## This is gonna to break at next Mac upgrade. But I do not have any
	## good alternative right now and, anyhow, 
	## I hate Macs - 20160513 pamedeo
    	# is a mac machine    
	if($uname[0] eq 'Darwin') {
	    chomp($home = $self->getMacJavaHomePath($preset));
	}
	else {
	    $home = $JAVA_HOME{$preset};
	}

    }
    $self->javaHome($home) if (defined $home);
}

sub checkEnvironmentVersion($$)
{
    my ($self, $preset) = @_;
    if ($ENV{JAVA_HOME})
    {
	my $versionString = `$ENV{JAVA_HOME}/bin/java -version 2>&1`;
	my ($majorVersion,) = ($versionString =~ /version "1\.(\d)\./);
	if ($majorVersion eq $preset)
	{
	    return $ENV{JAVA_HOME}
	}
    }
}

sub javaHome($$) {
    my ($self, $home) = @_;
    
    if (defined $home) {
        $self->{javaHome} = $home;
        $self->{javaBin} = undef;
        $self->{javaExe} = undef;
    }
    unless (defined($self->{javaHome})) {
    	$self->{javaHome} = DEFAULT_JAVA_HOME;
    }
    unless (-d $self->{javaHome}) {
    	die "Unable to find Java home directory (\"$self->{javaHome}\") or the path is not accessible to the current user.\n\n";
    }
   	return $self->{javaHome};
}

sub javaBin($$) {
    my ($self, $bin) = @_;
    
    if (defined $bin) {
        $self->{javaBin} = $bin;
        $self->{javaExe} = undef;
    }
    unless (defined($self->{javaBin})) {
    	$self->{javaBin} = $self->javaHome() . PATH_SEP . DEFAULT_JAVA_BIN;
    }
    unless (-d $self->{javaBin}) {
    	die "Unable to find Java bin directory (\"$self->{javaBin}\") or the path is not accessible to the current user.\n\n";
    }
   	return $self->{javaBin};
}

sub javaExe($$) {
    my ($self, $exe) = @_;
    
    if (defined($exe)) {
    	$self->{javaExe} = $exe;
    }
    unless (defined ($self->{javaExe})) {
    	$self->{javaExe} = $self->javaBin() . PATH_SEP . DEFAULT_JAVA_EXE;
    }
    unless (-x $self->{javaExe}) {
    	die "Unable to find java in the supplied location (\"$self->{javaExe}\") or the file is not executable by the current user.\n\n";
    }
	return $self->{javaExe}
}

sub mainClass($$) {
    my ($self, $newMain) = @_;
    
    if (defined($newMain)) {
    	$self->{mainClass} = $newMain;
    }
    return $self->{mainClass};
}

sub mainJar($$) {
    my ($self, $newMain) = @_;
    
    if (defined($newMain)) {
    	$self->{mainJar} = $newMain;
    }
    return $self->{mainJar};
}

sub initialHeapSize($$) {
    my ($self, $size) = @_;
    
    if (defined($size)) {
    	$self->{initHeap} = $size;
    }
    return $self->{initHeap};
}

sub maxHeapSize($$) {
    my ($self, $size) = @_;
    
    if (defined($size)) {
    	$self->{maxHeap} = $size;
    }
    return $self->{maxHeap};
}

sub addParameters($@) {
    my ($self, @params) = @_;
    
    push(@{$self->{params}}, @params);
}

sub parameters($) {
    my ($self) = @_;
    
    return @{$self->{params}};
}

sub addClassLocation($$) {
    my ($self, $path) = @_;
    
    push(@{$self->{classLoc}}, realpath($path));
}

sub addJarDirectory($$) {
    my ($self, $jardir) = @_;
    
    my %jars = ();
    
    my $dir = new IO::Dir;
    if ($dir->open($jardir)) {
        while( my $filename = $dir->read()) {
            my $path = $jardir . '/' . $filename;
            
            if (-d $path) {
            	next;
            }
            $self->addClassLocation($path);
        }
    }
    else {
        print STDERR "Failed to open JAR directory: $jardir\n";
    }
    foreach my $jar (sort { lc($a) cmp lc($b) } keys %jars) {
        print("+ Adding $jar\n");
        $self->addClassLocation($jar);
    }
}

sub clearClassPath($) {
    my ($self) = @_;
    $self->{classLoc} = [];
}

sub getClassPath() {
    my ($self) = @_;
    return join(':', @{$self->{classLoc}});
}

sub setProperty($$$) {
    my ($self, $property, $value) = @_;
    $self->{properties}{$property} = $value;
}

sub properties($) {
    my ($self) = @_;
    return %{$self->{properties}};
}

sub getCommandArgumentList($) {
    my ($self) = @_;
    my @args = ();
    
    # Add the JVM executable
    push(@args, $self->javaExe());
    
    # Add JVM options
    if (defined $self->{initHeap}) {
    	my $init_heap;
    	
    	if ($self->{initHeap} =~ /[KkmMgGtT]\s*$/) {
    		$init_heap = "-Xms$self->{initHeap}";
    	}
    	elsif ($self->{initHeap} =~ /^\s*\d+\s*$/) {
    	   $init_heap = sprintf("-Xms%dm", $self->{initHeap});
    	}
    	else {
    		die "Unrecognized value for 'initMap' (\"$self->{initHeap}\").\n";
    	}
    	push(@args, $init_heap);
    }
    if (defined $self->{maxHeap}) {
    	my $max_heap;
    	
    	if ($self->{maxHeap} =~ /[KkmMgGtT]\s*$/) {
    		$max_heap = "-Xmx$self->{maxHeap}";
    	}
    	elsif ($self->{maxHeap} =~ /^\s*\d+\s*$/) {
    		$max_heap = sprintf("-Xmx%dm", $self->{maxHeap});
    	}
    	else {
    		die "Unrecognized value for 'maxHeap' (\"$self->{maxHeap}\")\n";
    	}
        push(@args, $max_heap);
    }
    #turn off GC Overhead Limit?
    if($self->{useGCOverheadLimit}) {
    	push(@args, "-XX:-UseGCOverheadLimit");
    }
    # Add the classpath
    if (scalar(@{$self->{classLoc}}) > 0) {
        push(@args, "-classpath");
        push(@args, $self->getClassPath());
    }
    # Add properties
    for my $prop (keys %{$self->{properties}}) {
        my $value = $self->{properties}{$prop};
        push(@args, ("-D" . $prop . '=' . ($value or "")));
    }
    # Add from environment
    if ($ENV{JAVA_OPTS})
    {
	push(@args,split(/\s+/,$ENV{JAVA_OPTS}));
    }
    
    # Add the execution target (main jar or main class)
    if (defined $self->{mainJar}) {
        push(@args, "-jar");
        push(@args, $self->{mainJar});
    }
    elsif (defined $self->{mainClass}) {
        push(@args, $self->{mainClass});
    }
    else {
        return undef;
    }
    # Add the execution parameters
    push(@args, $self->parameters());
    return @args;
}

sub execute($) {
    my ($self) = @_;
    
    # Set the LD_LIBRARY_PATH
    if (scalar(@{$self->{nativeLibPath}}) > 0) {
        my $ld_lib_path = join(':', @{$self->{nativeLibPath}});
        $ENV{LD_LIBRARY_PATH} = $ld_lib_path;
    }
    else {
        delete $ENV{LD_LIBRARY_PATH};
    }
    my @args = $self->getCommandArgumentList();
    exec(@args);
}

sub getCommandLine($) {
    my ($self) = @_;
    my $cmd = join(' ', $self->getCommandArgumentList());
    return $cmd;
}

sub addJcviGridSupport() {
	my ($self) = @_;
	
	$ENV{SGE_ROOT} = SGE_ROOT;
	
	# Discover the client architecture
	chomp(my $sge_arch = `$ENV{SGE_ROOT}/util/arch`);

	$self->addNativeLibraryPath(SGE_ROOT . "/lib/$sge_arch");
}

1;
