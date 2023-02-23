# $Id$
#
# File: GLKLib.pm
# Authors: Jeff Sitz, Dan Katzel, Paolo Amedeo
#
#  Copyright @ 2005, The Institute for Genomic Research (TIGR).  All
#  rights reserved.
#
# Reusable GLK access subroutines
#

=head1 NAME

GLKLib - A library of perl routines for retrieving information from the GLK
database schema.

=head1 SYNOPSIS

use TIGR::GLKLib;

=head1 DESCRIPTION

A library of perl routines for retrieving information from the GLK database
schema.

=cut

package TIGR::GLKLib;

use strict;
use warnings;
use DBI;
#use TIGR::Foundation;
use TIGR::EUIDService;
use Time::ParseDate;
use DateTime;
use File::Basename;
use Data::Dumper;
use Sys::Hostname;
use ProcessingObjects::SafeIO;
use Cwd (qw(abs_path getcwd));

## configuration management
our $VERSION = "1.62";
#our $BUILD = (qw/$Revision$/ )[1];
#our $VERSION_STRING = "v$VERSION Build $BUILD";
our @DEPEND = ();

# #########################################################################
#
#    Export Variables and Routines
#
# #########################################################################
BEGIN {
    use Exporter ();
    use vars (qw(@EXPORT @EXPORT_OK @ISA %EXPORT_TAGS));

    @ISA         = (qw(Exporter));
    @EXPORT      = (qw($GLKLIB_VERSION @DEPEND));
    %EXPORT_TAGS = ();
    @EXPORT_OK   = ();
}

use vars @EXPORT;
use vars @EXPORT_OK;

# Set some exported variables
$GLKLIB_VERSION = $VERSION;
my $dt = DateTime->now();
$dt->set_time_zone('local');
my $silent_log_file = join('_', '/usr/local/scratch/VIRAL/GLKLibLog/SilentWarnings', $dt->ymd(''), $dt->hms() , $$ . '.log');
my $bail_count = 0; # In order to avoid deep recursions in bail()

# #########################################################################
#
#    GLOBAL constants
#
# #########################################################################
my $DBTYPE = "Sybase";
use constant TRUE                   => 1;
use constant FALSE                  => 0;
use constant SUCCESS                => 1;
use constant FAILURE                => 0;
use constant SEG_NAME_TYPE_MAP_PATH => '/usr/local/projdata/700010/projects/Elvira/etc';
use constant SEG_MAP_FILE_SUFFIX    => '.regionNumbers.properties';
use constant DEBUG                  => 0; # Setting it to a non-zero value, it triggers the writing of logInfo() messages to the log file
use constant ONLY_AT_THIS_LEVEL     => 'ignore_above';
use constant FIRST_FOUND_ABOVE      => 'stop_at_first';
use constant COMBINE_TOP_DOWN       => 'concatenate';
use constant COMBINE_BOTTOM_UP      => 'concatenate_bottom_up';
use constant MERGE_ON_PLACEHOLDER   => 'merge_on_placeholder';
use constant FNF_AUTHOR_SEPARATOR   => '; ';
use constant FNF_INITIALS_SEPARATOR => ' ';
use constant LNF_AUTHOR_SEPARATOR   => ', ';
use constant LNF_INITIALS_SEPARATOR => '.';
use constant LNF_LAST_N_SEPARATOR   => ',';
use constant ATTR_COMBINE_SEPARATOR => '; ';
use constant EXT_ATTR_UNKNOWN_VAL   => 'Unknown';
use constant EXT_ATTR_MISSING_VAL   => 'Missing';
use constant EXT_ATTR_NOT_APPL_VAL  => 'Not applicable';
use constant BIOPROJECT_ID_PREFIX   => 'PRJNA';

my %eat_allowing_unknown = (age                              => undef,
                            collection_date                  => undef,
                            country                          => undef,
                            detection_method                 => undef,
                            disease_course                   => undef,
                            district                         => undef,
                            drug_dosage                      => undef,
                            drug_generic_chemical_name       => undef,
                            drug_method_of_delivery          => undef,
                            environmental_material           => undef,
                            gender                           => undef,
                            genotype                         => undef,
                            host                             => undef,
                            host_common_name                 => undef,
                            host_disease                     => undef,
                            host_disease_outcome             => undef,
                            host_ethnicity                   => undef,
                            host_health_status               => undef,
                            host_species                     => undef,
                            onset_abdominal_pain             => undef,
                            onset_chills                     => undef,
                            onset_cough                      => undef,
                            onset_dehydration                => undef,
                            onset_diarrhea                   => undef,
                            onset_disorientation             => undef,
                            onset_fever                      => undef,
                            onset_headache                   => undef,
                            onset_lethargy                   => undef,
                            onset_myalgia                    => undef,
                            onset_runny_nose                 => undef,
                            onset_speed_abdominal_pain       => undef,
                            onset_speed_chills               => undef,
                            onset_speed_cough                => undef,
                            onset_speed_dehydration          => undef,
                            onset_speed_diarrhea             => undef,
                            onset_speed_disorientation       => undef,
                            onset_speed_fever                => undef,
                            onset_speed_headache             => undef,
                            onset_speed_lethargy             => undef,
                            onset_speed_myalgia              => undef,
                            onset_speed_runny_nose           => undef,
                            onset_speed_vomiting             => undef,
                            onset_vomiting                   => undef,
                            original_virus_name              => undef,
                            other_diseases                   => undef,
                            passage_date                     => undef,
                            passage_history                  => undef,
                            severity_abdominal_pain          => undef,
                            severity_chills                  => undef,
                            severity_cough                   => undef,
                            severity_dehydration             => undef,
                            severity_diarrhea                => undef,
                            severity_disorientation          => undef,
                            severity_fever                   => undef,
                            severity_headache                => undef,
                            severity_lethargy                => undef,
                            severity_myalgia                 => undef,
                            severity_runny_nose              => undef,
                            severity_vomiting                => undef,
                            site_of_vaccination              => undef,
                            specimen_site                    => undef,
                            suspected_organism_pathogenicity => undef,
                            symptom_abdominal_pain           => undef,
                            symptom_chills                   => undef,
                            symptom_cough                    => undef,
                            symptom_dehydration              => undef,
                            symptom_diarrhea                 => undef,
                            symptom_disorientation           => undef,
                            symptom_fever                    => undef,
                            symptom_headache                 => undef,
                            symptom_lethargy                 => undef,
                            symptom_myalgia                  => undef,
                            symptom_runny_nose               => undef,
                            symptom_vomiting                 => undef,
                            temperature_measured_method      => undef,
                            temperature_measured_value       => undef,
                            vaccine_type                     => undef,
                            weight_measured_method           => undef);

my %eat_allowing_na =  (age                                 => undef,
                        disease_course                      => undef,
                        environmental_material              => undef,
                        gender                              => undef,
                        host                                => undef,
                        host_common_name                    => undef,
                        host_disease                        => undef,
                        host_disease_outcome                => undef,
                        host_ethnicity                      => undef,
                        host_health_status                  => undef,
                        host_species                        => undef,
                        onset_abdominal_pain                => undef,
                        onset_chills                        => undef,
                        onset_cough                         => undef,
                        onset_dehydration                   => undef,
                        onset_diarrhea                      => undef,
                        onset_disorientation                => undef,
                        onset_fever                         => undef,
                        onset_headache                      => undef,
                        onset_lethargy                      => undef,
                        onset_myalgia                       => undef,
                        onset_runny_nose                    => undef,
                        onset_speed_abdominal_pain          => undef,
                        onset_speed_chills                  => undef,
                        onset_speed_cough                   => undef,
                        onset_speed_dehydration             => undef,
                        onset_speed_diarrhea                => undef,
                        onset_speed_disorientation          => undef,
                        onset_speed_fever                   => undef,
                        onset_speed_headache                => undef,
                        onset_speed_lethargy                => undef,
                        onset_speed_myalgia                 => undef,
                        onset_speed_runny_nose              => undef,
                        onset_speed_vomiting                => undef,
                        onset_vomiting                      => undef,
                        severity_abdominal_pain             => undef,
                        severity_chills                     => undef,
                        severity_cough                      => undef,
                        severity_dehydration                => undef,
                        severity_diarrhea                   => undef,
                        severity_disorientation             => undef,
                        severity_fever                      => undef,
                        severity_headache                   => undef,
                        severity_lethargy                   => undef,
                        severity_myalgia                    => undef,
                        severity_runny_nose                 => undef,
                        severity_vomiting                   => undef,
                        site_of_vaccination                 => undef,
                        specimen_site                       => undef,
                        suspected_organism_pathogenicity    => undef,
                        symptom_abdominal_pain              => undef,
                        symptom_chills                      => undef,
                        symptom_cough                       => undef,
                        symptom_dehydration                 => undef,
                        symptom_diarrhea                    => undef,
                        symptom_disorientation              => undef,
                        symptom_fever                       => undef,
                        symptom_headache                    => undef,
                        symptom_lethargy                    => undef,
                        symptom_myalgia                     => undef,
                        symptom_runny_nose                  => undef,
                        symptom_vomiting                    => undef);

my %eat_allowing_missing = (country      => undef,
                            host_disease => undef);

my $auth_plchold_regex = qr/\s*##Additional_Authors##[;,]?/;

my %finishing_grade = (1 => [0,1,1,1],    ## [internal gaps absent(0)/present(1), partial(0)/complete(1), draft(0)/finished(1), default submit_annotation]
                       2 => [0,1,0,1],
                       3 => [1,1,0,0],
                       4 => [0,0,1,1],
                       5 => [0,0,0,1],
                       6 => [1,0,0,0],
                       7 => [0,0,0,0],    ## Segments < 50% expected length, no internal gaps
                       8 => [1,0,0,0]);   ## Segments < 50% expected length, with internal gaps


# #########################################################################
#
#    TIGR EUID Service
#
# #########################################################################
my $euid_service = new TIGR::EUIDService;
$euid_service->setBlockSize(1);

sub getEUID {
    return $euid_service->getEUID()
}

# #########################################################################
#
#    QUERY Table
#
# #########################################################################
my %QUERIES =
    (

# -------------------------------------------------------------
#  Type Caching Queries
# -------------------------------------------------------------

LOAD_EXTENT_TYPES =>
'SELECT t.Extent_Type_id, t.type
FROM Extent_Type t',

LOAD_SEQREAD_TYPES =>
'SELECT t.SequenceReadType_id, t.type
FROM SequenceReadType t',

LOAD_SEQREADATTR_TYPES =>
'SELECT t.SequenceReadAttributeType_id, t.type
FROM SequenceReadAttributeType t',

LOAD_EXTENT_ATTR_TYPES =>
'SELECT t.ExtentAttributeType_id, t.type, t.value_type, t.description, t.combining_rule
FROM vir_common..ExtentAttributeType t',

LOAD_TRIMSEQ_ATTR_TYPES =>
'SELECT t.TrimSequenceAttributeType_id, t.name
FROM TrimSequenceAttributeType t',

#LOAD_LIB_STAT_TYPES =>
#'SELECT t.Library_Stats_Template_id AS id, t.tag AS name
#FROM Library_Stats_Template t',

LOAD_EXTENT_ATTRIBUTE_PLACE =>
'SELECT ExtAttrPlace_id, Extent_Type_id, ExtentAttributeType_id
FROM vir_common..ExtAttrPlace',

LOAD_VC_COLLECTIONS =>
'SELECT collection_id, Extent_id, db
FROM vir_common..Collection',

#DELETE_TRIMSEQ_ATTR =>
#'DELETE TrimSequenceAttribute
#WHERE TrimSequence_id = ? AND TrimSequenceAttributeType_id = ?',

LOAD_BIOPROJECTS =>
'SELECT BioProject_id, locus_tag_prefix, project_aim, project_title, is_umbrella
FROM vir_common..BioProject',

LOAD_DEPRECATED =>
'SELECT Extent_id
FROM vir_common..deprecated',

GET_DB_NAMES =>
'SELECT g.db
FROM common..genomes g, master..sysdatabases s
WHERE g.type = "VGD" AND g.db = s.name',

DELETE_EXT_ATTR_PLACE_BY_ET_EAT =>
'DELETE vir_common..ExtAttrPlace
WHERE Extent_Type_id = ? AND ExtentAttributeType_id = ?',

DELETE_EXT_ATTR_PLACE_BY_EAP_ID =>
'DELETE vir_common..ExtAttrPlace
WHERE ExtAttrPlace_id = ?',

UNDEPRECATE_EXTENT_VC =>
'DELETE vir_common..deprecated
WHERE Extent_id = ?',

GET_SAMPLE_INFO_BY_BLINDED_NUMBER_VC =>
'SELECT sample_id, lib_id, db
FROM vir_common..Sample_ID
WHERE blinded_number = ?',

GET_SAMPLE_INFO_BY_BAC_ID_VC =>
'SELECT lib_id, db, blinded_number
FROM vir_common..Sample_ID
WHERE sample_id = ?',

# -------------------------------------------------------------
#  Type Creation Queries
# -------------------------------------------------------------

#CREATE_EXTENT_TYPE =>
#'INSERT INTO Extent_Type (type, description)
#VALUES (?, ?)',

#CREATE_SEQREAD_TYPE =>
#'INSERT INTO SequenceReadType (type)
#VALUES (?)',

#ADD_EXTENT_ATTR_TYPE =>
#'INSERT INTO vir_common..ExtentAttributeType (type)
#VALUES (?)',

#ADD_TRIMSEQ_ATTR_TYPE =>
#'INSERT INTO TrimSequenceAttributeType (name)
#VALUES (?)',

#CREATE_SEQREAD_ATTR_TYPE =>
#'INSERT INTO SequenceReadAttributeType (type)
#VALUES (?)',

ADD_EXT_ATTR_PLACE =>
'INSERT INTO vir_common..ExtAttrPlace (Extent_Type_id, ExtentAttributeType_id)
VALUES (?, ?)',

ADD_VC_COLLECTION =>
'INSERT INTO vir_common..Collection (collection_id, db, Extent_id)
VALUES (?, ?, ?)',

# -------------------------------------------------------------
#  Extent Queries
# -------------------------------------------------------------

GET_EXTENT_ROOT =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.parent_id = NULL',

EXISTS_EXTENT =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.Extent_id = ?',

GET_EXTENT_INFO =>
'SELECT e.Extent_id, e.ref_id, e.parent_id, e.Extent_Type_id, e.description
FROM Extent e
WHERE e.Extent_id = ?',

#GET_EXTENT_BY_TYPE_REF =>
#'SELECT e.Extent_id
#FROM Extent e
#WHERE e.Extent_Type_id = ? AND e.ref_id = ?',

#TODO Track down this redundancy
GET_EXTENT_BY_REF_TYPEID =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.Extent_Type_id = ? AND e.ref_id = ?',

GET_EXTENTS_BY_TYPEID =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.Extent_Type_id = ?',

ADD_EXTENT =>
'INSERT INTO Extent (Extent_id, ref_id, parent_id, Extent_Type_id, description)
VALUES (?, ?, ?, ?, ?)',

SET_EXTENT_PARENT =>
'UPDATE Extent
SET parent_id = ?
WHERE Extent_id = ?',

#SET_EXTENT_DESC =>
#'UPDATE Extent
#SET description = ?
#WHERE Extent_id = ?',

#GET_SEGMENT_BY_BAC_AND_NAME =>
#'SELECT DISTINCT(Extent_id)
#FROM current_asmbl_Extent
#WHERE bac_id = ?
#AND segment_name = ?',

GET_SEG_NAME_BY_ASMBL_ID =>
'SELECT segment_name
FROM current_asmbl_Extent
WHERE asmbl_id = ?',

GET_SAMPLE_EXTENTS_BY_BATCH_ID =>
'SELECT e.Extent_id
FROM Extent e, Extent_Type et, ExtentAttribute ea, vir_common..ExtentAttributeType eat
WHERE eat.type = "batch_id"
AND eat.ExtentAttributeType_id = ea.ExtentAttributeType_id
AND ea.value = ?
AND ea.Extent_id = e.Extent_id
AND e.Extent_Type_id = et.Extent_Type_id
AND et.type = "SAMPLE"',

DEPRECATE_EXTENT_VC =>
'INSERT vir_common..deprecated
(Extent_id)
VALUES (?)',

# -------------------------------------------------------------
#  Extent Tree Queries
# -------------------------------------------------------------

GET_EXTENT_PARENT =>
'SELECT e.parent_id
FROM Extent e
WHERE e.Extent_id = ?',

GET_EXTENT_CHILDREN_BY_ID =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.parent_id = ?',

GET_EXTENT_CHILDREN_BY_TYPE =>
'SELECT e.Extent_id
FROM Extent e
WHERE e.parent_id = ? AND e.Extent_Type_id = ?',

#GET_EXTENT_CHILD_BY_DESC =>
#'SELECT e.Extent_id
#FROM Extent e
#WHERE e.parent_id = ? AND description = ?',

# -------------------------------------------------------------
#  Extent Attribute Queries
# -------------------------------------------------------------

GET_EXTENT_BY_ATTRVAL =>
'SELECT Extent_id
FROM ExtentAttribute
WHERE ExtentAttributeType_id = ? and value = ?',

GET_EXTENT_ATTR =>
'SELECT a.value
FROM ExtentAttribute a
WHERE a.Extent_id = ? and a.ExtentAttributeType_id = ?',

HAS_EXTENT_ATTR =>
'SELECT 1 WHERE EXISTS (
SELECT ea.value
FROM ExtentAttribute ea
WHERE Extent_id = ? AND ea.ExtentAttributeType_id = ?)',

GET_EXTENT_ATTRS =>
'SELECT a.ExtentAttributeType_id, a.value
FROM ExtentAttribute a
WHERE Extent_id = ?',

ADD_EXTENT_ATTR =>
'INSERT INTO ExtentAttribute (Extent_id, ExtentAttributeType_id, value)
VALUES (?, ?, ?)',

UPDATE_EXTENT_ATTR =>
'UPDATE ExtentAttribute
SET value = ?
WHERE Extent_id = ? AND ExtentAttributeType_id = ?',

DELETE_EXTENT_ATTR =>
'DELETE FROM ExtentAttribute
WHERE Extent_id = ? AND ExtentAttributeType_id = ?',

GET_EXTENT_ATTR_TYPES =>
'SELECT type from vir_common..ExtentAttributeType ',

CHANGE_EXTENT_ATTR_TYPE =>
'UPDATE ExtentAttribute
SET ExtentAttributeType_id = ?
WHERE Extent_id = ?
AND ExtentAttributeType_id = ?',

# -------------------------------------------------------------
#  SequenceRead Queries
# -------------------------------------------------------------

#GET_SEQREAD_INFO =>
#'SELECT s.SequenceRead_id, s.seq_name, t.type, s.SequenceReadType_id, s.strand, s.Extent_id
#FROM SequenceRead s
#JOIN SequenceReadType t ON t.SequenceReadType_id = s.SequenceReadType_id
#WHERE s.SequenceRead_id = ?',

GET_SEQREAD_BY_NAME =>
'SELECT sr.SequenceRead_id
FROM SequenceRead sr
WHERE sr.seq_name = ?',

#GET_SEQREAD_PARENT =>
#'SELECT sr.Extent_id
#FROM SequenceRead sr
#WHERE sr.SequenceRead_id = ?',

GET_SEQREADS_BY_PARENT =>
'SELECT s.SequenceRead_id, s.seq_name
FROM SequenceRead s
WHERE s.Extent_id = ?',

#SET_SEQREAD_PARENT =>
#'UPDATE SequenceRead
#SET Extent_id = ?
#WHERE SequenceRead_id = ?',

#ADD_SEQREAD =>
#'INSERT INTO SequenceRead (SequenceRead_id, seq_name, strand, Extent_id, SequenceReadType_id)
#VALUES (?, ?, ?, ?, ?)',

#GET_SEQUENCE_FEATURE_BY_TYPE =>
#'SELECT f.end5, f.end3
#FROM feature f
#WHERE f.seq_name = ? AND f.feat_type = ?',

#GET_AVG_QUALITY =>
#'SELECT b.avg_quality
#FROM sequence s
#JOIN bases b ON s.id = b.sequence_id  AND b.version = 1
#WHERE s.seq_name = ?',

# -------------------------------------------------------------
#  SequenceRead Mate Queries
# -------------------------------------------------------------

#GET_MATES =>
#'SELECT sr.SequenceRead_id, s.seq_name, s.trash
#FROM SequenceRead  sr
#JOIN sequence s ON sr.seq_name = s.seq_name
#WHERE sr.Extent_id = ? AND sr.strand = ? AND s.trash is null
#ORDER BY s.ed_ln DESC',

# -------------------------------------------------------------
#  SequenceRead Attribute Queries
# -------------------------------------------------------------

#GET_SEQREAD_ATTR =>
#'SELECT a.value
#FROM SequenceReadAttribute a
#WHERE a.SequenceRead_id = ? and a.SequenceReadAttributeType_id = ?',

#HAS_SEQREAD_ATTR =>
#'SELECT 1 WHERE EXISTS (
#SELECT sra.value
#FROM SequenceReadAttribute sra
#WHERE SequenceRead_id = ? AND sra.SequenceReadAttributeType_id = ?)',

#GET_SEQREAD_ATTRS =>
#'SELECT a.SequenceReadAttributeType_id, a.value
#FROM SequenceReadAttribute a
#WHERE SequenceRead_id = ?',

#ADD_SEQREAD_ATTR =>
#'INSERT INTO SequenceReadAttribute (SequenceRead_id, SequenceReadAttributeType_id, value)
#VALUES (?, ?, ?)',

#UPDATE_SEQREAD_ATTR =>
#'UPDATE SequenceReadAttribute
#SET value = ?
#WHERE SequenceRead_id = ? AND SequenceReadAttributeType_id = ?',

#REMOVE_SEQREAD_ATTR =>
#'DELETE SequenceReadAttribute
#HERE SequenceRead_id = ? AND SequenceReadAttributeType_id = ?',

# -------------------------------------------------------------
#  Library Queries
# -------------------------------------------------------------

GET_LIBRARY_BY_LIMSREF =>
'SELECT l.Library_id
FROM Library l
WHERE l.lims_ref = ?',

#GET_LIBRARY_FOR_EXTENT =>
#'SELECT el.Library_id
#FROM Extent_Library el
#WHERE el.Extent_id = ?',

GET_EXTENTS_FOR_LIBRARY =>
'SELECT el.Extent_id
FROM Extent_Library el
WHERE el.Library_id = ?',

GET_LIBRARY_INFO =>
'SELECT *
FROM Library l
WHERE l.Library_id = ?',

#GET_LIBRARY_INFO_BY_EXTENT =>
#'SELECT l.*
#FROM Library l
#JOIN Extent_Library el ON el.Library_id = l.Library_id
#WHERE el.Extent_id = ?',

#ADD_LIBRARY =>
#'INSERT INTO Library (Library_id, Extent_id, lims_ref, nominal_size, description, CloningSystem_id)
#VALUES (?, ?, ?, ?, ?, ?)',

SET_LIBRARY_CLONE_SYS =>
'UPDATE Library
SET CloningSystem_id = ?
WHERE Library_id = ?',

SET_LIBRARY_DESCR =>
'UPDATE Library
SET description = ?
WHERE Library_id = ?',

#LINK_EXTENT_LIBRARY =>
#'INSERT INTO Extent_Library (Extent_id, Library_id)
#VALUES (?, ?)',

#SET_EXTENT_LIBRARY =>
#'UPDATE Extent_Library SET Library_id = ? WHERE Extent_id = ?',

#MOVE_LIBRARY =>
#'UPDATE Library SET Extent_id = ? WHERE Library_id = ?',

#TODO This is deprecated now that Extents have only one child library.
GET_CHILD_LIBRARIES =>
'SELECT l.Library_id
FROM Library l
WHERE Extent_id = ?',

#GET_LIBRARY_EXPERIMENTS =>
#'SELECT le.Library_Experiment_id
#FROM Library_Experiment le
#WHERE Library_id = ?',

#GET_LIBRARY_BY_TRACKID =>
#'SELECT l.Library_id
#FROM Library l
#JOIN track..library tl ON l.lims_ref = tl.cat#
#WHERE tl.lib_id = ?',

# -------------------------------------------------------------
#  CloningSystem Queries
# -------------------------------------------------------------

GET_CLONESYS_BY_NAME =>
'SELECT cs.CloningSystem_id
FROM CloningSystem cs
WHERE cs.name = ?',

#GET_CLONESYS_INFO =>
#'SELECT cs.*
#FROM CloningSystem cs
#WHERE cs.CloningSystem_id = ?',

GET_TRIMSEQ_CLONESYS =>
'SELECT CloningSystem_id
FROM CloningSystemTrimSequence
WHERE TrimSequence_id = ?',

ADD_CLONESYS =>
'INSERT INTO CloningSystem (CloningSystem_id, name, createDate, description)
VALUES (?, ?, GETDATE(), ?)',

#TODO Deprecate this query
#GET_CLONESYS_BY_TRIMSEQ_ATTR =>
#'SELECT DISTINCT CloningSystem_id as clone_sys
#FROM CloningSystemTrimSequence csts
#JOIN TrimSequenceAttribute tsa ON tsa.TrimSequence_id = csts.TrimSequence_id
#WHERE tsa.TrimSequenceAttributeType_id = ? AND tsa.value = ?',

# -------------------------------------------------------------
#  TrimSequence Queries
# -------------------------------------------------------------

GET_TRIMSEQ_BY_NAME =>
'SELECT ts.TrimSequence_id
FROM TrimSequence ts
WHERE ts.name = ?',

#GET_TRIMSEQ_BY_SEQ_ATTR =>
#'SELECT ts.TrimSequence_id
#FROM TrimSequence ts
#JOIN TrimSequenceAttribute tsa ON tsa.TrimSequence_id = ts.TrimSequence_id
#WHERE tsa.TrimSequenceAttributeType_id = ? AND tsa.value = ?
#AND ts.sequence = ?',

#GET_TRIMSEQ_INFO =>
#'SELECT ts.*
#FROM TrimSequence ts
#WHERE ts.TrimSequence_id = ?',

ADD_TRIMSEQ =>
'INSERT INTO TrimSequence (TrimSequence_id, name, direction, sequence)
VALUES (?, ?, ?, ?)',

LINK_TRIMSEQ_CLONESYS =>
'INSERT INTO CloningSystemTrimSequence (CloningSystem_id, TrimSequence_id)
VALUES (?, ?)',

GET_TRIMSEQ_ATTRS =>
'SELECT a.TrimSequenceAttributeType_id, a.value
FROM TrimSequenceAttribute a
WHERE a.TrimSequence_id = ?',

#HAS_TRIMSEQ_ATTR =>
#'SELECT 1 WHERE EXISTS (
#SELECT tsa.value
#FROM TrimSequenceAttribute ea
#WHERE TrimSequence_id = ? AND tsa.TrimSequenceAttributeType_id = ?)',

#SET_TRIMSET_ATTR =>
#'UPDATE TrimSequenceAttribute
#SET value = ?
#WHERE TrimSequence_id = ? AND TrimSequenceAttributeType_id = ?',

ADD_TRIMSEQ_ATTR =>
'INSERT INTO TrimSequenceAttribute (TrimSequence_id, TrimSequenceAttributeType_id, value)
VALUES (?, ?, ?)',

#GET_TRIMSEQ_BY_CLONESYS =>
#'SELECT csts.TrimSequence_id
#FROM CloningSystemTrimSequence csts
#WHERE csts.CloningSystem_id = ?',

# -------------------------------------------------------------
#  Library Policy Queries
# -------------------------------------------------------------

#ADD_LIBRARY_POLICY =>
#'INSERT INTO Library_Policy (Library_id, min_size, max_size, priority, Experiment_id)
#VALUES (?, ?, ?, 1.0, ?)',

GET_LIBRARY_POLICY =>
'SELECT lp.Library_id, lp.min_size, lp.max_size, lp.Experiment_id
FROM Library_Policy lp
WHERE lp.Library_id = ?',

#UPDATE_LIBRARY_POLICY =>
#'UPDATE Library_Policy SET
#min_size = ?,
#max_size = ?,
#Experiment_id = ?
#WHERE Library_id = ?',

# -------------------------------------------------------------
#  Library Experiment Queries
# -------------------------------------------------------------

#ADD_EXPERIMENT =>
#'INSERT INTO Experiment (Experiment_id, comment, rundate, refno, user_name)
#VALUES (?, ?, GETDATE(), 0, ?)',

GET_EXPERIMENT =>
'SELECT Experiment_id, comment, rundate, refno, user_name
FROM Experiment
WHERE Experiment_id = ?',

#GET_EXPERIMENT_BY_COMMENT =>
#'SELECT Experiment_id, comment, rundate, refno, user_name
#FROM Experiment
#WHERE comment = ?',

#ADD_LIBRARY_EXPERIMENT =>
#'INSERT INTO Library_Experiment (Library_Experiment_id, Library_id, Experiment_id)
#VALUES (?, ?, ?)',

#GET_LIBRARY_EXPERIMENT =>
#'SELECT Library_Experiment_id, Library_id, Experiment_id
#FROM Library_Experiment
#WHERE Library_Experiment_id = ?',

# -------------------------------------------------------------
#  Library Stats Queries
# -------------------------------------------------------------

#GET_STATS_TEMPLATE =>
#'SELECT Library_Stats_Template_id, tag, description
#FROM Library_Stats_Template
#WHERE Library_Stats_Template_id = ?',

#GET_STAT_TEMPLATE_ID_FOR_TYPE =>
#'SELECT Library_Stats_Template_id, tag, description
#FROM Library_Stats_Template
#WHERE tag = ?',

#ADD_STATS_TEMPLATE =>
#'INSERT INTO Library_Stats_Template (tag, description, Stats_Type_id)
#VALUES (?, ?, 1)',

#ADD_LIBRARY_STAT =>
#'INSERT INTO Library_Stats (Library_Experiment_id, Library_Stats_Template_id, value)
#VALUES (?, ?, ?)',

#GET_LIBRARY_STAT =>
#'SELECT ls.value AS value
#FROM Library_Experiment le
#JOIN Library_Stats ls ON (le.Library_Experiment_id = ls.Library_Experiment_id)
#WHERE le.Library_id = ? AND le.Experiment_id = ?
#AND ls.Library_Stats_Template_id = ?',

#GET_ALL_LIBRARY_STATS =>
#'SELECT ls.Library_Stats_Template_id AS var, ls.value AS value
#FROM Library_Experiment le
#JOIN Library_Stats ls ON (le.Library_Experiment_id = ls.Library_Experiment_id)
#WHERE le.Library_id = ? AND le.Experiment_id = ?',

# -------------------------------------------------------------
#  Sample Status Queries
# -------------------------------------------------------------

#GET_STATUS_TYPE_BY_EXTENT_ID =>
#'SELECT t.type FROM Status s, StatusType t
#WHERE s.StatusType_id=t.StatusType_id AND s.Extent_id = ?',

#GET_STATUS_INFO_BY_EXTENT_ID =>
#'SELECT s.Extent_id, s.Status_id, s.StatusType_id, t.type, s.Extent_id, s.creator,
#s.create_date, s.description
#FROM Status s, StatusType t
#WHERE s.StatusType_id = t.StatusType_id AND s.Extent_id = ?',#

#GET_ALL_STATUS_INFO =>
#'SELECT s.Extent_id, s.Status_id, s.StatusType_id, t.type, s.Extent_id, s.creator,
#s.create_date, s.description
#FROM Status s, StatusType t
#WHERE s.StatusType_id = t.StatusType_id',

#GET_STATUS_INFO_BY_STATUS_TYPE =>
#'SELECT s.Extent_id, s.Status_id, s.StatusType_id, t.type, s.Extent_id, s.creator,
#s.create_date, s.description
#FROM Status s, StatusType t
#WHERE s.StatusType_id = t.StatusType_id AND t.type = ?',

# CRUD operations on Status table
#ADD_STATUS_TYPE =>
#'INSERT INTO StatusType (StatusType_id, type) VALUES (?, ?)',

#UPDATE_STATUS_TYPE =>
#'UPDATE StatusType SET type = ?',

# -------------------------------------------------------------
#  Extraction and PCR Queries
# -------------------------------------------------------------

GET_EXTRACTIONS_BY_EXTENT_ID =>
'SELECT ExtractionNumber, Date, Method, Technician, Institution, Comments
FROM Extraction
WHERE Extent_id = ?
ORDER BY ExtractionNumber',

GET_EXTRACTION_BY_EXTENT_ID_EXTR_NO =>
'SELECT Date, Method, Technician, Institution, Comments
FROM Extraction
WHERE Extent_id = ? AND ExtractionNumber = ?',

GET_MAX_EXTRACTION_BY_EXTENT_ID =>
'SELECT MAX(ExtractionNumber)
FROM Extraction
WHERE Extent_id = ?',

EXISTS_EXTRACTION_BY_EXTENT_ID_EX_NUM =>
'SELECT COUNT(*)
FROM Extraction
WHERE Extent_id = ? AND ExtractionNumber = ?',

GET_PCRS_BY_EXTENT_ID =>
'SELECT PCRNumber, ExtractionNumber, PcrType, Date, Volume, Concentration, PrimerSet,
PlateLocation, Score, Technician, Comments
FROM PCR
WHERE Extent_id = ?
ORDER BY PCRNumber',

GET_PCR_BY_EXTENT_ID_PCR_NO =>
'SELECT ExtractionNumber, PcrType, Date, Volume, Concentration, PrimerSet,
PlateLocation, Score, Technician, Comments
FROM PCR
WHERE Extent_id = ? AND PCRNumber = ?',

GET_MAX_PCR_BY_EXTENT_ID =>
'SELECT MAX(PCRNumber)
FROM PCR
WHERE Extent_id = ?',

EXISTS_PCR_BY_EXTENT_ID_PCR_NO =>
'SELECT COUNT(*)
FROM PCR
WHERE Extent_id = ? AND PCRNumber = ?',

INSERT_EXTRACTION =>
'INSERT INTO Extraction
(Extent_id, ExtractionNumber, Date, Method, Technician, Institution, Comments)
VALUES (?,?,?,?,?,?,?)',

UPDATE_EXTRACTION =>
'UPDATE Extraction
SET Date = ?,
Method = ?,
Technician = ?,
Institution = ?,
Comments = ?
WHERE Extent_id = ? AND ExtractionNumber = ?',

INSERT_PCR =>
'INSERT INTO PCR
(PCRNumber, Extent_id, ExtractionNumber, PcrType, Date, Volume, Concentration,
PrimerSet, PlateLocation, Score, Technician, Comments)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',

UPDATE_PCR =>
'UPDATE PCR
SET ExtractionNumber = ?,
PcrType = ?,
Date = ?,
Volume = ?,
Concentration = ?,
PrimerSet = ?,
PlateLocation = ?,
Score = ?,
Technician = ?,
Comments = ?
WHERE Extent_id = ? AND PCRNumber = ?',

# -------------------------------------------------------------
#  Plate and Well Queries
# -------------------------------------------------------------
#GET_ALL_PLATES =>
#'SELECT id from Plate',

#GET_PLATE_INFO =>
#'SELECT *
#FROM Plate
#WHERE id= ?',

#GET_PLATE_ID_BY_NAME =>
#'SELECT id
#FROM Plate
#WHERE name= ?',

#GET_ALL_WELLS_BY_PLATE =>
#'SELECT id
#FROM Well
#WHERE Plate_id= ?',

#GET_WELL_INFO =>
#'SELECT *
#FROM Well
#WHERE id= ?',

# -------------------------------------------------------------
#  Converting between Extent_id and Asmbl_id
# -------------------------------------------------------------

GET_SEGMENT_EXTENT_BY_ASMBL =>
'SELECT Extent_id
FROM current_asmbl_Extent
WHERE asmbl_id = ?',

GET_ASMBL_BY_SEGMENT_EXTENT =>
'SELECT asmbl_id
FROM current_asmbl_Extent
WHERE Extent_id = ?',

# -------------------------------------------------------------
#  Assembly Info Queries
# -------------------------------------------------------------

GET_BAC_ID_BY_ASMBL =>
'SELECT bac_id
FROM assembly
WHERE asmbl_id = ?',

#GET_ASMBL_SIZE =>
#'SELECT sequence_datalength
#FROM assembly
#WHERE asmbl_id = ?',

# -------------------------------------------------------------
#  Deprecated Queries
# -------------------------------------------------------------
#TODO Remove these queries and use them only in the scripts that
#actually need them.

#GET_TRACKBAC_SEQS =>
#'SELECT s.seq_name
#FROM track..sample s
#JOIN track..library l ON l.lib_id = s.lib_id
#WHERE l.bac_id = ?',

#GET_UNREPRESENTED_READS =>
#'SELECT s.seq_name
#FROM sequence s
#WHERE NOT EXISTS (SELECT sr.seq_name FROM SequenceRead sr WHERE sr.seq_name = s.seq_name)',

#GET_UNREPRESENTED_READS_BY_BAC =>
#'SELECT s.seq_name
#FROM sequence s
#JOIN track..sample ts ON ts.seq_name = s.seq_name
#JOIN track..library l ON l.lib_id = ts.lib_id
#WHERE l.bac_id = ?
#AND NOT EXISTS (SELECT sr.seq_name FROM SequenceRead sr WHERE sr.seq_name = s.seq_name)'
);


## GLKLib - The code starts here


=head1 MODULE API

TIGR::GLKLib is primarily a function library.  It provides the following
methods as a convenience to the developer.

=cut

# ##############################################################################
#
#    CONSTRUCTORS AND INITIALIZERS
#
# ##############################################################################

=head2 CONSTRUCTORS AND INITIALIZERS

The following functions may be used to construct new instances of the
TIGR::GLKLib wrapper object and initialize some systems

=over

=cut

=over

=item $new_instance = new TIGR::GLKLib($DBI_handle);

This function creates a new instance of a TIGR::GLKLib object.  The
optional DBI handle C<$DBI_handle> in the call allows the user to use an
existing database connection.  If not provided, no database connection
will be made.  There is no checking done on the handle provided.

=back

=cut

sub new {
    my ($pkg, $dbh) = @_;

    my $self = {db => $dbh};
    $self->{inherited_dbh} = defined($dbh) ? 1 : 0; # Distinguishing if the object gets the database connection from somewhere else or not. It influences the behavior in DESTROY()
    bless($self, $pkg);

=comment - Date format:
    Date format: Flu projects use date as yyyy-mm-dd and the dedicated software already handles that.
    Non-flu project use the NCBI format dd-Mmm-yyyy and the transformation is now handled by this module
    both while inserting information into GLK tables and when extracting such information.
    _set_date_format() checks if the database is a flu database or not and set accordingly the 'NCBI_Date' flag.
    The flag is accessible through the NCBI_Date() accessor (both for setting and retrieving a value), and should
    be checked directly or indirectly by any function dealing with ExtentAttributes.
    The first check is to assess if the AttributeType is supposed to contain a date (isDateAttribute()).
=cut

    $self->_set_date_format();
    $self->setQueryLookup(\%QUERIES);
    $self->setWrittenOnlyWarnings(FALSE);
    $self->setAttrValValidation(TRUE);
    $self->_loadDeprecated();
    $self->_loadBioProjects();

    return $self
}

=over

=item $new_instance = TIGR::GLKLib::newConnect($server, $db_name, $username, $password)

This will create a new TIGR::GLKLib object and connect to a database with
the given information.  If the connection fails, the function returns
C<undef>.

=back

=cut

sub newConnect {
    my ($server, $catalog, $user, $password) = @_;

    unless (defined($server) && defined($catalog) && defined($user) && defined($password)) {
        return undef
    }
    my $connect_string = sprintf("dbi:%s:server=%s;database=%s", $DBTYPE, $server, $catalog);
    my $dbh = DBI->connect($connect_string, $user, $password, {PrintError => 0});

    unless (defined($dbh)) {
        return undef
    }
    my $obj = new TIGR::GLKLib($dbh);
    $obj->{db_name} = $catalog;
    $obj->{inherited_dbh} = 0;
    return $obj
}

=over

=item $new_instance = TIGR::GLKLib::newWithDBIh($db_handle);

This function creates a new instance of a TIGR::GLKLib object using a
pre-exising connection to the database.  The C<$db_handle> parameter
is assumed to be an open connection to a GLK-enabled project database.
No checking or verification is performed.

=back

=cut

sub newWithDBIh {
    my ($dbh) = @_;

    my $obj = new TIGR::GLKLib($dbh);
    return $obj
}

=over


=item $glk->setLogger($logger_obj);

This function associates a Log4Perl object with the TIGR::GLKLib
object.  If supplied, logging messages will be sent back through this
object for reporting.

=back

=cut

sub setLogger {
    my ($self, $logger) = @_;

    $self->{logger} = $logger;
}

=comment NO LONGER USED 2017-04-20

=over

=item $glk->tableExists($table_name);

This function checks to see if the given table name exists in the current
database. If it does, it returns the tables Object ID.  If it does not, it
returns C<undef>.  The result of this function is cached so that multiple calls
will not cause multiple queries to the database.

=back

#=cut

sub tableExists {
    my ($self, $table_name) = @_;

    if (exists $self->{'table_exists'}{$table_name}) {
        return $self->{'table_exists'}{$table_name}
    }
    else {
        $self->addQuery('GET_OBJECT_ID', "SELECT OBJECT_ID('%s')", $table_name);
        my $oid;

        if ($self->runQuery('GET_OBJECT_ID')) {
            $oid = $self->fetchSingle('GET_OBJECT_ID');
        }
        $self->endQuery('GET_OBJECT_ID');
        $self->{'table_exists'}{$table_name} = $oid;

        return $oid
    }
}
=cut
=over

=item B<< $glk->_loadAllCollections() >>

It loads all the collections names, Extent_ids, and database from vir_common..Collection

=back

=cut

sub _loadVcCollections {
    my $self = shift();
    if ($self->runQuery('LOAD_VC_COLLECTIONS')) {
        while (my $row = $self->fetchRow('LOAD_VC_COLLECTIONS')) {
            $self->{COLLECTION}{$row->{db}}{$row->{collection_id}} = $row->{Extent_id};
        }
        $self->endQuery('LOAD_VC_COLLECTIONS');
    }
    else {
        $self->bail("_loadCollections() - Problems running the query 'LOAD_VC_COLLECTIONS'");
    }
}

=over

=item B<< $glk->_loadSegNames([$db_name]) >>

It loads the project-specific mapping between segment names and NCBI segment numbers.
It looks for a mapping table located under the directory specified by the constant SEG_NAME_TYPE_MAP_PATH.
If non-univocous/non-bi-univocous mappings are present, only the first one in each direction is considered. Any duplicate will be ignored.
=back

=cut

sub _loadSegNames {
    my ($self, $db_name) = @_;

    if (defined($db_name)) {
        $self->{db_name} = $db_name;
    }

    if (!defined($self->{segMap_file})) {
        if (defined($self->{db_name})) {
            $self->{segMap_file} = SEG_NAME_TYPE_MAP_PATH . "/$self->{db_name}" . SEG_MAP_FILE_SUFFIX;
        }
        else {
            $self->logWarn("loadSegNames() - Unable to establish to which database the program is connected, and no mapping file has been provided. - assuming it is a non-segmented virus.");
        }
    }

    if (defined($self->{segMap_file}) && -e $self->{segMap_file}) {
        open(my $fh, $self->{segMap_file}) || $self->bail("_loadSegNames() - Impossible to open the file \"$self->{segMap_file}\" for reading");
        my %seen;

        while (<$fh>) {
            next if /^#/ || /^\s*$/;
            chomp();
            my ($seg_name, $seg_no) = split/\t/;

            unless (exists($seen{seg_names}{$seg_no})) {
                $self->{seg_names}{$seg_no} = $seg_name;
            }
            undef($seen{seg_names}{$seg_no});

            unless (exists($seen{seg_numbers}{$seg_name})) {
                $self->{seg_numbers}{$seg_name} = $seg_no;
            }
            undef($seen{seg_numbers}{$seg_name});
        }
        close($fh);
    }
    else { # No file found, assuming that we're dealing with an unsegmented virus => seg_name = "MAIN"; seg_number = 1
        $self->{seg_numbers}{MAIN} = 1;
        $self->{seg_names}{1} = 'MAIN';
    }
}

sub _loadCountries {
    my ($self, $country_file) = @_;

    unless (defined($country_file)) {
        ($country_file = $INC{'TIGR/GLKLib.pm'}) =~ s/GLKLib.pm/INSDC_Countries.list/;
    }
    open(my $list, $country_file) || $self->bail("Impossible to open the file \"$country_file\" for reading.");

    while (<$list>) {
        next if /^#/ || /^\s*$/;
        chomp();
        undef($self->{INSDC_Countries}{$_});
    }
    close($list);
}

sub _loadSampleLocationByBlindedNumber {
    my ($self, $bl_no) = @_;

    unless (defined($bl_no)) {
        $self->bail("_loadSampleLocationByBlindedNumber() - Called without a defined blinded_number ");
    }
    if ($self->runQuery('GET_SAMPLE_INFO_BY_BLINDED_NUMBER_VC', $bl_no)) {
        my $r_info = $self->fetchRow('GET_SAMPLE_INFO_BY_BLINDED_NUMBER_VC');

        if (defined($r_info) && scalar(keys(%{$r_info}))) {
            $self->{SAMPLE_ADDRESS}{BID}{$r_info->{sample_id}} = {blinded_number => $bl_no,
                                                                  lib_id         => $r_info->{lib_id},
                                                                  db             => $r_info->{db}};
            $self->{SAMPLE_ADDRESS}{BLINO}{$bl_no} = {sample_id => $r_info->{sample_id},
                                                      lib_id    => $r_info->{lib_id},
                                                      db        => $r_info->{db}};
        }
        else {
            $self->logError("_loadSampleLocationByBlindedNumber() - Unable to find a sample correspondent to blinded_number \"$bl_no\".");
        }
        $self->endQuery('GET_SAMPLE_INFO_BY_BLINDED_NUMBER_VC');
    }
    else {
        $self->bail("_loadSampleLocationByBlindedNumber() - Problems running the query \"GET_SAMPLE_INFO_BY_BLINDED_NUMBER_VC\".");
    }
}


sub _loadSampleLocationByBacID {
    my ($self, $bid) = @_;

    unless (defined($bid) && $bid =~ /^\d+$/) {
        $self->bail("_loadSampleLocationByBacID() - Called without a defined or valid BAC ID");
    }
    if ($self->runQuery('GET_SAMPLE_INFO_BY_BAC_ID_VC', $bid)) {
        my $r_info = $self->fetchRow('GET_SAMPLE_INFO_BY_BAC_ID_VC');

        if (defined($r_info) && scalar(keys(%{$r_info}))) {
            $self->{SAMPLE_ADDRESS}{BLINO}{$r_info->{blinded_number}} = {sample_id => $bid,
                                                                         lib_id    => $r_info->{lib_id},
                                                                         db        => $r_info->{db}};
            $self->{SAMPLE_ADDRESS}{BID}{$bid} = {blinded_number => $r_info->{blinded_number},
                                                  lib_id         => $r_info->{lib_id},
                                                  db             => $r_info->{db}};
        }
        else {
            $self->logError("_loadSampleLocationByBacID() - Unable to find a sample correspondent to BAC ID \"$bid\".");
        }
        $self->endQuery('GET_SAMPLE_INFO_BY_BAC_ID_VC');
    }
    else {
        $self->bail("_loadSampleLocationByBacID() - Problems running the query \"GET_SAMPLE_INFO_BY_BAC_ID_VC\".");
    }
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $glk->setSegMapFile($filename) >>

It stores in the GLKLib object the name of a custom file mapping the viral segment names to the corresponding NCBI segment numbers.
=back

#=cut

sub setSegMapFile {
    my ($self, $segmap_file) = @_;
    $self->_cleanValue(\$segmap_file, 'setSegMapFile()', '$segmap_file');

    if (defined($segmap_file) && -e $segmap_file) {
        $self->{segMap_file} = $segmap_file;
    }
    else {
        $self->bail("setSegMapFile() - Unable to find the specified segment mapping file \"$segmap_file\"");
    }
}
=cut
=over

=item B<< loadExtAttrPlace() >>

Loads all the globally accepted Extent_Type - ExtentAttributeType combinations from vir_common..ExtAttrPlace.
This information is stored in the following two ways:
$self->{ExtAttrPlace_id}{$extent_type_id}{$extent_attribute_type_id} = $ext_attr_place_id
$self->{ExtAttrPlace_combo}{$ext_attr_place_id} = [$extent_type_id, $extent_attribute_type_id];

=back

=cut

sub loadExtAttrPlace {
    my $self = shift();

    $self->runQuery('LOAD_EXTENT_ATTRIBUTE_PLACE');

   while(my $row = $self->fetchRow('LOAD_EXTENT_ATTRIBUTE_PLACE')) {
        my ($et_id, $eat_id, $eap_id) = ($row->{Extent_Type_id}, $row->{ExtentAttributeType_id}, $row->{ExtAttrPlace_id});
        $self->{ExtAttrPlace_id}{$et_id}{$eat_id} = $eap_id;
        $self->{ExtAttrPlace_combo}{$eap_id} = [$et_id, $eat_id];
   }
   $self->endQuery('LOAD_EXTENT_ATTRIBUTE_PLACE');
}

=over

=item B<< loadExtAttrRegister($db) >>

Loads all the database-specific Extent_Type - ExtentAttributeType combinations from ExtAttrRegister.
This information is stored in $self->{ExtAttrRegister}{$extent_type_id}{$extent_attribute_type_id} = $required
If the optional argument database is specified, it will load the values for that database, instead of the current one.
=back

=cut
sub loadExtAttrRegister {
    my ($self, $db) = @_;

    unless (defined($self->{ExtAttrPlace_combo})) {
        $self->loadExtAttrPlace();
    }
    unless (defined($db)) {
        $db = $self->getDbName();

        unless ($self->isVgdDb($db)) {
            $self->bail("loadExtAttrRegister() - Called with a non-VGD database (\"$db\")");
        }
    }
    my $qry_name = "LOAD_${db}_EXTENT_ATTRIBUTE_REGISTER";
    undef($self->{ExtAttrRegister}{$db});
    undef($self->{ExtAttrRegister_EAP}{$db});

    unless (defined($self->{query}{$qry_name})) {
        $self->addQuery($qry_name, "SELECT ExtAttrPlace_id, required FROM $db..ExtAttrRegister");
    }
    if ($self->runQuery($qry_name)) {
        while(my $row = $self->fetchRow($qry_name)) {
            my ($eap_id, $required) = ($row->{ExtAttrPlace_id}, $row->{required});

            if (exists($self->{ExtAttrPlace_combo}{$eap_id}) && defined($self->{ExtAttrPlace_combo}{$eap_id})) {
                my ($et_id, $eat_id) = @{$self->{ExtAttrPlace_combo}{$eap_id}};
                $self->{ExtAttrRegister}{$db}{$et_id}{$eat_id} = $required;
                $self->{ExtAttrRegister_EAP}{$db}{$eap_id}     = $required;
            }
            else {
                $self->bail("loadExtAttrRegister() Database: $db - Unexpected data corruption: table ExtAttrRegister refers to a non-existing vir_common..ExtAttrPlace.ExtAttrPlace_id ($eap_id). The two tables should be linked by FK relationship.");
            }
        }
    }
    $self->endQuery($qry_name);
}


# ##############################################################################
#
#    OBJECT NAME TRANSLATION
#
# ##############################################################################

=back

=head2 OBJECT NAME TRANSLATION

These functions are provided to make calls through the library simpler by
allowing either the calling script or the library itself to be ambiguous
about the format of the input parameters.  These functions attempt to
translate input text into database ID's for function calls.

=over

=cut

#
#  ---------------- Auto Translate Extent Names --------------------
#

=over

=item $extent_id = $glklib_obj->translateExtentName($extent_name)

This function attempts to translate a free-text Extent name to a real
Extent ID.  Currently extent names can be either text representations
of the Extent ID (ie: "104702928102") or the standard TYPE:Ref designation
for the extent (ie: "BAC:31337").  The function returns C<undef> if
no tranlation is found.

=back

=cut

sub translateExtentName {
    my ($self, $name) = @_;

    unless (defined($name)) {
        return undef
    }
    $self->_cleanValue(\$name, 'translateExtentName()', '$name');

    if ($name =~ /^\d{3,5}$/) {
        my $eid;

        # Try to find a SAMPLE
        $eid = $self->getExtentByTypeRef("SAMPLE", $name);

        if (defined($eid)) {
            return $eid
        }
        # Try to find a BAC
        $eid = $self->getExtentByTypeRef("BAC", $name);

        if (defined($eid)) {
            return $eid
        }
        return(undef);
    }
    elsif ($name =~ /^\d+$/) {
        if ($self->extentExists($name)) {
            return $name
        }
        # The ID doesn't exist.
        return undef
    }
    elsif ($name =~ /^(.+)\:(.+)$/) {
        return $self->getExtentByTypeRef($1, $2)
    }
    return undef
}

#
#  ---------------- Auto Translate Extent Types --------------------
#

=over

=item $extent_type_id = $glklib_obj->translateExtentType($type_name)

This function attempts to translate a free-text Extent Type name to
a real Extent Type ID.  Currently, extent type names can be either
text representations of the Extent Type ID (ie: "8") or the text name
of the type (ie: "TRANSPOSON");

=back

=cut

sub translateExtentType {
    my ($self, $type) = @_;

    unless (defined($type)) {
        return undef
    }
    my $typeid;

    if ($type =~ /^\d+$/) {
        $typeid = $type;
    }
    else {
        $self->_cleanValue(\$type, 'translateExtentType', '$type');
        $typeid = $self->getExtentTypeID($type);
    }
    return $typeid
}
#
#  ---------------- Auto Translate Extent Attribute Types --------------------
#

=over

=item $attr_type_id = $glklib_obj->translateExtentAttrType($attr_name)

This function attempts to translate a free-text Extent Type name to
a real Extent Type ID.  Currently, Extent attribute type names can be
either text representations of the Extent Attribute Type ID (ie: "8")
or the text name of the type (ie: "source");

=back

=cut

sub translateExtentAttrType {
    my ($self, $type) = @_;

    unless (defined($type)) {
        $self->logError("translateExtentAttrType() - Called with undefined parameter (ExtentAttributeType)", 1);
        return undef
    }
    $self->_cleanValue(\$type, 'translateExtentAttrType()', '$type');
    my $typeid;

    if ($type =~ /^\d+$/) {
        $typeid = $type;
## TODO: Wait a minute: this doesn't make any sense!!!
        my $att_type = $self->getExtentAttrTypeName($typeid); # Checking that the ExtentAttributeType is valid...

        unless (defined($att_type)) {
            return undef
        }
    }
    else {
        $typeid = $self->getExtentAttrTypeID($type); # The error handling is done at the level of getExtentAttrTypeID()
    }
    return $typeid
}

=comment NO LONGER USED 2017-04-20

#
#  ---------------- Auto Translate Sequence Names --------------------
#

=over

=item $seqread_id = $glklib_obj->translateSeqName($seq_name)

This function attempts to translate a free-text Sequence name to
a real Sequence Read ID.  Currently, sequence names can be either
text representations of the Seuqnce Read ID (ie: "1045586587") or the text
name of the sequence (ie: "IVAA06TFB");

=back

#=cut

sub translateSeqName {
    my ($self, $name) = @_;

    unless (defined($name)) {
        return undef
    }
    my $srid = undef;

    if ($name =~ /^\d+$/) {
        $srid = $name;
    }
    else {
        $self->_cleanValue(\$name, 'translateSeqName()', '$name');
        $srid = $self->getSeqReadByName($name);
    }
    return $srid
}
=cut
#
#  ---------------- Auto Translate Extent Types --------------------
#


=over

=item $seg_no = $glklib_obj->getGBsegmentNumber($seg_name, [$db_name])

This function returns the NCBI segment number correspondent to the segment name (ExtentAttributeType segment_name).
Optionally, it is possible to pass the database name to be used for the mapping.
It returns undef if there is no mapping for that given segment.
Before giving up, it will try removing a lower-case letter at the end of the segment name, to account for possible mixed-infection samples.

=back

=cut

sub getGBsegmentNumber {
    my ($self, $seg_name, $db_name) = @_;

    if ((defined($db_name)) && $db_name =~ /^\S+$/) {
        $self->{db_name} = $db_name;
    }
    unless (exists($self->{seg_numbers}) && defined($self->{seg_numbers})) {
        $self->_loadSegNames();
    }
    $self->_cleanValue(\$seg_name, 'getGBsegmentNumber()', '$seg_name');
    my $seg_id;

    if (exists($self->{seg_numbers}{$seg_name})) {
        $seg_id = $self->{seg_numbers}{$seg_name};
    }
    elsif ($seg_name =~ s/[a-z]$// && exists($self->{seg_numbers}{$seg_name})) {
        $seg_id = $self->{seg_numbers}{$seg_name};
    }
    else {
        $self->logError("getGBsegmentNumber() - Impossible to find a segment number correspondent to \"$seg_name\"", 1);
    }
    return $seg_id
}

=comment NO LONGER USED 2017-04-20

=over

=item $seg_name = $glklib_obj->getSegmentNameByGBnumber($seg_no, [$db_name])

This function returns the segment name (ExtentAttributeType segment_name) correspondent to the NCBI segment number.
Optionally, it is possible to pass the database name to be used for the mapping.
It returns undef if there is no mapping for that given segment.


=back

#=cut

sub getSegmentNameByGBnumber {
    my ($self, $seg_no, $db_name) = @_;

    if ((defined($db_name)) && $db_name =~ /^\S+$/) {
        $self->{db_name} = $db_name;
    }
    unless (exists($self->{seg_names}) && defined($self->{seg_names})) {
        $self->_loadSegNames();
    }
    my $seg_name = $self->{seg_names}{$seg_no};

    unless (defined($seg_name)) {
        $self->logError("getSegmentNameByGBnumber() - Impossible to find a segment name correspondent to \"$seg_no\"", 1);
    }
    return $seg_no
}
=cut


# ##############################################################################
#
#    EXTENTS
#
# ##############################################################################

=back

=head2 EXTENTS

An Extent is an object representing a sequenceable piece of DNA, or a logical
grouping of such pieces which share a common source.

=over

=cut

=over

=item B<< $eid = $glk->getExtentByAttribute($attr, $val, $strict) >>

This function will return an Extent ID for the given attribute, value pair.
If no Extent is found, undef is returned. If multiple Extents are found,
only one is returned.
When the third and optional argument is set to a non-zero value and multiple non-deprecated results are found, the function will return undef, instead of the first element in the list.

=back

=cut

sub getExtentByAttribute {
    my ($self, $type, $val, $strict) = @_;
    $self->_cleanValue(\$type, 'getExtentByAttribute()', '$type');
    $self->_cleanValue(\$val,  'getExtentByAttribute()', '$val');

    my $type_id = $self->getExtentAttrTypeID($type);

    unless (defined($type_id)) {
        $self->logWarn("getExtentByAttribute() - Invalid ExtentAttributeType \"$type\"");
        return undef
    }
    unless ($self->isCorrectValueType($type, $val)) {
        $self->logWarn("getExtentByAttribute() - The value supplied (\"$val\" is not compatible with the attribute type \"$type\". For now the function will continue, but in future it will return undef.");

        ## Not implemented for now, uncomment only when the type verification has been finalized:
        # return undef;
    }
    if ($self->runQuery('GET_EXTENT_BY_ATTRVAL', $type_id, $val)){
        my @results = ();

        foreach my $row (@{$self->fetchAllArrayRef('GET_EXTENT_BY_ATTRVAL')}) {
            push(@results, $row->[0]);
        }
        $self->endQuery('GET_EXTENT_BY_ATTRVAL');

        if (scalar(@results > 1)) { ## If more than one Extent fulfill the original criteria...
            my $message = "getExtentByAttribute() - The query returned " . scalar(@results) . " Extent records having attribute \"$type\" with value \"$val\"\n";

            ## Removing from the list the deprecated samples
            my @deprecated = ();
            my $removed;

            for (my $n = $#results; $n > -1; --$n) {
                if ($self->isDeprecated($results[$n])) {
                    push(@deprecated, $n);
                }
            }
            if (scalar(@deprecated)) {
                $message .= "Removed " . scalar(@deprecated) . " deprecated Extent records from the results.\n";

                foreach my $n (@deprecated) {
                    $removed = splice(@results, $n, 1);
                }
            }
            if (scalar(@results > 1)) {
                if ($strict) {
                    $self->logWarn("$message - Multiple non-deprecated records (". join(', ', @results)."). Returning undef.");
                    return undef
                }
                else {
                    $self->logWarn("$message - Returning the first of the " . scalar(@results) . " non-deprecated Extent records.");
                    return $results[0]
                }
            }
            elsif (scalar(@results)) {
                $self->logWarn("$message - Returning the only non-deprecated Extent record.");
                return $results[0]
            }
            else {
                $self->logWarn("$message - All the records are flagged as deprecated. Returning the first from the list.");
                return $removed
            }
        }
        elsif (scalar(@results)) {
            return $results[0]
        }
        else {
            return undef
        }
    }
    return undef
}

=over

=item B<< $eid_arrayref = $glk->getExtentsForLibrary($libid) >>

This function will return a reference to an array containing the IDs of all
Extents which are most closely associated with the given Library.  If no Extents
are associated with this Library, the empty list is returned.

=back

=cut

sub getExtentsForLibrary {
    my ($self, $libid) = @_;

    my @exlist = ();

    if ($self->runQuery('GET_EXTENTS_FOR_LIBRARY', $libid)) {
        while(my $exid = $self->fetchSingle('GET_EXTENTS_FOR_LIBRARY')) {
            push(@exlist, $exid);
        }
    }
    $self->endQuery('GET_EXTENTS_FOR_LIBRARY');

    return \@exlist
}

=over

=item B<< $eid = $glk->addExtent($parent_eid, $type, $ref_id, $desc, $attr => $val...) >>

This function creates a new Extent with the given data.  The type can be
supplied as either an Extent Type Name or an Extent Type ID.  The Attributes
are added using the C<addExtentAttribute()> function.  See the documentation for
that function to find out how it works.  If the Extent cannot be added, a fatal exception is thrown.

=back

=cut

sub addExtent {
    my ($self, $parent, $type, $ref_id, $desc, %attrs) = @_;
    $self->_cleanValue(\$type,   'addExtent()', '$type');
    $self->_cleanValue(\$ref_id, 'addExtent()', '$ref_id');
    $self->_cleanValue(\$desc,   'addExtent()', '$desc');
    my $typeid = $self->getExtentTypeID($type);

    unless (defined($typeid)) {
        return undef
    }
    my $eid = getEUID();

    unless (defined($eid)) {
        return undef
    }
    unless (defined($ref_id)) {
        $ref_id = "$eid";
    }
    if ($self->runQuery('ADD_EXTENT', $eid, $ref_id, $parent, $typeid, $desc)) {
        $self->endQuery('ADD_EXTENT');

        foreach my $attr (keys %attrs) {
            $self->_cleanValue(\$attr, 'addExtent()', '$attr');
            my $val = $attrs{$attr};
            my $is_flag = $self->isFlagAttribute($attr);

            if (defined($is_flag) && !$is_flag && (!defined($val) || $val !~ /\S/)) { ### A value is therefore required...
                $self->logWarn("addExtent() - Attempt to add an empty value for a non-flag attribute (\"$attr\"). - Skipping it.");
                next
            }
            if (defined($val)) {
                $self->_cleanValue(\$val,  'addExtent()', '$cal');
            }
            else {
                $val = ' ';
            }
            $self->logLocal("addExtent() - Adding Extent Attribute: $attr='$val' to Extent_id $eid.", 4);
            $self->addExtentAttribute($eid, $attr, $val);
        }

        if ($type eq 'COLLECTION') {
            my $force = $ref_id eq 'XX' ? 1 : 0;
            $self->_addVcCollection($eid, $ref_id, $self->getDbName(), $force);
        }
        return $eid
    }
    else {
        $self->bail("addExtent() - Failed to create the new Extent (");
    }
}

=over

=item B<< $success = $glk->moveExtent($eid, $new_parent_eid) >>

This function moves the given Extent under a new parent.  If the move fails,
C<undef> is returned.  No validity checking is currently done.  Users should
be cautious with this function, as it is possible to make drastic changes to
the Extent tree, including reassigning library relationships.  This function
does not perform cycle detection.  It is up to the user to ensure that they do
not create circular references.

=back

=cut

sub moveExtent {
    my ($self, $eid, $new_parent) = @_;
   ##  Ensure that we're not setting ourselves as our parent
    if ($eid == $new_parent) {
        return undef
    }
    my $success = undef;

    if ($self->runQuery('SET_EXTENT_PARENT', $new_parent, $eid)) {
        $success = 1;
    }
    $self->endQuery('SET_EXTENT_PARENT');

    return $success
}

=over

=item B<< my $yesno = $glk->isDeprecated($eid) >>

It returns 1 if the given Extent is deprecated. 0 if it is still a valid entry.
It raises a fatal exception when called with an inexistent Extent_id

=back

=cut

sub isDeprecated {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        no warnings;
        $self->bail("isDeprecated() - Called with undefined or invalid Extent_id (\"$eid\".");
    }
    elsif (! $self->extentExists($eid)) {
        $self->bail("isDeprecated() - Called with non-existing Extent_id (\"$eid\".");
    }
    ## Taking advantage of the new vir_common..deprecated table.
    if (exists($self->{DEPRECATED}{$eid})) {
        return TRUE
    }
    return FALSE
}

=over

=item B<< my $xx_eid = $glk->getXXeid() >>
=item B<< my $xx_eid = $glk->getXXeid($suppress_warnings) >>

It returns the Extent_id of the master collection (ref_id = 'XX')
it takes an optional argument that, when set to a non-zero value, it will result in suppressing the warnings.

=back

=cut

sub getXXeid {
    my ($self, $suppress_warnings) = @_;

    unless (defined($suppress_warnings)) {
        $suppress_warnings = FALSE;
    }
    unless (defined($self->{XX})) {
        my $db = $self->getDbName();
        $self->{XX} = $self->getCollectionExtentId('XX', $db);

        unless (defined($self->{XX})) {
            unless ($suppress_warnings) {
                $self->logWarn("getXXeid() - Impossible to find the master collection in the current database ($db). Setting XX to -1");
            }
            $self->{XX} = -1;
        }
    }
    return $self->{XX}
}

=over

=item B<< $glk->setDeprecated() >>

It properly sets the deprecated status of an Extent of type COLLECTION, LOT, or SAMPLE propagating the status to all their children of the above-listed Extent Types.

=back

=cut

sub setDeprecated {
    my ($self, $eid) = @_;
    if (!defined($eid) || $eid !~ /^\d+$/) {
        no warnings;
        $self->bail("setDeprecated() - Called with undefined or invalid Extent_id (\"$eid\".");
    }
    elsif (! $self->extentExists($eid)) {
        $self->bail("setDeprecated() - Called with non-existing Extent_id (\"$eid\".");
    }
    if (exists($self->{DEPRECATED}{$eid})) {
        return ## if it already exists, it means it's already deprecated and we don't have anything to do.
    }
    ## If we are here, it means that we need to insert the correspondent record in vir_common..deprecated and, possibly, add the "deprecated" ExtentAttribute
    my $r_info = $self->getExtentInfo($eid);

    if ($r_info->{type} eq 'COLLECTION' || $r_info->{type} eq 'LOT') {
        my $r_children = $self->getExtentChildren($eid);

        foreach my $child (@{$r_children}) {
            my $r_child_info = $self->getExtentInfo($child);

            if ($r_child_info->{type} eq 'COLLECTION' || $r_child_info->{type} eq 'LOT' || $r_child_info->{type} eq 'SAMPLE') {
                $self->setDeprecated($child);
            }
        }
    }
    elsif ($r_info->{type} ne 'SAMPLE') {
        $self->logError("setDeprecated() - Called with inappropriate Extent_Type (\"$r_info->{type}\") - Ignoring it and its progeny.");
        return
    }
    unless ($self->hasExtentAttribute($eid, 'deprecated')) {
        $self->addExtentAttribute($eid, 'deprecated', 1);
    }
    if ($self->runQuery('DEPRECATE_EXTENT_VC', $eid)) {
        $self->endQuery('DEPRECATE_EXTENT_VC');
        undef($self->{DEPRECATED}{$eid});
    }

}

=over

=item B<< $glk->unsetDeprecated() >>

It properly removes the deprecated status of an Extent of type COLLECTION, LOT, or SAMPLE.
Due to the possibility that children extents are deprecated for other reasons, this function does not remove the deprecated status from any of the possible children
It is possible to provide a new parent_id to the re-established Extent. This is necessary in the case the Extent had been moved under the XX collection.

=back

=cut

sub unsetDeprecated {
    my ($self, $eid, $new_dad_id);

    if (!defined($eid) || $eid !~ /^\d=$/) {
        no warnings;
        $self->bail("unsetDeprecated() - Called with undefined, empty, or invalid Extent_id (\"$eid\".");
    }
    elsif (! $self->extentExists($eid)) {
        $self->bail("unsetDeprecated() - Called with non-existing Extent_id (\"$eid\".");
    }
    my $xx = $self->getXXeid();
    my $r_info = $self->getExtentInfo($eid);

    if (defined($new_dad_id)){
        if ($new_dad_id !~ /^\d+$/) {
            no warnings;
            $self->bail("unsetDeprecated() - Called with empty or invalid parent_id (\"$new_dad_id\".");
        }
        elsif (! $self->extentExists($new_dad_id)) {
            $self->bail("unsetDeprecated() - Called with non-existing parent_id (\"$new_dad_id\".");
        }
        elsif ($new_dad_id == $xx) {
            $self->bail("unsetDeprecated() - Called with XX collection Extent ID as new parent ID. The new parent_id must belong to a non-deprecated Extent.");
        }
        elsif ($self->isDeprecated($new_dad_id)) {
            $self->bail("unsetDeprecated() - Called with a deprecated Extent ID as new parent ID. The new parent_id must belong to a non-deprecated Extent.");
        }
    }
    elsif ($r_info->{parent} == $xx) { ## If the parent_id is pointing to the XX collection, but the functions has been called without providing a valid parent_id...
        $self->bail("unsetDeprecated() - Called without providing a parent_id. Extent $eid is currently pointing to the XX collection. In order to un-deprecate it, you need a valid (non-deprecated) parent_id");
    }
    if ($eid == $xx) {
            $self->bail("unsetDeprecated() - It's against the law to undeprecate the XX collection. - Attempt blocked.");
    }
    if ($r_info->{type} ne 'COLLECTION' && $r_info->{type} ne 'LOT' && $r_info->{type} ne 'SAMPLE') {
        $self->error("setDeprecated() - Called with inappropriate Extent_Type (\"$r_info->{type}\") - Ignoring it and its progeny.");
    }
    else {
        ## Checking
        if ($self->hasExtentAttribute($eid, 'deprecated')) {
            $self->removeExtentAttribute($eid, 'deprecated');
        }
        if (exists($self->{DEPRECATED}{$eid})) {
            if ($self->runQuery('UNDEPRECATE_EXTENT_VC', $eid)) {
                $self->endQuery('UNDEPRECATE_EXTENT_VC');
            }
            else {
                $self->bail("setDeprecated() - Problem removing vir_common..deprecated record for Extent $eid.");
            }
            delete($self->{DEPRECATED}{$eid});
        }
        if (defined($new_dad_id)) {
            $self->moveExtent($eid, $new_dad_id);
        }
    }
}


#
#  ---------------- Extents vs Assemblies --------------------
#
=over

=item B<< $r_asmbls = $glk->getAsmblBySegmentEid($eid, $strict) >>

Given the $eid of an Extent of type "SEGMENT" it returns a list of assemblies.

=back

=cut

sub getAsmblBySegmentEid {
    my ($self, $eid, $strict) = @_;

    unless (defined($strict)) {
        $strict = 0;
    }
    my @asmbls = ();
    my $asmbl;
    my $get_asmbls = $self->getQueryObject('GET_ASMBL_BY_SEGMENT_EXTENT');
    $get_asmbls->execute($eid) || $self->bail("getAsmblBySegmentEid() - Problems executing query \"GET_ASMBL_BY_SEGMENT_EXTENT\"");
    $get_asmbls->bind_columns(\$asmbl);

    while ($get_asmbls->fetch()) {
        push(@asmbls, $asmbl);
    }
    if (!@asmbls) {
        my $has_ss_attr = $self->hasExtentAttribute($eid, 'submit_sequence');
        my $error_msg = "getAsmblBySegmentEid() - Extent_id $eid is not linked to any assembly or the Segment Extent does not have the 'segment_name' attribute.";

        if ($has_ss_attr && $self->getExtentAttribute($eid, 'submit_sequence')) { ## The sequence is supposed to be there...
            if ($strict) {
                $self->bail($error_msg);
            }
            else {
                $self->logError($error_msg, 1);
            }
        }
        elsif (!$has_ss_attr) {
            $self->logWarn("$error_msg - The Extent is missing the 'submit_sequence' attribute too.");
        }
    }
    return \@asmbls
}

=over

=item B<< $bac_id = $glk->getBacIdByAsmbl($eid) >>

Given the Assembly ID, it returns the correspondent BAC ID stored in the assembly table.

=back

=cut

sub getBacIdByAsmbl {
    my ($self, $asmbl) = @_;
    my $get_bac = $self->getQueryObject('GET_BAC_ID_BY_ASMBL');
    $get_bac->execute($asmbl) || $self->bail("getBacIdByAsmbl() - Problems executing query \"GET_BAC_ID_BY_ASMBL\"");
    my $bac = $self->fetchSingle('GET_BAC_ID_BY_ASMBL');
    $self->endQuery('GET_BAC_ID_BY_ASMBL');

    if (!defined($bac)) {
        $self->logError("getBacIdByAsmbl() - Unable to find the BAC ID correspondent to assembly $asmbl.");
    }

    return $bac
}
=over

=item B<< $locus_tag_prefix = $glk->getBioProjectLocusTagPfix($bp_id) >>

Given a BioProject ID, it returns the corresponding locus_tag prefix or undef, if the project is not associated with any locus_tag prefix.

=back

=cut

sub getBioProjectLocusTagPfix {
    my ($self, $bp_id) = @_;
    my $lt_pfix;

    if ($self->isValidBioProject(\$bp_id)) {
        $lt_pfix = $self->{BIOPROJECT}{$bp_id}{locus_tag_prefix};
    }
    else {
        $self->logError("getBioProjectLocusTagPfix() - Invalid BioProject ID (\"$bp_id\").")
    }
    return $lt_pfix
}

=over

=item B<< @eids = @{$glk->getSampleEidByBatchId($batch_id) >>

Given a batch ID it returns a reference to a list of sample-level Extent IDs for all the non-deprecated samples in that batch.
In the case there is no sample associated with the given batch id, the function will raise a warning message and return a reference to an empty array.
The warning message will distinguish between no samples at all and no non-deprecated samples
=back

=cut

sub getSampleEidByBatchId {
    my ($self, $batch_id) = @_;

    unless (defined($batch_id) && $batch_id =~ /\S/) {
        $self->bail("getSampleEidByBatchId() - Called with undefined/empty Batch ID");
    }
    my @samples = ();
    my $eid;
    my $get_eids = $self->getQueryObject('GET_SAMPLE_EXTENTS_BY_BATCH_ID');
    $get_eids->execute($batch_id);
    $get_eids->bind_columns(\$eid);

    while ($get_eids->fetch()) {
        push(@samples, $eid);
    }
    unless (scalar(@samples)) {
        $self->logWarn("getSampleEidByBatchId() - No samples corresponding to batch ID \"$batch_id\".");
        return \@samples
    }
    ## Removing from the list all the deprecated samples...
    my @deprecated = ();

    for (my $n = $#samples; $n >= 0; --$n) {
        my $eid = $samples[$n];

        if ($self->isDeprecated($eid)){
            push(@deprecated, $n);
        }
    }
    foreach my $n (@deprecated) {
        splice(@samples, $n, 1);
    }
    unless (scalar(@samples)) {
        $self->logWarn("getSampleEidByBatchId() - All the samples corresponding to batch ID \"$batch_id\" are flagged as deprecated.");
    }
    return \@samples
}
=over

=item B<< @extractions = @{$glk->getSampleExtractions($eid)} >>

Given an Extent_id, it returns a reference to an array containing hashes of key-value pairs of fields from the Extraction table.
The Extractions are ordered by ExtractionNumber, being the array index = ExtractionNumber - 1.
If a discontinuity is found in the ExtractionNumber, a fatal exception is thrown.

=back

=cut

sub getSampleExtractions {
    my ($self, $eid) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getSampleExtractions() - Called with undefined/empty Extent ID ($eid)");
    }
    my @extractions = ();
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("getSampleExtractions() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    if ($self->runQuery('GET_EXTRACTIONS_BY_EXTENT_ID', $eid)) {
        my $expected_extr_no = 1;

        while (my $extr_data = $self->fetchRow('GET_EXTRACTIONS_BY_EXTENT_ID')) {
            if ($extr_data->{ExtractionNumber} == $expected_extr_no) {
                ++$expected_extr_no;
            }
            else {
                $self->bail("getSampleExtractions() - Problems retrieving Extraction information Found ExtractionNumber $extr_data->{ExtractionNumber} instead of $expected_extr_no.");
            }
            push(@extractions, $extr_data);
        }
        $self->endQuery('GET_EXTRACTIONS_BY_EXTENT_ID');
    }
    else {
        $self->bail("getSampleExtractions() - Problems executing 'GET_EXTRACTIONS_BY_EXTENT_ID' query with Extent ID $eid.\n\n");
    }
    return \@extractions
}

=over

=item B<< my %extraction = %{$glk->getSampleExtractionByNumber($eid, $extraction_number)} >>

Given an Extent_id and an ExtractionNumber, it returns a reference to a hash of key-value pairs of fields from the Extraction table.
If no Extraction is found corresponding to the requirements, it returns undef.

=back

=cut

sub getSampleExtractionByNumber {
    my ($self, $eid, $extr_no) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getSampleExtractionByNumber() - Called with undefined/empty/invalid Extent ID (\"$eid\")");
    }
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getSampleExtractionByNumber() - Called with undefined/empty/invalid ExtractionNumber (\"$extr_no\")");
    }
    my $r_extraction;
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("getSampleExtractionByNumber() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    if ($self->runQuery('GET_EXTRACTION_BY_EXTENT_ID_EXTR_NO', $eid, $extr_no)) {
        $r_extraction = $self->fetchRow('GET_EXTRACTION_BY_EXTENT_ID_EXTR_NO');
        $self->endQuery('GET_EXTRACTION_BY_EXTENT_ID_EXTR_NO');
    }
    else {
        $self->bail("getSampleExtractionByNumber() - Problems executing 'GET_EXTRACTIONS_BY_EXTENT_ID_EXTR_NO' query with Extent ID $eid.\n\n");
    }
    return $r_extraction
}

=over

=item B<< my $max_extraction = $glk->getMaxExtraction($eid) >>

Given an Extent_id, it returns the maximum ExtractionNumber associated with that sample.
If no Extraction record has been found, it returns 0.

=back

=cut

sub getMaxExtraction {
    my ($self, $eid) = @_;
    my $max_extr;

    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getMaxExtraction() - Called with undefined/empty Extent ID ($eid)");
    }
    if ($self->runQuery('GET_MAX_EXTRACTION_BY_EXTENT_ID', $eid)) {
        $max_extr = $self->fetchSingle('GET_MAX_EXTRACTION_BY_EXTENT_ID');
        $self->endQuery('GET_MAX_EXTRACTION_BY_EXTENT_ID');
    }
    else {
        $self->bail("getMaxExtraction() - Problems running the query GET_MAX_EXTRACTION_BY_EXTENT_ID.");
    }
    unless (defined($max_extr) && $max_extr =~ /^\d+$/) {
        $max_extr = 0;
    }
    return $max_extr
}

=over

=item B<< my $max_pcr = $glk->getMaxPCR($eid) >>

Given an Extent_id, it returns the maximum PcrNumber associated with that sample.
If no PCR record has been found, it returns 0.

=back

=cut

sub getMaxPCR {
    my ($self, $eid) = @_;
    my $max_pcr;

    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getMaxPCR() - Called with undefined/empty Extent ID ($eid)");
    }
    if ($self->runQuery('GET_MAX_PCR_BY_EXTENT_ID', $eid)) {
        $max_pcr = $self->fetchSingle('GET_MAX_PCR_BY_EXTENT_ID');
        $self->endQuery('GET_MAX_PCR_BY_EXTENT_ID');
    }
    else {
        $self->bail("getMaxPCR() - Problems running the query GET_MAX_PCR_BY_EXTENT_ID.");
    }
    unless (defined($max_pcr) && $max_pcr =~ /^\d+$/) {
        $max_pcr = 0;
    }
    return $max_pcr
}

=over

=item B<< @pcrs = @{$glk->getSamplePCRs($eid)} >>

Given an Extent_id, it returns a reference to an array containing hashes of key-value pairs of fields from the PCR table.
The PCRs are ordered by PcrNumber, being the array index = PcrNumber - 1.
If a discontinuity is found in the PcrNumber, a fatal exception is thrown.

=back

=cut

sub getSamplePCRs {
    my ($self, $eid) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getSamplePCRs() - Called with undefined/empty Extent ID ($eid)");
    }
    my @pcrs = ();
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("getSamplePCRs() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    if ($self->runQuery('GET_PCRS_BY_EXTENT_ID', $eid)) {
        my $expected_pcr_no = 1;

        while (my $pcr_data = $self->fetchRow('GET_PCRS_BY_EXTENT_ID')) {
            if ($pcr_data->{PCRNumber} == $expected_pcr_no) {
                ++$expected_pcr_no;
            }
            else {
                $self->bail("getSamplePCRs() - Problems retrieving PCR information. Found PcrNumber $pcr_data->{PCRNumber} instead of $expected_pcr_no.");
            }
            push(@pcrs, $pcr_data);
        }
        $self->endQuery('GET_PCRS_BY_EXTENT_ID');
    }
    else {
        $self->bail("getSamplePCRs() - Problems executing 'GET_PCRS_BY_EXTENT_ID' query with Extent ID $eid.\n\n");
    }
    return \@pcrs
}
=over

=item B<< @pcrs = @{$glk->getSamplePcrByNumber($eid, $pcr_number)} >>

Given an Extent_id, and a PCRNumber, it returns a reference to a hash of key-value pairs of fields from the PCR table.
If no record is found undef is returned.

=back

=cut

sub getSamplePcrByNumber {
    my ($self, $eid, $pcr_no) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("getSamplePcrByNumber() - Called with undefined/empty/invalid Extent ID ($eid)");
    }
    unless (defined($pcr_no) && $pcr_no =~ /^\d+$/) {
        no warnings;
        $self->bail("getSamplePcrByNumber() - Called with undefined/empty/invalid PCRNumber ($pcr_no)");
    }
    my $r_pcr;
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("getSamplePcrByNumber() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    if ($self->runQuery('GET_PCR_BY_EXTENT_ID_PCR_NO', $eid, $pcr_no)) {
        $r_pcr = $self->fetchRow('GET_PCR_BY_EXTENT_ID_PCR_NO');
        $self->endQuery('GET_PCR_BY_EXTENT_ID_PCR_NO');
    }
    else {
        $self->bail("getSamplePcrByNumber() - Problems executing 'GET_PCR_BY_EXTENT_ID_PCR_NO' query with Extent ID $eid and PCRNumber $pcr_no.\n\n");
    }
    return $r_pcr
}

=over

=item B<< $success = $glk->loadSampleExtraction($eid, \%extraction_data) >>

Given an Extent_id and a reference to an hash of key-values pairs, it verifies that all the requirements are met and that the provided ExtractionNumber is exactly one unit greater than the last recorded Extraction record for that sample.

=back

=cut

sub loadSampleExtraction {
    my ($self, $eid, $r_extr) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("loadSampleExtraction() - Called with undefined/empty Extent ID ($eid)");
    }
    unless (defined($r_extr)) {
        $self->bail("loadSampleExtraction() - Missing reference to hash with extraction data.");
    }
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("loadSampleExtraction() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    my @mandatory = (qw(ExtractionNumber Date Method Technician));

    my $r_missing = $self->_checkRequiredFields($r_extr, \@mandatory);

    if (scalar(@{$r_missing})) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSampleExtraction() - Sample $bid - Missing the following required fields: \"" . join('", "', @{$r_missing}) . "\".");
    }
    my $max_extr = $self->getMaxExtraction($eid);

    if ($r_extr->{ExtractionNumber} <= $max_extr) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSampleExtraction() - Sample $bid - Attempting to enter an extraction (Extraction ID $r_extr->{ExtractionNumber}) that already exists in the database (max Extraction ID $max_extr).");
    }
    elsif ($r_extr->{ExtractionNumber} > $max_extr + 1) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSampleExtraction() - Sample $bid - Attempting to enter an extraction with an ID (Extraction ID $r_extr->{ExtractionNumber}) more than a unit greater than last recorded Extraction (max Extraction ID $max_extr).");
    }
    ## End of consistency checks, now the load should proceed without hiccups

    if ($self->runQuery('INSERT_EXTRACTION', $eid, $r_extr->{ExtractionNumber}, $r_extr->{Date}, $r_extr->{Method}, $r_extr->{Technician}, $r_extr->{Institution}, $r_extr->{Comments})) {
        $self->endQuery('INSERT_EXTRACTION');
    }
    else {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSampleExtraction() - Problems executing 'INSERT_EXTRACTION' query with Extent ID $eid.\n\n");
    }
    return SUCCESS
}

=over

=item B<< $success = $glk->updateSampleExtraction($eid, $extraction_number, \%extraction_data) >>

Given an Extent_id, an ExtractionNumber, and a reference to an hash of key-values pairs, it verifies that all the requirements are met and that the provided ExtractionNumber exists and it updates the record with all the supplied values.
This method does not allow wiping out a field that was previously populated: the current value is retained.

=back

=cut

sub updateSampleExtraction {
    my ($self, $eid, $extr_no, $r_extr) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("updateSampleExtraction() - Called with undefined/empty/invalid Extent ID ($eid)");
    }
    unless (defined($extr_no) && $extr_no =~ /^\d+$/) {
        no warnings;
        $self->bail("updateSampleExtraction() - Called with undefined/empty/invalid ExtractionNumber ($extr_no)");
    }
    unless (defined($r_extr)) {
        $self->bail("updateSampleExtraction() - Missing reference to hash with extraction data.");
    }
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("updateSampleExtraction() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    my $r_current_extr = $self->getSampleExtractionByNumber($eid, $extr_no);

    unless (defined($r_current_extr)) {
        $self->bail("updateSampleExtraction() - Called with an inexistent ExtractionNumber ($extr_no) for Extent $eid.");
    }
    ## End of consistency checks, now filling the gaps with existing values...

    my @updatable_fields = (qw(Date Method Technician Institution Comments));

    foreach my $field (@updatable_fields) {
        unless (defined($r_extr->{$field}) && $r_extr->{$field} =~ /\S/) {
            if (defined($r_current_extr->{$field}) && $r_current_extr->{$field} =~ /\S/) {
                $r_extr->{$field} = $r_current_extr->{$field};
            }
        }
    }
    ## Now loading
    if ($self->runQuery('UPDATE_EXTRACTION', $r_extr->{Date}, $r_extr->{Method}, $r_extr->{Technician}, $r_extr->{Institution}, $r_extr->{Comments}, $eid, $extr_no)) {
        $self->endQuery('UPDATE_EXTRACTION');
    }
    else {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("updateSampleExtraction() - Problems executing 'UPDATE_EXTRACTION' query with Extent ID $eid.\n\n");
    }
    return SUCCESS
}
=over

=item B<< $success = $glk->loadSamplePCR($eid, \%pcr_data) >>

Given an Extent_id and a reference to an hash of key-values pairs, it verifies that all the requirements are met and that the provided ExtractionNumber exists and that the provided PCRNumber is exactly one unit greater than the last recorded PCR record for that sample.

=back

=cut

sub loadSamplePCR {
    my ($self, $eid, $r_pcr) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("loadSamplePCR() - Called with undefined/empty Extent ID ($eid)");
    }
    unless (defined($r_pcr)) {
        $self->bail("loadSamplePCR() - Missing reference to hash with extraction data.");
    }
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("loadSamplePCR() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    my @mandatory = (qw(PCRNumber ExtractionNumber PcrType Date Volume Concentration Technician));

    my $r_missing = $self->_checkRequiredFields($r_pcr, \@mandatory);

    if (scalar(@{$r_missing})) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSamplePCR() - Sample $bid - Missing the following required fields: \"" . join('", "', @{$r_missing}) . "\".");
    }
    unless ($self->existsExtraction($eid, $r_pcr->{ExtractionNumber})) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSamplePCR() - Sample $bid - Attempting to insert a PCR reaction with a non-existing ExtractionNumber ($r_pcr->{ExtractionNumber}).");
    }
    my $max_pcr = $self->getMaxPCR($eid);

    if ($r_pcr->{PCRNumber} <= $max_pcr) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSamplePCR() - Sample $bid - Attempting to enter a PCR (PCR ID $r_pcr->{PCRNumber}) that already exists in the database (max PCR ID $max_pcr).");
    }
    elsif ($r_pcr->{PCRNumber} > $max_pcr + 1) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSamplePCR() - Sample $bid - Attempting to enter a PCR with an ID (PCR ID $r_pcr->{PCRNumber}) more than a unit greater than last recorded PCR (max PCR ID $max_pcr).");
    }
    ## End of consistency checks, now the load should proceed without hiccups

    if ($self->runQuery('INSERT_PCR', $r_pcr->{PCRNumber}, $eid, $r_pcr->{ExtractionNumber}, $r_pcr->{PcrType}, $r_pcr->{Date}, $r_pcr->{Volume},  $r_pcr->{Concentration},  $r_pcr->{PrimerSet},  $r_pcr->{PlateLocation},  $r_pcr->{Score}, $r_pcr->{Technician}, $r_pcr->{Comments})) {
        $self->endQuery('INSERT_PCR');
    }
    else {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("loadSamplePCR() - Problems executing 'INSERT_PCR' query with Extent ID $eid.\n\n");
    }
    return SUCCESS
}

=over

=item B<< $success = $glk->updateSamplePCR($eid, PCRNumber, \%pcr_data) >>

Given an Extent_id, a PCRNumber, and a reference to an hash of key-values pairs, it verifies that all the requirements are met, that the provided ExtractionNumber exists, that that PCR record exists, and it updates it.
This method does not allow wiping out a field that was previously populated: the current value is retained.


=back

=cut

sub updateSamplePCR {
    my ($self, $eid, $pcr_no, $r_pcr) = @_;
    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("updateSamplePCR() - Called with undefined/empty/invalid Extent ID ($eid)");
    }
    unless (defined($pcr_no) && $pcr_no =~ /^\d+$/) {
        no warnings;
        $self->bail("updateSamplePCR() - Called with undefined/empty/invalid PCRNumber ($pcr_no)");
    }
    unless (defined($r_pcr)) {
        $self->bail("updateSamplePCR() - Missing reference to hash with extraction data.");
    }
    my $ext_type = $self->getExtentInfo($eid)->{type};

    if ($ext_type ne 'SAMPLE') {
        $self->bail("updateSamplePCR() - Called with wrong Extent type (Extent ID $eid - Actual type: \"$ext_type\")");
    }
    my $current_pcr = $self->getSamplePcrByNumber($eid, $pcr_no);

    unless (defined($current_pcr)) {
        $self->bail("updateSamplePCR() - Called with inexistent PCRNumber ($pcr_no) on Extent $eid.");
    }
    my @updatable_fields = (qw(ExtractionNumber PcrType Date Volume Concentration PrimerSet PlateLocation Score Technician Comments));

    foreach my $field (@updatable_fields) {
        unless (defined($r_pcr->{$field}) && $r_pcr->{$field} =~ /\S/) {
            if (defined($current_pcr->{$field}) && $current_pcr->{$field} =~ /\S/) {
                $r_pcr->{$field} = $current_pcr->{$field};
            }
        }
    }
    unless ($self->existsExtraction($eid, $r_pcr->{ExtractionNumber})) {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("updateSamplePCR() - Sample $bid - Attempting to insert a PCR reaction with a non-existing ExtractionNumber ($r_pcr->{ExtractionNumber}).");
    }

    ## Now loading
    if ($self->runQuery('UPDATE_PCR', $r_pcr->{ExtractionNumber}, $r_pcr->{PcrType}, $r_pcr->{Date}, $r_pcr->{Volume},  $r_pcr->{Concentration},  $r_pcr->{PrimerSet},  $r_pcr->{PlateLocation},  $r_pcr->{Score}, $r_pcr->{Technician}, $r_pcr->{Comments}, $eid, $pcr_no)) {
        $self->endQuery('UPDATE_PCR');
    }
    else {
        my $bid = $self->getExtentInfo($eid)->{'ref'};
        $self->bail("updateSamplePCR() - Problems executing 'UPDATE_PCR' query with Extent ID $eid BAC ID $bid.\n\n");
    }
    return SUCCESS
}

=over

=item B<< my $yes_no = $glk->existsExtraction($eid, $extr_no) >>

Given a SAMPLE Extent_id and an ExtractionNumber, it returns 1 if that record exists in the Extraction table, 0 otherwise
=back

=cut

sub existsExtraction {
    my ($self, $eid, $extr_no) = @_;
    my $exists;

    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("existsExtraction() - Called with undefined/empty or otherwise invalid Extent ID (\"$eid\")");
    }
    unless (defined($extr_no) && $extr_no =~ /^\d+$/) {
        no warnings;
        $self->bail("existsExtraction() - Called with undefined/empty or otherwise invalid ExtractionNumber (\"$extr_no\").");
    }
    if ($self->runQuery('EXISTS_EXTRACTION_BY_EXTENT_ID_EX_NUM', $eid, $extr_no)) {
        $exists = $self->fetchSingle('EXISTS_EXTRACTION_BY_EXTENT_ID_EX_NUM');
        $self->endQuery('EXISTS_EXTRACTION_BY_EXTENT_ID_EX_NUM');
    }
    else {
        $self->bail("existsExtraction() - Problems running the query 'EXISTS_EXTRACTION_BY_EXTENT_ID_EX_NUM'.");
    }
    return $exists
}


=over

=item B<< my $yes_no = $glk->existsPCR($eid, $pcr_no) >>

Given a SAMPLE Extent_id and a PCRNumber, it returns 1 if that record exists in the PCR table, 0 otherwise
=back

=cut

sub existsPCR {
    my ($self, $eid, $pcr_no) = @_;
    my $exists;

    unless (defined($eid) && $eid =~ /^\d+$/) {
        no warnings;
        $self->bail("existsPCR() - Called with undefined/empty or otherwise invalid Extent ID (\"$eid\")");
    }
    unless (defined($pcr_no) && $pcr_no =~ /^\d+$/) {
        no warnings;
        $self->bail("existsPCR() - Called with undefined/empty or otherwise invalid PCRNumber (\"$pcr_no\").");
    }
    if ($self->runQuery('EXISTS_PCR_BY_EXTENT_ID_PCR_NO', $eid, $pcr_no)) {
        $exists = $self->fetchSingle('EXISTS_PCR_BY_EXTENT_ID_PCR_NO');
        $self->endQuery('EXISTS_PCR_BY_EXTENT_ID_PCR_NO');
    }
    else {
        $self->bail("existsPCR() - Problems running the query 'EXISTS_PCR_BY_EXTENT_ID_PCR_NO'.");
    }
    return $exists
}

=over

=item B<< @eids = @{$glk->getBacIdByBatchId($batch_id) >>

Given a batch ID it returns a reference to a list of BAC IDs for all the non-deprecated samples in that batch.
In the case there is no sample associated with the given batch id, the function will raise a warning message and return a reference to an empty array.
The warning message will distinguish between no samples at all and no non-deprecated samples
=back

=cut

sub getBacIdByBatchId {
    my ($self, $batch_id) = @_;

    unless (defined($batch_id) && $batch_id =~ /\S/) {
        $self->bail("getSampleEidByBatchId() - Called with undefined/empty Batch ID");
    }
    my $r_samples = $self->getSampleEidByBatchId($batch_id);
    ## All the warnings for not having samples have been already raised by getSampleEidByBatchId()
    unless (scalar(@{$r_samples})) {
        return [];
    }
    my @bids = ();

    foreach my $eid (@{$r_samples}) {
        my $bid = $self->getExtentInfo($eid)->{ref};

        if (defined($bid) && $bid =~ /^\d+$/) {
            if ($self->isDeprecated($eid)) {
                $self->logWarn("getBacIdByBatchId() - Sample Extent $eid is flagged as deprecatent, but it still has Batch ID $batch_id.");
            }
            else {
                push(@bids, $bid);
            }
        }
        else {
            $self->bail(" getBacIdByBatchId() - Unable to find a valid BAC ID for Extent $eid.");
        }
    }
    return \@bids
}
=over

=item B<< $eid = $glk->getSegmentEidByAsmbl($asmbl_id, $strict) >>

Given an $asmbl_id it returns the Extent_id of the corresponding "SEGMENT".
If the flag $strict is set, the function will die raising the appropriate error message in the cases that the assembly is not linked to an Extent and if it is linked to more than one Extent. Otherwise, just error messages will be generated.

=back

=cut

sub getSegmentEidByAsmbl {
    my ($self, $asmbl_id, $strict) = @_;

    unless (defined($strict)) {
        $strict = 0;
    }
    my @ext_ids = ();
    my $eid;
    my $get_eids = $self->getQueryObject('GET_SEGMENT_EXTENT_BY_ASMBL');
    $get_eids->execute($asmbl_id) || $self->bail("getSegmentEidByAsmbl() - Problems executing query \"GET_SEGMENT_EXTENT_BY_ASMBL\"");
    $get_eids->bind_columns(\$eid);

    while ($get_eids->fetch()) {
        push(@ext_ids, $eid);
    }
    if (! @ext_ids) {
        my $msg = "getSegmentEidByAsmbl() - Assembly $asmbl_id is not linked to any Extent through asmbl_link table";

        if ($strict > 1) {
            $self->bail($msg);
        }
        else {
            $self->logError("getSegmentEidByAsmbl() - Unable to find any link between assembly $asmbl_id and GLK.\n\nMake sure that the GLK tables are correctly populated for this segment and that the segment Extent has the attribute 'segment_name'", 1);
        }
    }
    elsif (@ext_ids > 1) {
        my $msg = "getSegmentEidByAsmbl() - Assembly $asmbl_id is linked to too many Extents: " . join(', ', @ext_ids);

        if ($strict) {
            $self->bail($msg);
        }
        else {
            $self->logError($msg, 1);
        }
    }
    $eid = $ext_ids[0];

    return $eid
}
=comment NO LONGER USED 2017-04-20

=item B<< $eid = $glk->getSegmentEidByAsmblInfo($asmbl_id, $strict) >>

Given an $asmbl_id it returns the Extent_id of the corresponding "SEGMENT".

NOTE: This function is deprecated and getSegmentEidByAsmbl() should be used instead.

=back

#=cut

sub getSegmentEidByAsmblInfo {
    my ($self, $asmbl_id, $strict) = @_;

    unless (defined($strict)) {
        $strict = 0;
    }
    $self->logWarn('getSegmentEidByAsmblInfo() - Deprecated function. Use getSegmentEidByAsmbl() instead.');

    return $self->getSegmentEidByAsmbl($asmbl_id, $strict)
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $eid = $glk->getSegmentEidByAsmblInfo($asmbl_id, $seg_name) >>

Given a BAC ID and the segment name it returns the Extent_id of the corresponding "SEGMENT".

=back

#=cut

sub getSegmentEidByBacAndName {
    my ($self, $bid, $seg_name) = @_;

    if (!defined($bid) || $bid !~ /^\d+$/) {
        $self->bail("getSegmentEidByBacAndName() - Called with invalid/undefined BAC ID");
    }
    elsif (!defined($seg_name) || $seg_name !~ /^\S+$/) {
        $self->bail("getSegmentEidByBacAndName() - Called with undefined/empty segment name");
    }
    my $eid;
    my @results = ();
    my $get_eid = $self->getQueryObject('GET_SEGMENT_BY_BAC_AND_NAME');
    $get_eid->execute($bid, $seg_name) || $self->bail("getSegmentEidByBacAndName() - Problems executing the query 'GET_SEGMENT_BY_BAC_AND_NAME'.\n");
    $get_eid->bind_columns(\$eid);

    while ($get_eid->fetch()) {
        push(@results, $eid);
    }
    if (scalar(@results) > 1) {
        $self->bail("getSegmentEidByBacAndName() - BAC ID $bid has multiple segments with name \"$seg_name\". List of all Extent_id: " . join(', ', @results));
    }
    elsif (scalar(@results)) {
        return $results[0]
    }
    else {
        $self->logWarn("getSegmentEidByBacAndName() - BAC ID $bid missing segment with name \"$seg_name\", the segment Extent is missing the \"segment_name\" attribute, or no current assembly has been loaded for the given segment.");
        return undef
    }
}
=cut

=over 

=item B<< $seg_name = $glk->getSegmentNameByAsmblId($asmbl_id) >>

Given an Assembly ID, it returns the segment name, or undef, in the case the assembly is not a current assembly or is not linked to any Extent.

=back

=cut

sub getSegmentNameByAsmblId {
    my ($self, $asmbl_id) = @_;

    if (!defined($asmbl_id) || $asmbl_id !~ /^\d+$/) {
        $self->bail("getSegmentNameByAsmblId() - Called with invalid/undefined Assembly ID");
    }
    my $seg_name;
    my @results = ();
    my $get_seg_name = $self->getQueryObject('GET_SEG_NAME_BY_ASMBL_ID');
    $get_seg_name->execute($asmbl_id) || $self->bail("getSegmentNameByAsmblId() - Problems executing the query 'GET_SEG_NAME_BY_ASMBL_ID'.\n");
    $get_seg_name->bind_columns(\$seg_name);

    while ($get_seg_name->fetch()) {
        push(@results, $seg_name);
    }
    if (scalar(@results) > 1) {
        $self->bail("getSegmentNameByAsmblId() - Assembly $asmbl_id is linked to multiple Extents.");
    }
    elsif (scalar(@results)) {
        return $results[0]
    }
    else {
        $self->logWarn("getSegmentNameByAsmblId() - Assembly $asmbl_id is not a current assembly or is not otherwise linked to any Extent.");
        return undef
    }
}

#
#  ---------------- Extent Attributes --------------------
#


=head3 Extent Attributes

=over

=item B<< $info_hashref = $glk->getRequiredExtentAttributes($type) >>

Given an Extent Type, it returns a reference to a list of the required ExtentAttributeType

=back

=cut

sub getRequiredExtentAttributes {
    my ($self, $ext_type, $db) = @_;
    my $ext_type_id;
    my @attypes = ();

    if (!defined($ext_type) || $ext_type =~ /^\s*$/) {
        $self->bail("getRequiredExtentAttributes() - Missing required parameter Extent_Type.");
    }
    unless (defined($db)) {
        $db = $self->getDbName();
    }
    if ($ext_type =~ /^\d+$/) { ## We have the Extent_Type_id instead...
        $ext_type_id = $ext_type;
        $ext_type = $self->getExtentTypeName($ext_type_id);

        unless (defined($ext_type)) {
            $self->bail("getRequiredExtentAttributes() - Called with invalid Extent_Type_id ($ext_type_id).");
        }
    }
    else {
        $ext_type_id = $self->getExtentTypeID($ext_type);
    }
    unless (defined($ext_type_id)) {
        $self->bail("getRequiredExtentAttributes() - Called with invalid Extent_Type (\"$ext_type\").");
    }
    unless (defined($self->{ExtAttrRegister}{$db})) {
        $self->loadExtAttrRegister($db);
    }
    while (my ($eat_id, $required) = each(%{$self->{ExtAttrRegister}{$db}{$ext_type_id}})) {
        if (exists($self->{ExtAttrPlace_id}{$ext_type_id}{$eat_id}) && $required) {
            unless (defined($self->{ExtAttrPlace_id}{$ext_type_id}{$eat_id})) {
                $self->bail("getRequiredExtentAttributes() - Serious error: \$self->{ExtAttrPlace_id}{$ext_type_id}{$eat_id} exists, but is undefined.");
            }
            my $att_type = $self->getExtentAttrTypeName($eat_id);

            unless (defined($att_type)) {
                $self->bail("getRequiredExtentAttributes() - Very serious troubles: Table vir_common..ExtAttrPlace contains ExtentAttributeType_id ($eat_id) for non-existing attributes.");
            }
            push(@attypes, $att_type);
        }
    }
    return \@attypes
}

=over

=item B<< $info_hashref = $glk->getExtentInfo($eid) >>

This function returns a reference to a hash of information about the given
Extent.  If the extent is not found, a reference to an empty hash is returned.
On success, the following hash values will exist:

    id      :  The Extent ID
    ref     :  The Reference ID
    parent  :  The Extent ID of the parent extent
    type    :  The Extent Type name
    type_id :  The Extent Type ID
    desc    :  The Extent description

=back

=cut

sub getExtentInfo {
    my ($self, $eid) = @_;
    my %info = (id      => undef,
                'ref'   => undef,
                parent  => undef,
                type    => undef,
                type_id => undef,
                desc    => undef);
    $self->preloadExtentTypes();

    if (!defined($eid)) {
        $self->logWarn("getExtentInfo() - Called with an undefined Extent ID.");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->logWarn("getExtentInfo() - Called with an invalid Extent ID (\"$eid\").");
    }
    elsif ($self->extentExists($eid)) {
        if ($self->runQuery('GET_EXTENT_INFO', $eid)) {
            my $row = $self->fetchRow('GET_EXTENT_INFO');

            if (defined($row->{'Extent_id'})) {
                %info = ('id' => $row->{'Extent_id'},
                         'ref' => $row->{'ref_id'},
                         'parent' => $row->{'parent_id'},
                         'type' => $self->getExtentTypeName($row->{'Extent_Type_id'}),
                         'type_id' => $row->{'Extent_Type_id'},
                         'desc' => $row->{'description'},
                         );
            }
            else {
                $self->logWarn("getExtentInfo() - Database: \"" . $self->getDbName()  . "\" - The Extent seems to exist, but it is impossible to get info. Indexes problems?");
            }

        }
        else {
            $self->logWarn("getExtentInfo() - Database: \"" . $self->getDbName()  . "\" - Problem running the query \"GET_EXTENT_INFO\".");
        }
        $self->endQuery('GET_EXTENT_INFO');
    }
    else {
        $self->logWarn("getExtentInfo() - Database: \"" . $self->getDbName()  . "\" - The Extent $eid does not exist in this database.");
    }

    return \%info
}

=item B<< $extent_type = $glk->getExtentType($extent_id); >>

Given an Extent ID, it returns its type.

=over

=back

=cut

sub getExtentType {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        $self->bail("getExtentType() - Called with undefined or invalid Extent ID");
    }
    return $self->getExtentInfo($eid)->{type}
}

=item B<< $boolean = $glk->extentExists($extent_id) >>

=over

This function returns C<1> if the given Extent ID exists in the Extent Tree of the
current database.  If the Extent ID is not found, it returns C<undef>.

=back

=cut

sub extentExists {
    my ($self, $exid) = @_;

    my $success = undef;

    if (exists($self->{EXTENT_EXISTS}{$exid})) {
        $success = 1;
    }
    elsif ($self->runQuery('EXISTS_EXTENT', $exid)) {
        if($self->fetchSingle('EXISTS_EXTENT')) {
            $success = 1;
            undef($self->{EXTENT_EXISTS}{$exid});
        }
        $self->endQuery('EXISTS_EXTENT');
    }
    return $success
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< $name = $glk->getExtentName($eid) >>

This function returns the standard string representation of an Extent name.
This is created by concatenating the Extent's type, a colon, and the Extent's
reference ID.  For example: "BAC:31337".  If the Extent ID is not found,
C<undef> is returned.

=back

#=cut

sub getExtentName {
    my ($self, $eid) = @_;

    my $info = $self->getExtentInfo($eid);

    if (exists $info->{'type'} and exists $info->{'ref'}) {
        return sprintf("%s:%s", $info->{'type'}, $info->{'ref'})
    }
    return undef;
}

=cut
=back

=cut

#
#  ---------------- Finding Extents --------------------
#

=head3 Finding Extents

=over

=cut

=over

=item B<< $eid = $glk->getExtentByTypeRef($type, $ref_id, $strict) >>

This function will return the Extent ID of the Extent matching the given Extent
type and reference ID.  This provides the most common mechanism for finding
extents.  If no matching Extent is found C<undef> is returned.
The third argument is optional: when set to a non-zero value, the function will return undef if more than one record is found.

=back

=cut

sub getExtentByTypeRef {
    my ($self, $type, $ref, $strict) = @_;

    $self->_cleanValue(\$type, 'getExtentByTypeRef()', '$type');
    $self->_cleanValue(\$ref,  'getExtentByTypeRef()', '$ref');

    #TODO This should perhaps use translation services instead
    my $typeid = $self->getExtentTypeID($type);

    unless (defined($typeid)) {
        $self->logWarn("getExtentByTypeRef() - Invalid ExtentattributeType (\"$type\").");
        return undef
    }

    if ($self->runQuery('GET_EXTENT_BY_REF_TYPEID', $typeid, $ref)) {
        my @results = ();

        foreach my $row (@{$self->fetchAllArrayRef('GET_EXTENT_BY_REF_TYPEID')}) {
            push(@results, $row->[0]);
        }
        $self->endQuery('GET_EXTENT_BY_REF_TYPEID');

        if (scalar(@results) > 1) { ## In the worrisome case we fetched multiple records and we're returning a single one...
            my $message = "getExtentByTypeRef() - The query returned " . scalar(@results) . " Extent records of type \"$type\" having ref_id \"$ref\"\n";

            ## Removing from the list the deprecated samples
            my @deprecated = ();
            my $removed;

            for (my $n = $#results; $n > -1; --$n) {
                if ($self->isDeprecated($results[$n])) {
                    push(@deprecated, $n);
                }
            }
            if (scalar(@deprecated)) {
                $message .= "Removed " . scalar(@deprecated) . " deprecated Extent records from the results.\n";

                foreach my $n (@deprecated) {
                    $removed = splice(@results, $n, 1);
                }
            }
            if (scalar(@results > 1)) {
                if ($strict) {
                    $self->logWarn("$message - Multiple non-deprecated records (". join(', ', @results)."). Returning undef.");
                    return undef
                }
                else {
                    $self->logWarn("$message - Returning the first of the " . scalar(@results) . " non-deprecated Extent records.");
                    return $results[0]
                }
            }
            elsif (scalar(@results)) {
                $self->logWarn("$message - Returning the only non-deprecated Extent record.");
                return $results[0]
            }
            else {
                $self->logWarn("$message - All the records are flagged as deprecated. Returning the first from the list.");
                return $removed
            }
        }
        elsif (scalar(@results)) {
            return $results[0]
        }
        else {
            return undef
        }

    }
    return undef
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< $eid = $glk->getExtentChildByDesc($parent_eid, $desc) >>

This function will return the first child Extent of the given Extent whose
description matches the one supplied.  The match must be exact, and if more
than one child matches the given description, the matching record returned is
non-deterministic.  (Due to this, this function may become deprecated in the
future)

=back

#=cut

## TODO This is a very dangerous method: If multiple extents are found, only one is returned. I personally find this quite absurd.
## Even if in a specific case we would need a random result, it wouldn't harm returning an arrayref.

sub getExtentChildByDesc {
    my ($self, $parent, $desc) = @_;

    $self->_cleanValue(\$desc, 'getExtentChildByDesc()', '$desc');
    my $eid = undef;

    if ($self->runQuery('GET_EXTENT_CHILD_BY_DESC', $parent, $desc)) {
        $eid = $self->fetchSingle('GET_EXTENT_CHILD_BY_DESC');
    }
    $self->endQuery('GET_EXTENT_CHILD_BY_DESC');

    return $eid
}
=cut 

=over

=item B<< $extent_arrayref = $glk->getExtentsByType($extent_type) >>

This function returns a reference to an array holding the Extent IDs of all
Extents belonging to the specified type.  The type can be supplied either as
a type ID or type name.  If no Extents are found, a reference to an empty
array is returned.

=back

=cut

sub getExtentsByType {
    my ($self, $type) = @_;

    $self->_cleanValue(\$type, 'getExtentsByType()', '$type');
    my $typeid = $self->translateExtentType($type);

    unless (defined($typeid)) {
        $self->logWarn("getExtentsByType() - Invalid Extent_Type (\"$type\").");
        return []
    }
    my @exlist = ();
    if ($self->runQuery('GET_EXTENTS_BY_TYPEID', $typeid)) {
        while (my ($eid) = $self->fetchListRow('GET_EXTENTS_BY_TYPEID')) {
            push @exlist, $eid;
        }
    }
    $self->endQuery('GET_EXTENTS_BY_TYPEID');

    return \@exlist
}

=back

=cut

#
#  ---------------- Extent Tree Listings --------------------
#

=head3 Extent Tree Listings

=over

=cut

=over

=item B<< $eid = $glk->getExtentRoot() >>

This function will return the root Extent of the Extent Tree.  An error will be
thrown if more than one root Extent is found in the current database. A root
Extent is defined as an Extent which does not have a parent.  If no root extent
is found, C<undef> is returned.

NOTE: The result of this query is cached.  Once a real Extent Root is found,
subsequent calls will return the same value without querying the database.

=back

=cut

sub getExtentRoot {
    my ($self) = @_;

    # Return fast if we already know the root extent
    if (defined($self->{extent_root})) {
        return $self->{extent_root}
    }
    my $eid = undef;

    if ($self->runQuery('GET_EXTENT_ROOT')) {
        $eid = $self->fetchSingle('GET_EXTENT_ROOT');

        # Store the result in the object
        $self->{extent_root} = $eid;

        # Check to see if we found more than one root extent
        if (defined $self->fetchSingle('GET_EXTENT_ROOT')) {
            $self->logError("getExtentRoot() - WARNING: Multiple root extents detected.", 1);
        }
    }
    $self->endQuery('GET_EXTENT_ROOT');

    #TODO We need to throw an error if no root is found.
    return $eid
}

=item B<< @eids = @{$glk->getExtentRoots()} >>

This function will return a reference to a list containing the Extent ID of all the root Extents in the Extent Tree.
A root Extent is defined as an Extent which does not have a parent.  If no root extent
is found, a reference to an empty array is returned.

=back

=cut

sub getExtentRoots {
    my ($self) = @_;

    # Return fast if we already know the root extent
    if (defined($self->{extent_root})) {
        return $self->{extent_root}
    }
    my @roots = ();

    if ($self->runQuery('GET_EXTENT_ROOT')) {
        while (my $eid = $self->fetchSingle('GET_EXTENT_ROOT')) {
            push(@roots, $eid);
        }
        $self->endQuery('GET_EXTENT_ROOT');
    }
    return \@roots
}

=over

=item B<< $parent_eid = $glk->getParentExtent($eid) >>

This function refrieves the Extent ID of the extent supplied.  If the Extent is
not found, or if the Extent has no parent (ie: it is the root Extent) then
C<undef> is returned.  NOTE: It is not an error for the root Extent to have an
undefined parent.  Programmers should not interpret an C<undef> return value as
an error.  Instead, they should use C<extentExists()> to check for the
non-existance of the Extent.

=back

=cut

sub getParentExtent {
    my ($self, $eid) = @_;
    my $parent = undef;

    if (exists($self->{daddy}) && exists($self->{daddy}{$eid})) {
         return $self->{daddy}{$eid}
    }
    if ($self->runQuery('GET_EXTENT_PARENT', $eid)) {
        $parent = $self->fetchSingle('GET_EXTENT_PARENT');
        $self->{daddy}{$eid} = $parent;
    }
    $self->endQuery('GET_EXTENT_PARENT');

    return $parent
}

=over

=item B<< $ancestor_arrayref = $glk->getAncestors($eid) >>

This function returns a reference to an array of the Extent ancestors of the
given Extent.  The most distant ancestor (the root of the Extent tree) is found
in the first position of the array, and the Extent ID given is found in the last
position of the array.

=back

=cut

sub getAncestors {
    my ($self, $eid) = @_;
    my $parent = $self->getParentExtent($eid);
    my $ancestors = [];

    if (defined($parent)) {
        $ancestors = $self->getAncestors($parent);
    }
    push(@{$ancestors}, $eid);
    return $ancestors
}

=over

=comment NO LONGER USED 2017-04-20

=item B<< $db = $glk->getSampleDbByBacId($bid) >>

Given a BAC ID (sample_id), it returns the database to which the sample belong or undef, if no sample has been found.
In the case of no sample, an error is also thrown.

=back

#=cut

sub getSampleDbByBacId {
    my ($self, $bid) = @_;

    unless (defined($bid) && $bid =~ /^\d+$/) {
        $self->bail("getSampleDbByBacId() - Called without a defined or valid BAC ID");
    }
    unless (exists($self->{SAMPLE_ADDRESS}{BID}{$bid})) {
        $self->_loadSampleLocationByBacId($bid);
    }
    if (exists($self->{SAMPLE_ADDRESS}{BID}{$bid})) {
        return $self->{SAMPLE_ADDRESS}{BID}{$bid}{db}
    }
    else {
        $self->error("getSampleDbByBacId() - Impossible to find a sample correspondent to BAC ID $bid.");
        return undef
    }
}
=cut

=over

=item B<< $db = $glk->getSampleDbByBlindedNumber($blinded_number) >>

Given a blinded_number, it returns the database to which the sample belong or undef, if no sample has been found.
In the case of no sample, an error is also thrown.

=back

=cut

sub getSampleDbByBlindedNumber {
    my ($self, $bl_no) = @_;

    unless (defined($bl_no)) {
        $self->bail("getSampleDbByBlindedNumber() - Called without a defined blinded_number");
    }
    unless (exists($self->{SAMPLE_ADDRESS}{BLINO}{$bl_no})) {
        $self->_loadSampleLocationByBlindedNumber($bl_no);
    }
    if (exists($self->{SAMPLE_ADDRESS}{BLINO}{$bl_no})) {
        return $self->{SAMPLE_ADDRESS}{BLINO}{$bl_no}{db}
    }
    else {
        $self->logError("getSampleDbByBlindedNumber() - Impossible to find a sample correspondent to Blinded Number \"$bl_no\".");
        return undef
    }
}

=over

=item B<< $bid = $glk->getSampleIdByBlindedNumber($blinded_number) >>

Given a blinded_number, it returns the sample_id or undef, if no sample has been found.
In the case of no sample, an error is also thrown.

=back

=cut

sub getSampleIdByBlindedNumber {
    my ($self, $bl_no) = @_;

    unless (defined($bl_no)) {
        $self->bail("getSampleIdByBlindedNumber() - Called without a defined blinded_number");
    }
    unless (exists($self->{SAMPLE_ADDRESS}{BLINO}{$bl_no})) {
        $self->_loadSampleLocationByBlindedNumber($bl_no);
    }
    if (exists($self->{SAMPLE_ADDRESS}{BLINO}{$bl_no})) {
        return $self->{SAMPLE_ADDRESS}{BLINO}{$bl_no}{sample_id}
    }
    else {
        $self->error("getSampleIdByBlindedNumber() - Impossible to find a sample correspondent to Blinded Number \"$bl_no\".");
        return undef
    }
}

=over

=item B<< $children_arrayref = $glk->getExtentChildren($eid) >>

This function returns a reference to an array containing the Extent IDs of all
child Extents of the given Extent.  This is a list of direct children only, not
tree descendants.  

=back

=cut

sub getExtentChildren {
    my ($self, $eid) = @_;

    my @children = ();

    if ($self->runQuery('GET_EXTENT_CHILDREN_BY_ID', $eid)) {
        while (my ($child) = $self->fetchListRow('GET_EXTENT_CHILDREN_BY_ID')) {
            push @children, $child;
        }
    }
    $self->endQuery('GET_EXTENT_CHILDREN_BY_ID');

    return \@children
}

=over

=item B<< $children_arrayref = $glk->getExtentChildrenByType($eid, $extent_type) >>

This function returns a reference to an array of Extent IDs of all children of
the given extent which belong to the the supplied type.  The type can be either
in the form of an Extent Type ID or an Extent Type name.

=back

=cut

sub getExtentChildrenByType {
    my ($self, $eid, $type) = @_;

    $self->_cleanValue(\$type, 'getExtentChildrenByType()', '$type');

    my $typeid = $self->translateExtentType($type);

    unless (defined($typeid)) {
        $self->logWarn("getExtentChildrenByType() - Invalid Extent_Type (\"$type\")");
        return []
    }

    my @children = ();
    if ($self->runQuery('GET_EXTENT_CHILDREN_BY_TYPE', $eid, $typeid)) {
        while (my ($child) = $self->fetchListRow('GET_EXTENT_CHILDREN_BY_TYPE'))  {
            push @children, $child;
        }
    }
    $self->endQuery('GET_EXTENT_CHILDREN_BY_TYPE');

    return \@children
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< $children_arrayref = $glk->getExtentChildrenByTypeList($eid, $extent_type...) >>

This function returns a reference to an array of Extent IDs of all children of
the given Extent which belong to one of the types in the list supplied.
Multiple types can be supplied, and they may be supplied as either Extent Type
IDs or Extent Type names.  NOTE: This function uses a static SQL statement in
order to do its work.  This statement must be prepared for each function call,
and thus it is not nearly as efficient or database friendly as most other
functions provided by this module.

=back

#=cut

## TODO This is another ridiculus code duplication. It needs to get resolved by consecutive calls to getExtentChildrenByType()

my $SQL_GET_EXTENT_CHILDREN_BY_TYPELIST = '
SELECT e.Extent_id
FROM Extent e
WHERE e.parent_id = ? AND e.Extent_Type_id IN (%s)
';

sub getExtentChildrenByTypeList {
    my ($self, $eid, @types) = @_;

    my @typeids = ();
    foreach my $type (@types) {
        $self->_cleanValue(\$type, 'getExtentChildrenByTypeList()', '$type');

        my $typeid = $self->translateExtentType($type);

        if (defined($typeid)) {
            push(@typeids, $typeid);
        }
        else {
            $self->logWarn("getExtentChildrenByTypeList() - Invalid Extent_Type (\"$type\").");
        }
    }

    my $type_set = join(',', @typeids);

    my @children = ();

    my $st = $self->compile($SQL_GET_EXTENT_CHILDREN_BY_TYPELIST, $type_set);

    if ($st->execute($eid)) {
        while (my ($child) = $st->fetchrow_array()) {
            push @children, $child;
        }
    }
    $st->finish();

    return \@children
}
=cut
=comment NO LONGER USED 2017-04-20

=over

=item B<< $descendants_arrayref = $glk->getExtentDescendants($eid, $with_root, $levels) >>

This function returns a reference to an array of all Extent descendants of the
given Extent.  If C<$with_root> is defined (which it is, by default) then the
root Extent (the extent given as an argument) is included in the list.  The
Extent search will recurse up to C<$levels> levels, or indfinitely if not
defined (the default is to recurse indefinitely).  The list is built by inorder
appending of Extent IDs, but the order of the array should be considered
indeterminant.

=back

#=cut

sub getExtentDescendants {
    my ($self, $eid, $include_root, $levels) = @_;

    my @children = ();

    # Perform Recusion Limiting
    return \@children if (defined $levels and $levels < 1);
    $levels-- if defined ($levels);

    if ($include_root) {
        push(@children, $eid);
    }

    foreach my $child_eid (@{$self->getExtentChildren($eid)}) {
        push @children, @{$self->getExtentDescendants($child_eid, 1, $levels)};
    }
    return \@children
}
=cut
=comment NO LONGER USED 2017-04-20

=over

=item B<< $descendant_tree_hashref = $glk->getExtentDescendants_Hash($eid) >>

This function returns a reference to a multilevel hash tree of all the
descendants of the given Extent.

=back

#=cut
sub getExtentDescendants_Hash {
    my ($self, $eid,  $levels) = @_;
    my %children = ();

    # Perform Recusion Limiting
    if (defined $levels and $levels < 1) {
        return \%children
    }
    elsif (defined($levels)) {
        $levels--;
    }
    foreach my $child_eid (@{$self->getExtentChildren($eid)}) {
        #push @children, @{$self->getExtentDescendants_Hash($child_eid,  $levels)};
        $children{$child_eid} = $self->getExtentDescendants_Hash($child_eid,  $levels);
    }
    return \%children
}
=cut
=back

=cut

#
#  ---------------- Extent Attributes --------------------
#

=head3 Extent Attributes

=over

=cut

=over

=item B<< $attr_val = $glk->getExtentAttribute($eid, $attr_name) >>

This function returns the value of the given Attribute name for the supplied
Extent ID.  If the Attribute does not exist for that Extent, or if the Extent
itself does not exist, C<undef> is returned.

=back

=cut

sub getExtentAttribute {
    my ($self, $eid, $type) = @_;

    $self->_cleanValue(\$type, 'getExtentAttribute()', '$type');

    my $typeid = $self->getExtentAttrTypeID($type);

    if (!defined($typeid)) {
        $self->logWarn("getExtentAttribute() - Invalid attribute type (\"$type\").");
        return undef
    }
    my $value;
    if ($self->runQuery('GET_EXTENT_ATTR', $eid, $typeid)) {
        $value = $self->fetchSingle('GET_EXTENT_ATTR');
        $self->endQuery('GET_EXTENT_ATTR');
    }

    # dkatzel 2011/12 -
    # changed incorrect logic if undef.
    # Very often this get() call is made on an extent
    # that doesn't have the asked for attribute.
    # We should return undef in that case according to the documentation
    if(!defined($value)){
        return undef
    }
    if ($self->{VAL_VALIDATION}) {
        if ($value !~ /\S/) { # No real info in the argument (still acceptable for flag-type values)
            my $is_flag = $self->isFlagAttribute($type);
            if (defined($is_flag) && !$is_flag) {
                $self->logWarn("getExtentAttribute(), Extent $eid - Blank attribute value in non-flag attribute. Extent ID: $eid, ExtentAttributeType: $type");
            }
            elsif (!defined($is_flag)) {
                $self->logWarn("getExtentAttribute(), Extent $eid - Empty ExtentAttribute record found for Extent $eid attribute Type \"$type\" ('silent_warning attribute')");
            }
            undef($value);
        }
        elsif ($self->NCBI_Date() && $self->isDateAttribute($type)) {
            $value = $self->_ConvertToNCBIDate($value, 1);
        }
        if (defined($value)) {
            unless (exists($self->{AttrTypeChecked}{$eid}{$type}) && defined($self->{AttrTypeChecked}{$eid}{$type})) {
                $self->{AttrTypeChecked}{$eid}{$type} = $self->isCorrectValueType($type, $value, undef, $eid);
            }
            unless ($self->{AttrTypeChecked}{$eid}{$type}) {
                $self->logWarn("getExtentAttribute(), Extent $eid - The attribute value (\"$value\") is not compatible with the attribute type.");
                ## Not active right now, to be implemented at a later time:
                ## return undef
            }
        }
    }
    return $value
}

=head3 Extent Attributes

=over

=cut

=over

=item B<< $attr_val = $glk->getCombinedExtentAttribute($eid, $attr_name) >>

This function returns the value of the given Attribute name for the supplied
Extent ID, possibly together with the same attribute of all its ancestors, combined in a semicolons-separated list.
If the Attribute does not exist for that Extent, or any of its ancestors (in the case of inherited attributes), undef is returned.
or the attribute is a flag attribute without any value is present, an empty string will be returned in order to differentiate it from the case of being totally absent
It will raise an error message if the attribute type os not valid.

=back

=cut

sub getCombinedExtentAttribute {
    my ($self, $eid, $type, $separator) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        no warnings;
        $self->bail("getCombinedExtentAttribute() - Called with empty , undefined, or otherwise invalid Extent_id (\"$eid\")");
    }
    elsif (!defined($type) || $type !~ /^\S+$/) {
        no warnings;
        $self->bail("getCombinedExtentAttribute() - Called with empty or undefined ExtentAttribute Type (\"$type\")");
    }
    elsif (!defined($separator)) {
        $separator = ATTR_COMBINE_SEPARATOR;
    }
    $self->_cleanValue(\$type, 'getCombinedExtentAttribute()', '$type');
    my @values = ();
    my $r_ancestors = $self->getAncestors($eid);
    my $attr_found = 0;
    my $comb_rule = $self->getExtentAttrTypeCombiningRule($type);
    my $val_type  = $self->getExtentAttrValueType($type);
    my @ancestors = $comb_rule eq COMBINE_TOP_DOWN ? @{$r_ancestors} : reverse(@{$r_ancestors});

    foreach my $a_eid (@ancestors) {
        if ($self->hasExtentAttribute($a_eid, $type)) {
            my $val = $self->getExtentAttribute($a_eid, $type);
            ++$attr_found;

            if (defined($val) && $val =~ /\S/) {
                if ($comb_rule eq MERGE_ON_PLACEHOLDER) { ## Deal with author lists...
                    $self->mergeAuthors(\@values, $val);
                }
                else {
                    push(@values, $val);
                }
            }
            if ($comb_rule eq FIRST_FOUND_ABOVE) { ## We stop at the first found.
                last;
            }
        }
        if ($comb_rule eq ONLY_AT_THIS_LEVEL) { ## We search only ath the current level.
            last;
        }
    }
    if (scalar(@values)) {
        if ($comb_rule eq MERGE_ON_PLACEHOLDER) {
            return $self->toLastNameFirstAuthorList(\@values)
        }
        else {
            ## Trying to eliminate redundant values in the results...
            my $combined_val = $self->_deRedundifyAttr(\@values, $val_type, $separator);
            return $combined_val
        }
    }
    elsif ($attr_found) {
        return ''
    }
    else {
        return undef
    }
}

=over

=item B<< $is_published = $glk->isPublished($eid)
          $is_published = $glk->isPublished($bid)>>

given either an Extent ID or the BAC ID, this function returns 1 if the sample to which the given Extent belongs (accepted types 'sample' and any children of 'sample') has the 'jira_status' attribute set to 'Sample Published' 0 otherwise.
It wil throw a fatal exception if called with an Extent above the sample level.

=back

=cut

sub isPublished {
    my ($self, $id) = @_;
    my $eid;

    if (!defined($id)) {
        $self->bail("isPublished() called with undefined/missing required attribute Extent ID.");
    }
    elsif ($id =~ /\D/) {
        $self->bail("isPublished() called with invalid value (\"$id\") for required attribute Extent ID/BAC ID.");
    }
    elsif ($id =~ /^\d{5}$/) {
        $eid = $self->getExtentByTypeRef('SAMPLE', $id);
    }
    else {
        $eid = $id;
    }
    unless (defined($self->{Published}{$eid})) {
        my $sam_eid = $eid;
        my $bid = 0;

        while ($sam_eid) {
            my $ext_info = $self->getExtentInfo($sam_eid);

            if ($ext_info->{type} eq 'SAMPLE') {
                $bid = $ext_info->{'ref'};
                last;
            }
            $sam_eid = $ext_info->{parent};
        }
        if ($bid) {
            if ($self->hasExtentAttribute($sam_eid, 'jira_status')) {
                my $status = $self->getExtentAttribute($sam_eid, 'jira_status');

                if ($status eq 'Sample Published') {
                    $self->{Published}{$sam_eid} = 1;
                    $self->{Published}{$eid}     = 1;
                }
                else {
                    $self->{Published}{$sam_eid} = 0;
                    $self->{Published}{$eid}     = 0;
                }
            }
            else {
                $self->bail("isPublished() Called with Extent $eid - Sample $bid (Extent $sam_eid) does not have mandatory 'jira_status' attribute.");
            }
        }
        else {
            $self->bail("isPublished() Called with Extent $eid - This extent either does not exist in the current database or it belongs to an entity higher than sample level. Unable to find any sample related to this extent.");
        }
    }
    return $self->{Published}{$eid}
}

=head3 Extent Attributes

=over

=item B<< $is_a_flag = $glk->isFlagAttribute($type) >>

This function returns 1 if the attribute type (either name or id) is recognized as being a flag-attribute (i.e. it does not need an associated value), 0 if the attrribute is definitely a non-flag attribute, undef when the value type is "silent_warning" (known attributes that should have a value associated with--see http://confluence.jcvi.org/pages/viewpage.action?pageId=8945679).
It also return undef also when it is called with an invalid ExtentAttributeType.
=back

=cut

sub isFlagAttribute {
    my ($self, $type) = @_;

    unless (defined($type) && $type =~ /^\S+$/) {
        no warnings;
        $self->bail("isFlagAttribute() - Called with empty, undefined, or otherwise invalid ExtentAttribute Type (\"$type\")");
    }
    $self->_cleanValue(\$type, 'isFlagAttribute()', '$type');
    my $val_type = $self->getExtentAttrValueType($type);

    if (defined($val_type)) {
        if ($val_type eq 'flag') {
            return TRUE
        }
        elsif ($val_type eq 'silent_warning') {
            return undef
        }
        else {
            return FALSE
        }
    }
    else {
        $self->logWarn("isFlagAttribute() - Called with invalid ExtentAttributeType (\"$type\")");
        return undef
    }
}

=over

=item B<< $is_a_ignore_attr = $glk->isIgnoreAttribute($type) >>

This function returns 1 if the attribute type (either name or id) is recognized as being an attribute with value type set to 'ignore', 0 otherwise.
It also return undef also when it is called with an invalid ExtentAttributeType.
=back

=cut

sub isIgnoreAttribute {
    my ($self, $type) = @_;

    unless (defined($type) && $type =~ /^\S+$/) {
        no warnings;
        $self->bail("isIgnoreAttribute() - Called with empty, undefined, or otherwise invalid ExtentAttribute Type (\"$type\")");
    }
    $self->_cleanValue(\$type, 'isIgnoreAttribute()', '$type');
    my $val_type = $self->getExtentAttrValueType($type);

    if (defined($val_type)) {
        if ($val_type eq 'ignore') {
            return TRUE
        }
        else {
            return FALSE
        }
    }
    else {
        $self->logWarn("isIgnoreAttribute() - Called with invalid ExtentAttributeType (\"$type\")");
        return undef
    }
}

=over
=item B<< $yes_no = $glk->isDateAttribute($type) >>

This function returns 1 if the attribute type (either name or id) is recognized
as being a date, 0 otherwise.
It returns undef in the case the attribute provided is not a valid attribute (the call to getExtentAttrValueType() will also raise a warning for this issue).

=back

=cut

sub isDateAttribute {
    my ($self, $type) = @_;

    unless (defined($type) && $type =~ /^\S+$/) {
        no warnings;
        $self->bail("isDateAttribute() - Called with empty, undefined, or otherwise invalid ExtentAttribute Type (\"$type\")");
    }
    $self->_cleanValue(\$type, 'isDateAttribute()', '$type');
    my $val_type = $self->getExtentAttrValueType($type);

    if (defined($val_type)) {
        return $val_type eq 'date' ? 1 : 0
    }
    else {
        $self->logWarn("isDateAttribute() Called with invalid attribute type \"$type\"");
        return undef
    }
}

=over
=item B<< $yes_no = $glk->isDateListAttribute($type) >>

This function returns 1 if the attribute type (either name or id) is recognized as being a date_list, 0 otherwise.
It returns undef in the case the attribute provided is not a valid attribute (the call to getExtentAttrValueType() will also raise a warning for this issue).

=back

=cut

sub isDateListAttribute {
    my ($self, $type) = @_;

    unless (defined($type) && $type =~ /^\S+$/) {
        no warnings;
        $self->bail("isDateListAttribute() - Called with empty, undefined, or otherwise invalid ExtentAttribute Type (\"$type\")");
    }
    $self->_cleanValue(\$type, 'isDateListAttribute()', '$type');
    my $val_type = $self->getExtentAttrValueType($type);

    if (defined($val_type)) {
        return $val_type eq 'date_list' ? 1 : 0
    }
    else {
        $self->logWarn("isDateListAttribute() Called with invalid attribute type \"$type\"");
        return undef
    }
}


=over

=item B<< $boolean = $glk->hasExtentAttribute($eid, $attr_name) >>

This function checks to see if the given Extent ID has an associated Attribute
of the supplied type name.  If an Attribute is found, 1 is returned, otherwise
C<undef> is returned.

=back

=cut

sub hasExtentAttribute {
    my ($self, $eid, $type) = @_;

    $self->_cleanValue(\$type, 'hasExtentAttribute()', '$type');

    if (!defined($eid)) {
        $self->logWarn("hasExtentAttribute() - Called with undefined Extent_id");
        return undef
    }
    elsif ($eid !~ /^\d+$/) {
        $self->logWarn("hasExtentAttribute() - Called with an invalid Extent_id (\"$eid\")");
        return undef
    }
    elsif (!defined($type) || $type !~ /\S/) {
       $self->logWarn("hasExtentAttribute() - Called with an undefined or blank ExtentAttributeType(_id).");
        return undef
    }
    my $typeid = $self->getExtentAttrTypeID($type);

    unless (defined($typeid)) {
        $self->logWarn("hasExtentAttribute() - Called with an invalid ExtentAttributeType (\"$type\") Extent: $eid.");
        return undef
    }

    my $success = undef;
    if ( $self->runQuery('HAS_EXTENT_ATTR', $eid, $typeid)) {
        $success = $self->fetchSingle('HAS_EXTENT_ATTR');
        $self->endQuery('HAS_EXTENT_ATTR');
    }

    return $success
}

=over

=item B<< my $yes_no = hasSegmentGaps($asmbl_id) >>

Given an Extent_id of a segment-level Extent, it returns 1 if the segment has internal sequencing gaps, 0 if not.
If the Extent is not found or lacks the finishing_grade attribute, it will raise a fatal exception.

=cut

sub hasSegmentGaps {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid =~ /^\s*$/) {
        $self->bail("hasSegmentGaps() - Called with undefined/empty Extent ID.");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->bail("hasSegmentGaps() - Called with invalid Extent ID (\"$eid\").");
    }
    if ($self->hasExtentAttribute($eid, 'finishing_grade')) {
        my $fg = $self->getExtentAttribute($eid, 'finishing_grade');

        if (exists($finishing_grade{$fg})) {
            return $finishing_grade{$fg}[0]
        }
        else {
            $self->bail("hasSegmentGaps() - Invalid value found in \"finishing_grade\" attribute.");
        }
    }
    else {
        $self->bail("hasSegmentGaps() - Extent $eid does not have the \"finishing_grade\" attribute. Is it a valid segment-level Extent?");
    }
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< my $yes_no = hasAssemblyGaps($segment_extent_id) >>

Given an assembly_id it returns 1 if the segment has internal sequencing gaps, 0 if not.
If the assembly does not exsist or is not a current assembly, it will raise a fatal exception.

#=cut

sub hasAssemblyGaps {
    my ($self, $asmbl_id) = @_;

    if (!defined($asmbl_id) || $asmbl_id =~ /^\s*$/) {
        $self->bail("hasAssemblyGaps() - Called with undefined/empty assembly ID.");
    }
    elsif ($asmbl_id !~ /^\d+$/) {
        $self->bail("hasAssemblyGaps() - Called with invalid assembly ID (\"$asmbl_id\").");
    }
    my $eid = $self->getSegmentEidByAsmbl($asmbl_id);

    if (defined($eid)) {
        return $self->hasSegmentGaps($eid)
    }
    else {
        $self->logError("hasAssemblyGaps() - Impossible to find a segment Extent associated with the given assembly ID ($asmbl_id) (is it the current assembly for that segment?).");
        return undef
    }
}
=cut
=over

=item B<< my $yes_no = isSegmentComplete($asmbl_id) >>

Given an assembly_id it returns 1 if it is complete (i.e. it contains both the start of the first ORF and the end of the last one), 0 if not.
If the assembly does not exsist or is not a current assembly, it will raise an error message and return undef.

=cut

sub isSegmentComplete {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid =~ /^\s*$/) {
        $self->bail("isSegmentcomplete() - Called with undefined/empty Extent ID.");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->bail("isSegmentcomplete() - Called with invalid Extent ID (\"$eid\").");
    }
    if ($self->hasExtentAttribute($eid, 'finishing_grade')) {
        my $fg = $self->getExtentAttribute($eid, 'finishing_grade');

        if (exists($finishing_grade{$fg})) {
            return $finishing_grade{$fg}[1]
        }
        else {
            $self->bail("isSegmentcomplete() - Invalid value (\"$fg\") found in \"finishing_grade\" attribute.");
        }
    }
    else {
        $self->bail("isSegmentcomplete() - Extent $eid does not have the \"finishing_grade\" attribute. Is it a valid segment-level Extent?");
    }
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< my $yes_no = isAssemblyComplete($segment_extent_id) >>

Given an Extent_id of a segment-level Extent, it returns 1 if the segment is complete (i.e. it contains both the start of the first ORF and the end of the last one), 0 if not.
If the Extent is not found or lacks the finishing_grade attribute, it will raise an exception and return undef.
#=cut

sub isAssemblyComplete {
    my ($self, $asmbl_id) = @_;

    if (!defined($asmbl_id) || $asmbl_id =~ /^\s*$/) {
        $self->bail("isAssemblyComplete() - Called with undefined/empty assembly ID.");
    }
    elsif ($asmbl_id !~ /^\d+$/) {
        $self->bail("isAssemblyComplete() - Called with invalid assembly ID (\"$asmbl_id\").");
    }
    my $eid = $self->getSegmentEidByAsmbl($asmbl_id);

    if (defined($eid)) {
        return $self->isSegmentComplete($eid)
    }
    else {
        $self->logError("isAssemblyComplete() - Impossible to find a segment Extent associated with the given assembly ID ($asmbl_id) (is it the current assembly for that segment?).");
        return undef
    }
}
=cut

=over

=item B<< @seq_tech = @{$glk->getSeqTechnologies($eid)} >>

Given a SEGMENT-level Extent ID, it returns a reference to a list of sequence technologies used.
In the case it is called with the Extent ID of a different type of extent, it will raise a fatal exception.

=back

=cut

sub getSeqTechnologies {
    my ($self, $eid) = @_;
    my @seq_tech;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        $self->bail("getSeqTechnologies() - Called with undefined or invalid Extent ID");
    }
    else {
        my $ext_type = $self->getExtentType($eid);

        if ($ext_type ne 'SEGMENT') {
            $self->bail("getSeqTechnologies() - Called with \"$ext_type\" Estent, instead of \"SEGMENT\".");
        }
    }
    ## If we reach this point, we're dealing with a SEGMENT-type Extent and we will be agnostic if it is deprecated or not.
    my %seq_tech_att = (has_454         => '454',
                        has_illumina    => 'Illumina',
                        has_sanger      => 'Sanger',
                        has_ionTorrent  => 'Ion Torrent');

    my $r_attrs = $self->getExtentAttributes($eid);

    foreach my $attr (keys(%{$r_attrs})) {
        if (exists($seq_tech_att{$attr})) {
            push(@seq_tech, $seq_tech_att{$attr});
        }
    }
    return \@seq_tech
}
=over

=item B<< $yes_no = $glk->hasSeqTechnologies($eid) >>

Given a SEGMENT-level Extent ID, it returns 1 if it has at least one of the attributes related to sequence technologies, 0 otherwise.
In the case it is called with the Extent ID of a different type of extent, it will raise a fatal exception.

=back

=cut

sub hasSeqTechnologies {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        $self->bail("hasSeqTechnologies() - Called with undefined or invalid Extent ID");
    }
    else {
        my $ext_type = $self->getExtentType($eid);

        if ($ext_type ne 'SEGMENT') {
            $self->bail("hasSeqTechnologies() - Called with \"$ext_type\" Estent, instead of \"SEGMENT\".");
        }
    }
    ## If we reach this point, we're dealing with a SEGMENT-type Extent and we will be agnostic if it is deprecated or not.
    my $r_seq_tech = $self->getSeqTechnologies($eid);


    return scalar(@{$r_seq_tech}) ? 1 : 0
}

=over

=item B<< $bioproject_id_list = $glk->getBioprojectIds($eid) >>

Given an Extent ID, it returns a string containing a non-redundant list of BioProject IDs assigned to the given Extent.
If no bioproject_id is found,it returns undef.

=back

=cut

sub getBioprojectIds {
    my ($self, $eid) = @_;

    if (!defined($eid)) {
        $self->bail("getBioprojectIds() - Called without defined Extent_id");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->bail("getBioprojectIds() - Invalid value (\"$eid\") for Extent_id.");
    }
    my @bp = ();

    my $r_ancestors = $self->getAncestors($eid);

    foreach my $ext (@{$r_ancestors}) {
        my $r_atts = $self->getExtentAttributes($ext);

        if (exists($r_atts->{bioproject_id}) && defined($r_atts->{bioproject_id})) {
            push(@bp, $r_atts->{bioproject_id});
        }
    }
    if (scalar(@bp)) {
    ## Compacting the list...
        my $bp_string = $self->deredundify_string(join(',', @bp), qr/[;,]\s*/, ',');
        return $bp_string
    }
    else {
        return undef
    }
}

=over

=item B<< @bioproject_ids = @{$glk->getBioprojectsList($eid)} >>

Given an Extent ID, it returns a reference to a list containing a non-redundant set of BioProject IDs assigned to the given Extent.
If no bioproject_id is found, a reference to an empty array is returned.

=back

=cut

sub getBioprojectsList {
    my ($self, $eid) = @_;

    if (!defined($eid)) {
        $self->bail("getBioprojectsList() - Called without defined Extent_id");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->bail("getBioprojectaList() - Invalid value (\"$eid\") for Extent_id.");
    }

    my $bp_string = $self->getCombinedExtentAttribute($eid, 'bioproject_id', ATTR_COMBINE_SEPARATOR) || '';
    my @bp = split(/[,;]\s*/, $bp_string);

    if (scalar(@bp)) {
    ## Compacting the list...
        $self->deredundifyList(\@bp);
    }
    return \@bp
}


=comment NO LONGER USED 2017-04-20

=over

=item B<< $bioproject_id_list = $glk->getProjectIds($eid) >>

Given an Extent ID, it returns a string containing a non-redundant list of BioProject IDs assigned to the given Extent and its parents.
If no bioproject_id is found,it returns undef.

=back

#=cut

sub getProjectIds {
    my ($self, $eid) = @_;
    return $self->getBioprojectIds($eid)
}
=cut
=over

=item B<< $collection_id = $glk->getcollection($eid) >>

Given an Extent ID, it returns a collection ID if any is found. If the Extent does not exist or is flagged as deprecated, it returns undef.

=back

=cut

sub getCollection {
    my ($self, $eid) = @_;

    if (!defined($eid)) {
        $self->bail("getCollection() - Called with undefined Extent ID.");
    }
    elsif ($eid !~ /^\d+$/) {
        $self->bail("getCollection() - Invalid value (\"$eid\") for Extent ID.");
    }
    elsif (!$self->extentExists($eid)) {
        $self->bail("getCollection() - Extent $eid does not exist in this database.");
    }
    elsif ($self->isDeprecated($eid)) {
        $self->logWarn("getCollection() - Called with deprecated Extent ($eid).");
        return undef
    }
    my $r_ancestors = $self->getAncestors($eid);

    foreach my $p_eid (@{$r_ancestors}) {
        my $r_info = $self->getExtentInfo($p_eid);

        if ($r_info->{type} eq 'COLLECTION') {
            return $r_info->{ref}
        }
    }
    ## We get here only if we didn't find any parent collection.
    $self->logWarn("getCollection() - Unable to find a collection for Extent $eid.");
    return undef
}
=over

=item B<< $collection_id = $glk->getcollectionxtentId($collection_name, $db) >>

Given a collection name and--optionally--the name of the database, it returns the Extent_id corresponding to that collection extent.
If no database is given, it will search in the current database.

=back

=cut

sub getCollectionExtentId {
    my ($self, $coll_name, $db) = @_;

    unless (defined($coll_name)) {
        $self->bail("getCollectionExtentId() - Called with undefined Collection name.");
    }
    unless (defined($db)) {
        $db = $self->getDbName();
    }
    unless (defined($self->{COLLECTION})) {
        $self->_loadVcCollections();
    }
    if (exists($self->{COLLECTION}{$db}{$coll_name}) && defined($self->{COLLECTION}{$db}{$coll_name})) {
        my $eid = $self->{COLLECTION}{$db}{$coll_name};
        return $eid
    }
    else {
        $self->logWarn("getCollection() - Unable to find collection \"$coll_name\" for database \"$db\".");
        return undef
    }
}

=over

=item B<< $attr_hashref = $glk->getExtentAttributes($eid) >>

This function returns a reference to a hash containing attribute/value pairs
of all the attributes set for the given Extent.

=item B<< $attr_hashref = $glk->getExtentAttributes($eid, \@errors) >>

If a reference to an array is passed, it populates it with a list of the invalid attributes and the correspondent error messages

=back

=cut

sub getExtentAttributes {
    my ($self, $eid, $r_err_list) = @_;

    if (! defined($eid) || $eid !~ /^\d+$/) {
        $self->bail("getExtentAttributes() called without a valid Extent ID.");
    }
    elsif (defined($r_err_list) && ref($r_err_list) ne 'ARRAY') {
        $self->bail("getExtentAttributes() The second (optional) argument must be a reference to and array.");
    }


    # Pre-load extent attribute types
    $self->preloadExtentAttrTypes();
    my %fetched_attrs = ();
    my %attrs = ();

    if ($self->runQuery('GET_EXTENT_ATTRS', $eid)) {
        while (my ($typeid, $val) = $self->fetchListRow('GET_EXTENT_ATTRS')) {
            $fetched_attrs{$typeid} = $val;
        }
        $self->endQuery('GET_EXTENT_ATTRS');

        while (my ($typeid, $val) = each(%fetched_attrs)) {
            my $attr = $self->getExtentAttrTypeName($typeid);

            #
            #  Check to make sure the type was found
            #     (This is not guaranteed by the current schema)
            #
            if (defined $attr) {
                if ($self->{VAL_VALIDATION}) {
                    if ((!defined($val) || $val !~ /\S/)) { # No real info in the argument (still acceptable for flag-type values)
                        my $is_flag = $self->isFlagAttribute($attr);
                        my $is_ignore = $self->isIgnoreAttribute($attr);

                        if (defined($is_flag) && !$is_flag && !$is_ignore) { # It isn't a flag attribute, nor an attribute whose value can be ignored.
                            $self->logWarn("getExtentAttributes(), Extent $eid - Empty ExtentAttribute record found for attribute Type \"$attr\"");

                            if (defined($r_err_list) && !exists($self->{AttrTypeChecked}{$eid}{$attr})) {
                                push(@{$r_err_list}, [$attr, "Empty ExtentAttribute record found for Extent $eid"]);
                            }
                            $self->{AttrTypeChecked}{$eid}{$attr} = 0;
                            next # We don't want to mess around with empty attributes
                        }
                        elsif (!defined($is_flag)) { ## It's a 'silent_warning' attribute...
                            $self->logWarn("getExtentAttributes(), Extent $eid - Blank attribute value in 'silent_warning' attribute. - Extent ID: $eid attribute Type \"$attr\"");

                            if (defined($r_err_list) && !exists($self->{AttrTypeChecked}{$eid}{$attr})) {
                                push(@{$r_err_list}, [$attr, "Empty ExtentAttribute record ('silent_warning' type) found for Extent $eid"]);
                            }
                            $self->{AttrTypeChecked}{$eid}{$attr} = 0;
                        }
                        else {
                            $self->{AttrTypeChecked}{$eid}{$attr} = 1;
                        }
                    }
                    elsif ($val eq EXT_ATTR_UNKNOWN_VAL) {
                        $self->logTrace("getExtentAttributes() - found \"$val\" as value of attribute \"$attr\" (Technically, this is an acceptable value).");
                        $self->{AttrTypeChecked}{$eid}{$attr} = 1;
                    }
                    elsif (lc($val) eq lc(EXT_ATTR_UNKNOWN_VAL)) {
                        $self->logWarn("getExtentAttributes() - found \"$val\" as value of attribute \"$attr\". Converting the spelling into the accepted version \"" . EXT_ATTR_UNKNOWN_VAL . "\".");
                        $val = EXT_ATTR_UNKNOWN_VAL;

                        unless ($self->setExtentAttribute($eid, $attr, $val)){
                            $self->logWarn("getExtentAttributes() - Extent: $eid, Attribute: \"$attr\", Value: \"$val\" - Unable to update the value in the database - make sure you have the right credentials.");
                        }

                        $self->{AttrTypeChecked}{$eid}{$attr} = 1;
                    }
                    elsif ($self->isDateAttribute($attr)) {
                        ##Converting to NCBI date format is a cheap way to assess if a date is valid
                        my $new_val = $self->_ConvertToNCBIDate($val, 1);

                        if (defined($new_val)) {
                            $self->{AttrTypeChecked}{$eid}{$attr} = 1;

                            if ($self->NCBI_Date()) {
                                $val = $new_val;
                            }
                        }
                        else { # All the warning fuss is taken care to the called method.
                            if (defined($r_err_list) && !exists($self->{AttrTypeChecked}{$eid}{$attr})) {
                                push(@{$r_err_list}, [$attr, "Invalid date (\"$val\") for Extent $eid"]);
                            }
                            $self->{AttrTypeChecked}{$eid}{$attr} = 0;
                            next
                        }
                    }
                    unless (exists($self->{AttrTypeChecked}{$eid}{$attr})) {
                        my $err_msg = '';
                        $self->{AttrTypeChecked}{$eid}{$attr} =  $self->isCorrectValueType($attr, $val, \$err_msg, $eid);

                        unless ($self->{AttrTypeChecked}{$eid}{$attr}) {
                            $self->logWarn("getExtentAttributes(), Extent $eid, Attribute Type \"$attr\" - Invalid value type (\"$val\").");

                            if (defined($r_err_list)) {
                                push(@{$r_err_list}, [$attr, "Invalid value type (\"$val\") for Extent $eid: \"$err_msg\""]);
                            }
                        }
                        ## Not operative right now. it will be put in place when the datatype analtsis is more robust and reliable:
                        # next
                    }
                }
                $attrs{$attr} = $val;
            }
            else {
                $self->logWarn("getExtentAttributes() - Unidentified Type Found: $typeid. (Extent ID: $eid)");

                if (defined($r_err_list)) {
                    push(@{$r_err_list}, [$attr, "Unidentified Type Found: $typeid. for Extent $eid"]);
                }
            }
        }
    }
    return \%attrs
}

=over

=item B<< $attribute_arrayref = $glk->getAllExtentAttributeTypes() >>

This function returns an array of all the Extent Attributes in this project

=back

=cut

sub getAllExtentAttributeTypes {
    my $self = shift;
    my @attrs = ();

    if ($self->runQuery('GET_EXTENT_ATTR_TYPES')) {
        while (my $attr = $self->fetchListRow('GET_EXTENT_ATTR_TYPES')) {
            push @attrs, $attr;
        }
    }
    $self->endQuery('GET_EXTENT_ATTR_TYPES');

    return \@attrs
}

=item B<< $success = $glk->addExtentAttribute($eid, $type, $value) >>

This function adds a new Attribute of the supplied type to the given Extent.  If
an Attribute of that type already exists, it will fail, returning C<undef>.

=back

=cut

sub addExtentAttribute {
    my ($self, $eid, $type, $value) = @_;

    if (!defined($eid)) {
        $self->logError("addExtentAttribute() - Called with undefined Extent_id", 1);
        return undef
    }
    elsif ($eid =~ /\D/) {
        $self->logError("addExtentAttribute() - Called with an invalid Extent_id (\"$eid\")", 1);
        return undef
    }
    elsif (!defined($type) || $type !~ /\S/) {
       $self->logError("addExtentAttribute() - Called with an undefined or blank ExtentAttributeType.", 1);
        return undef
    }
    $self->_cleanValue(\$type,  'addExtentAttribute()', '$type');

    my $typeid = $self->getExtentAttrTypeID($type);

    unless (defined($typeid)) {
        $self->logWarn("addExtentAttribute() - Invalid ExtentAttributeType (\"$type\")");
        return undef
    }
    ## Removing the extent from the consistency check cached results
    undef($self->{ExtAttrTroubles}{$eid});
    undef($self->{AttrTypeChecked}{$eid});
    delete($self->{ExtAttrTroubles}{$eid});
    delete($self->{AttrTypeChecked}{$eid});

    my $is_flag = $self->isFlagAttribute($type);

    if (!defined($value) || $value !~ /\S/) {
        if (defined($is_flag) && !$is_flag) { ## a value is required...
            $self->logError("addExtentAttribute() - Called with an undefined or blank value on an attribute type whis is neither a flag or a silent_warning (\"$type\"). - Skipping it.");
            return undef
        }
        else {
            unless (defined($value)) {
                $value = ' ';
            }
        }
    }
    else {
        $self->_cleanValue(\$value, 'addExtentAttribute()', '$value');
        my $msg = '';

        unless ($self->{AttrTypeChecked}{$eid}{$type} = $self->isCorrectValueType($type, $value, \$msg, $eid)) {
            $self->logWarn("addExtentAttribute() Extent ID: $eid, Attribute Type: $type, Value: \"$value\" - The value did not match the expected type (still entering it for now, but not for too long):\n$msg");

            ## To be activated in the future:
            # return undef
        }
    }

    if ($self->hasExtentAttribute($eid, $type)) {
        $self->logWarn("addExtentAttribute() - Attempt of inserting an already-existing attribute \"$type\" (value: \"$value\") for Extent $eid");
        return undef
    }
#=comment ### Paolo Amedeo 2012 11 28 - Temporary fix: Perl modules to parse dates and times are currently unreliable. Turning off any date and time validation till implementing a proper replacement within the ProcessingObject library.

    if ($self->NCBI_Date() && $self->isDateAttribute($type)) { # If the dates are required to be in NCBI format (generally non-flu projects)
        my $clean_date = $self->_ConvertToNCBIDate($value);
        if (defined($clean_date)) {
            $value = $clean_date;
        }
        else {
            $self->logWarn("addExtentAttribute() - Extent $eid - Date format not recognized(\"$value\") for attribute \"$type\".");
            return undef
        }
    }
#=cut
    ## Adding consistency checks to make sure that the combination Extent Type-Attribute Type is valid.
    ## For now, this validation will only raise a warning if we are entering a combination that is not deemed valid
    ## However, as soon as the validation is properly integrate, it will raise an error and won't enter the attribute.

    my $et = $self->getExtentInfo($eid)->{type};

    unless($self->isLegalExtAttrCombo($et, $typeid)) {
        $self->logWarn("addExtentAttribute() - \"Illegal\" combination of Extent Type (\"$et\") and ExtentAttribute type (\"$type\"). Entering it for now, but it won't last for long.");
    }

    if ($self->runQuery('ADD_EXTENT_ATTR', $eid, $typeid, $value)) {
        $self->endQuery('ADD_EXTENT_ATTR');
        return SUCCESS
    }
}

=over

=item B<< $success = $glk->setExtentAttribute($eid, $type, $value, [$dont_add, [$dont_update]]) >>

This function will set an attribute for the given Extent ID.  The type can be
given either as an Attribute Type Name or an Attribute Type ID.  If C<$dont_add>
evaluates to C<true> then the function will not add an Extent of that type if
it does not already exist.  Otherwise, the given Attribute will be added or
updated to be equal to the supplied value.
If C<$dont_update> evaluates to C<true> then the existing attribute will not be updated.

=back

=cut

## TODO Consider ignoring the existence of a non-flag attribute without a non-blank value ##

sub setExtentAttribute {
    my ($self, $eid, $type, $value, $dont_add, $dont_update) = @_;

    ## Parameters checking
    $self->_cleanValue(\$type, 'setExtentAttribute()', '$type');

    if (!defined($eid)) {
        $self->logError("setExtentAttribute() - Called with undefined Extent_id", 1);
        return undef
    }
    elsif ($eid !~ /^\d+$/) {
        $self->logError("setExtentAttribute() - Called with an invalid Extent_id (\"$eid\")", 1);
        return undef
    }
    elsif (!defined($type) || $type !~ /\S/) {
       $self->logError("setExtentAttribute() - Called with an undefined or blank ExtentAttributeType(_id).", 1);
        return undef
    }
    $self->_cleanValue(\$type,  'setExtentAttribute()', '$type');

    my $typeid = $self->getExtentAttrTypeID($type);

    unless (defined($typeid)) {
        $self->logWarn("setExtentAttribute() - Invalid ExtentAttributeType (\"$type\")");
        return undef
    }
    ## Removing the extent from the consistency check cached results
    undef($self->{ExtAttrTroubles}{$eid});
    undef($self->{AttrTypeChecked}{$eid});
    delete($self->{ExtAttrTroubles}{$eid});
    delete($self->{AttrTypeChecked}{$eid});

    ## Checking if the attribute is not just spaces (unless it is a flag attribute)

    my $is_flag = $self->isFlagAttribute($type);

    if (!defined($value) || $value !~ /\S/) {
        if (defined($is_flag) && !$is_flag) { ## It neither a 'flag' or a 'silent_warning' type...
            $self->logError("setExtentAttribute() - Attempt to insert an empty value into a non-flag attribute.", 1);
            return undef
        }
        else {
            $value = ' ';
        }
    }
    else {
        $self->_cleanValue(\$value, 'setExtentAttribute()', '$value');
    }

    #
    #  Check to see if the attribute exists
    #  If the attribute doesn't exist, we call addExtentAttribute() and the check for validity will be performed there
    #
    if ($self->hasExtentAttribute($eid, $type)) {
        # The attribute exists.  Update it.

        ## Checking if the value is of the expected type. If not, for now we have just some ranting, but in the future we'll return without inserting the value

        my $msg = '';

        unless ($self->{AttrTypeChecked}{$eid}{$type} = $self->isCorrectValueType($type, $value, \$msg, $eid)) {
            $self->logWarn("setExtentAttribute() Extent ID: $eid, Attribute Type: $type, Value: \"$value\" - The value did not match the expected type (still entering it for now, but not for too long):\n$msg");

            ## To be activated in the future:
            # return undef
        }


#=comment ### Paolo Amedeo 2012 11 28 - Temporary fix: Perl modules to parse dates and times are currently unreliable. Turning off any date and time validation till implementing a proper replacement within the ProcessingObject library.

        ## Checking if it is a date attribute and, if necessary, transforming it into the proper format

        if ($self->NCBI_Date() && $self->isDateAttribute($type)) {
            my $good_date = $self->_ConvertToNCBIDate($value);

            unless (defined($good_date)) {
                $self->logWarn("setExtentAttribute() - Impossible to translate the string \"$value\" into a NCBI-formatted date.");
                return undef
            }
            $value = $good_date;
        }
#=cut
        my $typeid = $self->translateExtentAttrType($type);
        unless (defined($typeid)) {
            $self->logWarn("setExtentAttribute() - Invalid attribute type (\"$type\").");
            return undef
        }
        if ($dont_update) {
            return -1
        }

        my $success = undef;

        if ($self->runQuery('UPDATE_EXTENT_ATTR', $value, $eid, $typeid)) {
            $success = 1;
        }
        $self->endQuery('UPDATE_EXTENT_ATTR');

        return $success
    }
    elsif ($dont_add) {
        return undef
    }
    else {
        # The attribute doesn't exist.  Add it.
        return $self->addExtentAttribute($eid, $type, $value)
    }

    # We should never get here.
    return undef
}


=over

=item B<< $success = $glk->removeExtentAttribute($eid, $type) >>

This function will remove an attribute for the given Extent ID.  The type can be
given either as an Attribute Type Name or an Attribute Type ID.  If the given
attribute does not exist, then no action is taken and C<undef> is returned.

=back

=cut

sub removeExtentAttribute {
    my ($self, $eid, $type) = @_;

    #
    #  Check to see if the attribute exists
    # dkatzel 2011/12 - change from getExtent to hasExtent
    # so we don't flag warnings that this attribute
    # is missing if we try to delete something that doesn't exist.
    #
    $self->_cleanValue(\$type, 'removeExtentAttribute()', '$type');

    unless (defined($self->hasExtentAttribute($eid, $type))) {
        return undef
    }
    # The attribute exists.  Update it.
    my $typeid = $self->getExtentAttrTypeID($type);

    unless (defined($typeid)) {
        return undef
    }
    ## Removing the extent from the consistency check cached results
    undef($self->{ExtAttrTroubles}{$eid});
    undef($self->{AttrTypeChecked}{$eid});
    delete($self->{ExtAttrTroubles}{$eid});
    delete($self->{AttrTypeChecked}{$eid});

    my $success = undef;

    if ($self->runQuery('DELETE_EXTENT_ATTR', $eid, $typeid)) {
        $success = 1;
    }
    $self->endQuery('DELETE_EXTENT_ATTR');

    return $success
}

# ##############################################################################
#
#    SEQUENCE READS
#
# ##############################################################################

=back

=head2 SEQUENCE READS

A SequenceRead is an object that represents a sequence read from a sequencing
well.

=over

=cut


=comment NO LONGER USED 2017-04-20

=over

=item B<< $info_hashref = $glk->getSeqReadInfo($srid) >>

This function returns a reference to a hash of information about the given
SequenceRead.  If the SequenceRead is not found, a reference to an empty hash is
returned. On success, the following hash values will exist:

    id        :  The SequenceRead ID
    seq_name  :  The Sequence name
    extent    :  The Extent ID of the SequenceRead's Extent
    type      :  The SequenceRead Type name
    type_id   :  The SequenceRead Type ID
    strand    :  The SequenceRead's strand value.
    direction :  The SequenceRead's direction ("F" or "R")

=back

#=cut

sub getSeqReadInfo {
    my ($self, $srid) = @_;

    my %info = ();

    if ($self->runQuery('GET_SEQREAD_INFO', $srid)) {
        my $row = $self->fetchRow('GET_SEQREAD_INFO');
        $self->endQuery('GET_SEQREAD_INFO');

        %info = ('id' => $row->{'SequenceRead_id'},
                 'seq_name' => $row->{'seq_name'},
                 'extent' => $row->{'Extent_id'},
                 'type' => $self->getSeqReadTypeName($row->{'SequenceReadType_id'}),
                 'type_id' => $row->{'SequenceReadType_id'},
                 'strand' => $row->{'strand'},
                 'direction' => (($row->{'strand'}) ? "R" : "F"),
                );
    }
     my $seq_name = $info{'seq_name'};
    $info{"CLV"} = $self->populateSequenceFeature ($seq_name,"CLV" );
    $info{"CLR"} =$self->populateSequenceFeature ($seq_name,"CLR" );
    $info{'avg_quality'} = $self->getAvgQuality($seq_name);

    return \%info
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $srid = $glk->getSeqReadByName($seq_name) >>

This function will return a SequenceRead ID for the supplied sequence name.  If
the sequence name does not exist, then C<undef> is returned.

=back

#=cut

sub getSeqReadByName {
    my ($self, $seq_name) = @_;

    $self->_cleanValue(\$seq_name, 'getSeqReadByName()', '$seq_name');

    my $srid = undef;

    if ($self->runQuery('GET_SEQREAD_BY_NAME', $seq_name)) {
        $srid = $self->fetchSingle('GET_SEQREAD_BY_NAME');
    }
    $self->endQuery('GET_SEQREAD_BY_NAME');

    return $srid
}
=cut 

=comment NO LONGER USED 2017-04-20

=over

=item B<< $srid = $glk->addSequenceRead($name, $parent_eid, $direction, $type, $srid) >>

This function creates a new SequenceRead with the given data.  The type can be
supplied as a SequenceRead Type Name or a SequenceRead Type ID.  The direction
is used to set the strand value of the SequenceRead.  A direction of "R" will
result in a strand value of 1.  Any other direction will set a strand value of
0.  The final optional parameter is the SequenceRead ID to use.  If this is not
set (which should be the most common cases) a new EUID will be used.
On success, a SequenceRead ID is returned. On failure, C<undef> is returned.

=back

#=cut

sub addSequenceRead {
    my ($self, $seq_name, $parent, $direction, $type, $srid) = @_;

    $self->_cleanValue(\$seq_name,  'addSequenceRead()', '$seq_name');
    $self->_cleanValue(\$direction, 'addSequenceRead()', '$direction');
    $self->_cleanValue(\$type,      'addSequenceRead()', '$type');

    my $typeid = $self->getSeqReadTypeID($type);

    unless (defined($typeid)) {
        return undef
    }
    my $strand = ($direction eq 'R') ? 1 : 0;

    #
    #  Check to see that we have an ID, or generate one.
    #
    unless (defined $srid) {
        $srid = getEUID();
        unless (defined($srid)) {
            return undef
        }
    }
    my $added = undef;

    if ($self->runQuery('ADD_SEQREAD', $srid, $seq_name, $strand, $parent, $typeid)) {
        $added = $srid
    }
    $self->endQuery('ADD_SEQREAD');
    return $added
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $success = $glk->setSeqReadParent($seq, $eid) >>

This function sets the parent Extent of a given SequenceRead, effectively moving
it within the Extent tree.  Note that this may also change the library which
will be associated with this SequenceRead.

=back

#=cut

sub setSeqReadParent {
    my ($self, $seqname, $exid) = @_;

    $self->_cleanValue(\$seqname, 'setSeqReadParent()', '$seqname');
    my $srid = $self->translateSeqName($seqname);

    unless ($srid) {
        return undef
    }
    my $success = undef;

    if ($self->runQuery('SET_SEQREAD_PARENT', $exid, $srid)) {
        $success = 1;
    }
    $self->endQuery('SET_SEQREAD_PARENT');

    return $success
}
=cut
#
#  ---------------- SequenceRead Lists --------------------
#

=head3 Listing Sequence Reads

=over

=cut

=over

=item B<< $srid_arrayref = $glk->getSeqReads($eid) >>

This function returns a reference to an array of SequenceReads which are direct
children of the given Extent.  If the given Extent has no children, or there is
an error, a reference to an empty list is returned.

NOTE: This function returns only SequenceReads directly belonging to the given
Extent.  In most cases, clients will want to fetch the list of SequenceReads
belonging to the Extent tree rooted at a certain Extent. See the
C<getAllSeqReads()> function for this functionality.

=back

=cut

sub getSeqReads {
    my ($self, $exid) = @_;

    my @seqs = ();
    if ($self->runQuery('GET_SEQREADS_BY_PARENT', $exid)) {
        while(my $srid = $self->fetchSingle('GET_SEQREADS_BY_PARENT')) {
            push @seqs, $srid;
        }
    }
    # TODO: Return undef on failure
    $self->endQuery('GET_SEQREADS_BY_PARENT');

    return \@seqs
}

=over

=item B<< $srid_arrayref = $glk->getAllSeqReads($eid, [$field_to_return]) >>

This function returns a reference to an array of SequenceRead IDs which contain
the IDs of all sequences belonging to the given Extent or any of its descendant
Extents.  This function uses a recursive algorithm to find all sequences
belonging to an Extent tree.  In most cases, this is the function users will
want to use when pulling sequence data for an Extent.

=back

=cut

sub getAllSeqReads {
    my ($self, $exid, $field) = @_;

    # By default fetch the SequenceRead_id
    unless (defined($field)) {
        $field = "SequenceRead_id";
    }
    my @seqs = ();

    if ($self->runQuery('GET_SEQREADS_BY_PARENT', $exid)) {
        while(my $row = $self->fetchRow('GET_SEQREADS_BY_PARENT')) {
            push @seqs, $row->{$field};
        }
    }
    $self->endQuery('GET_SEQREADS_BY_PARENT');

    foreach my $child_eid (@{$self->getExtentChildren($exid)}) {
        push @seqs, @{$self->getAllSeqReads($child_eid, $field)};
    }
    return \@seqs
}

=back

=cut

#
#  ---------------- Sequence Read Mates --------------------
#

=head3 Sequence Read Mates

=over

=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $srid_list_ref = $glk->getSequenceReadMates($srid) >>

This function returns a reference to an array of SequenceRead IDs which are
valid sequence matest of the given SequenceRead.  In order to be a valid mate,
a SequenceRead must belong to the same Extent, have a strand value unlike the
given SequenceRead and not be trash.  If no mates are found, a reference to
an empty array is returned.

=back

#=cut

sub getSequenceReadMates {
    my ($self, $srid) = @_;

    my $srinfo = $self->getSeqReadInfo($srid);

    my $eid = $srinfo->{'extent'};
    my $strand = $srinfo->{'strand'};

    my $mate_strand = ($strand) ? 0 : 1;

    my @mates = ();

    if ($self->runQuery('GET_MATES', $eid, $mate_strand)) {
        while(my $mate_srid = $self->fetchSingle('GET_MATES')) {
            push @mates, $mate_srid;
        }
    }
    $self->endQuery('GET_MATES');
    return \@mates
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $srid = $glk->getBestSequenceReadMate($srid) >>

This function returns the SequenceRead_id of the defined "best" mate for this
SequenceRead.  The "best" mate is defined as the mate with the longest edited
length.  If no mates are available for this sequence, C<undef> is returned.

=back

#=cut

sub getBestSequenceReadMate {
    my ($self, $srid) = @_;

    my @mates = @{$self->getSequenceReadMates($srid)};

    if (scalar(@mates) == 0) {
        return undef
    }
    my $best = shift @mates;
}
=cut

#
#  ---------------- Sequence Read Attributes --------------------
#

=head3 Sequence Read Attributes

=over

=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $attr_val = $glk->getSequenceReadAttribute($eid, $attr_name) >>

This function returns the value of the given Attribute name for the supplied
SequenceRead ID.  If the Attribute does not exist for that SequenceRead, or if
the SequenceRead itself does not exist, C<undef> is returned.

=back

#=cut

sub getSequenceReadAttribute {
    my ($self, $srid, $type) = @_;

    $self->_cleanValue(\$type, 'getSequenceReadAttribute()', '$type');
    my $typeid = $self->getSequenceReadAttrTypeID($type);

    unless (defined ($typeid)) {
        return undef
    }
    my $value;
    if ($self->runQuery('GET_SEQREAD_ATTR', $srid, $typeid)) {
        $value = $self->fetchSingle('GET_SEQREAD_ATTR');
    }
    $self->endQuery('GET_SEQREAD_ATTR');

    return $value
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $boolean = $glk->hasSequenceReadAttribute($eid, $attr_name) >>

This function checks to see if the given SequenceRead ID has an associated
Attribute of the supplied type name.  If an Attribute is found, 1 is returned,
otherwise C<undef> is returned.

=back

#=cut

sub hasSequenceReadAttribute {
    my ($self, $srid, $type) = @_;

    $self->_cleanValue(\$type, 'hasSequenceReadAttribute()', '$type');
    my $typeid = $self->getSequenceReadAttrTypeID($type);

    my $success = undef;
    if ( $self->runQuery('HAS_SEQREAD_ATTR', $srid, $typeid)) {
        $success = $self->fetchSingle('HAS_SEQREAD_ATTR');
    }
    $self->endQuery('HAS_SEQREAD_ATTR');

    return $success
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $attr_hashref = $glk->getSequenceReadAttributes($eid) >>

This function returns a reference to a hash containing attribute/value pairs
of all the attributes set for the given SequenceRead.

=back

#=cut

sub getSequenceReadAttributes {
    my ($self, $srid) = @_;

    my %attrs = ();

    if ($self->runQuery('GET_SEQREAD_ATTRS', $srid)) {
        while (my ($typeid, $val) = $self->fetchListRow('GET_SEQREAD_ATTRS')) {
            my $attr = $self->getSeqReadAttrTypeName($typeid);
            $attrs{$attr} = $val;
        }
    }
    $self->endQuery('GET_SEQREAD_ATTRS');

    return \%attrs
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $attribute_arrayref = $glk->getAllSequenceReadAttributeTypes() >>

This function returns an array of all the Sequence Read Attributes in this project

=back

#=cut

sub getAllSequenceReadAttributeTypes {
    my $self = shift;

    unless (exists($self->{seqreadattr_type_name})) {
        $self->loadSeqReadAttrTypes();
    }
    return keys %{$self->{seqreadattr_type_name}}
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $success = $glk->addSequenceReadAttribute($srid, $type, $value) >>

This function adds an Attribute to the give SequenceRead.  The type can be given
as either a SequenceRead Attribute Type Name or a SequenceRead Type ID.  On
error C<undef> is returned.

=back

#=cut

sub addSequenceReadAttribute {
    my ($self, $srid, $type, $value) = @_;
    my $typeid = $self->getSeqReadAttrTypeID($type);

    unless (defined $typeid) {
        $self->addSequenceReadAttributeType($type);
        $typeid = $self->getSeqReadAttrTypeID($type);
    }
    unless (defined($typeid)) {
        return undef
    }
    my $success = undef;

    if ($self->runQuery('ADD_SEQREAD_ATTR', $srid, $typeid, $value)) {
        $success = 1;
    }
    $self->endQuery('ADD_SEQREAD_ATTR');

    return $success
}
=cut

# ##############################################################################
#
#    LIBRARIES
#
# ##############################################################################

=back

=head2 LIBRARIES

A Library is a collected set of Extents and SequenceReads which are related by
the process which created them.

=over

=over

=item B<< $info_hashref = $glk->getLibraryInfo($srid) >>

This function returns a reference to a hash of information about the given
Library.  If the Library is not found, a reference to an empty hash is
returned. On success, the following hash values will exist:

    id        :  The Library ID
    extent    :  The ID of the root Extent belonging to this Library
    lims_ref  :  A unique string reference for this Library
    size      :  The nominal size of the Library
    desc      :  A description of the Library (optional)
    clonesys  :  The ID of the CloningSystem associated with this Library
    minsize   :  The latest minimum size calculated for this Library
    maxsize   :  The latest maximum size calculated for this Library

=back

=cut

sub getLibraryInfo {
    my ($self, $libid) = @_;

    my %info = ();

    if ($self->runQuery('GET_LIBRARY_INFO', $libid)) {
        my $row = $self->fetchRow('GET_LIBRARY_INFO');
        $self->endQuery('GET_LIBRARY_INFO');

        %info = ('id' => $row->{'Library_id'},
                 'extent' => $row->{'Extent_id'},
                 'lims_ref' => $row->{'lims_ref'},
                 'size' => $row->{'nominal_size'},
                 'desc' => $row->{'description'},
                 'clonesys' => $row->{'CloningSystem_id'},
                 'minsize' => undef,
                 'maxsize' => undef,
                );
    }

    #
    #  Try to get the policy and set the sizes if it exists.
    #
    my $policy = $self->getLibraryPolicy($libid);

    if (defined $policy) {
        $info{'minsize'} = $policy->{'min_size'};
        $info{'maxsize'} = $policy->{'max_size'};
    }
    return \%info
}
=comment NO LONGER USED 2017-04-20

=over

=item B<< $lib_id = $glk->getLibraryForExtent($eid) >>

This function will return the ID of the Library which the given Extent is most
closely associated with.  Because of the tree-nature of Extents, a given Extent
may fall under a number of Libraries.  This function will return only the
"closest" one (by tree-depth).  If the Extent does not have an associated
Library, C<undef> is returned.

=back

#=cut

sub getLibraryForExtent {
    my ($self, $eid) = @_;

    my $libid = undef;
    if ($self->runQuery('GET_LIBRARY_FOR_EXTENT', $eid)) {
        $libid = $self->fetchSingle('GET_LIBRARY_FOR_EXTENT');
    }
    $self->endQuery('GET_LIBRARY_FOR_EXTENT');

    return $libid
}
=cut

=over

=item B<< $lib_id = $glk->getLibraryByLimsRef($lims_ref) >>

This function will return the ID of the Library with the given LIMS ref.
If the LIMS ref is not found, C<undef> is returned.

=back

=cut

sub getLibraryByLimsRef {
    my ($self, $lims_ref) = @_;

    my $libid = undef;

    if ($self->runQuery('GET_LIBRARY_BY_LIMSREF', $lims_ref)) {
        $libid = $self->fetchSingle('GET_LIBRARY_BY_LIMSREF');
    }
    $self->endQuery('GET_LIBRARY_BY_LIMSREF');

    return $libid
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $libid = $glk->addLibrary($eid, $lims_ref, $size, $desc, $csid) >>

This function creates a Library and any supporting structures necessary. On
success, the newly created Library ID is returned.  On failure, C<undef> is
returned.

NOTE: Currently this is simply a wrapper around C<addLibraryRaw()>.  This
function should only be used by advanced users, as it is not fully implemented.

=back

#=cut

# TODO: These functions need to be looked at now that library relationships
#       have changed a bit.
sub addLibrary {
    # Library_id, Extent_id, lims_ref, nominal_size, description, CloneSys_id
    my ($self, $eid, $ref, $size, $desc, $csid) = @_;

    my $lib_id = $self->addLibraryRaw($eid, $ref, $size, $desc, $csid);
    # TODO: Add supporting table insertions
    return $lib_id
}
=cut

=over

=item B<< $libid = $glk->addLibraryRaw($eid, $lims_ref, $size, $desc, $csid) >>

This function creates a Library entry given the supplied information.  On
success, the newly created Library ID is returned.  On failure, C<undef> is
returned.

=back

=cut

sub addLibraryRaw {
    # Library_id, Extent_id, lims_ref, nominal_size, description
    my ($self, $eid, $ref, $size, $desc, $csid) = @_;

    $self->_cleanValue(\$desc, 'addLibraryRaw()', '$desc');
    my $libid = getEUID();

    unless (defined($libid)) {
        return undef
    }
    my $added = undef;

    if ($self->runQuery('ADD_LIBRARY', $libid, $eid, $ref, $size, $desc, $csid)) {
        $added = $libid;
    }
    $self->endQuery('ADD_LIBRARY');

    return $added
}

=over

=head3 Library Policy Management

These functions control the mapping of Extents to Libraries.

=back

=comment NO LONGER USED 2017-04-20


=over

=item B<< $libid = $glk->addLibraryPolicy($libid, $min_size, $max_size, $expid) >>

This function creates a Library Policy tied to the given Library ID.  The
Experiment ID is required and should already be created.

=back

#=cut

sub addLibraryPolicy {
    my ($self, $libid, $min_size, $max_size, $expid) = @_;

    my $added = undef;

    if ($self->runQuery('ADD_LIBRARY_POLICY', $libid, $min_size, $max_size, $expid)) {
        $added = $libid;
    }
    $self->endQuery('ADD_LIBRARY_POLICY');

    return $added
}
=cut

=over

=item B<< $policy_hashref = $glk->getLibraryPolicy($libid) >>

This function returns a reference to a hash containing the current policy
information about the given Library ID.

=back

=cut

sub getLibraryPolicy {
    my ($self, $lid) = @_;

    my $row = undef;
    if ($self->runQuery('GET_LIBRARY_POLICY', $lid)) {
        $row = $self->fetchRow('GET_LIBRARY_POLICY');
    }
    $self->endQuery('GET_LIBRARY_POLICY');

    return $row

}


=over

=head2 Library Stats Management

These functions help manage the statistics attached to Libraries.

=back

=comment NO LONGER USED 2017-04-20

=over

=item B<< $stat_template_id = $glk->addLibraryStatType($type) >>

This function adds a new Library Stat Template (Type) to the list of available
statistic types.  On success, the newly created type ID is returned.  On
failure, C<undef> is returned.

=back

#=cut

sub addLibraryStatType {
    my ($self, $type) = @_;

    $self->_cleanValue(\$type, 'addLibraryStatType()', '$type');

    my $description = "Added by GLKLib";

    my $added = undef;

    if ($self->runQuery('ADD_STATS_TEMPLATE', $type, $description)) {
        $added = 1;
    }
    $self->endQuery('ADD_STATS_TEMPLATE');

    if ($added) {
        $self->loadLibraryStatTypes();
        $added = $self->getLibraryStatID($type);
    }
    return $added
}
=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< $stat_value = $glk->getLibraryStat($libid, $stat) >>

This function will retrieve the given statistic for the supplied Library.
On success, the value is returned. On error, C<undef> is returned.

=back

#=cut

sub getLibraryStat {
    my ($self, $libid, $stat) = @_;

    $self->_cleanValue(\$stat, 'getLibraryStat()', '$stat');

    my $stat_id = $self->getLibraryStatID($stat);

    #
    #  We need to fetch the policy to get the current Experiment
    #
    my $policy = $self->getLibraryPolicy($libid);

    unless (defined($policy)) {
        return undef
    }

    my $exp_id = $policy->{'Experiment_id'};

    my $value = undef;

    if ($self->runQuery('GET_LIBRARY_STAT', $libid, $exp_id, $stat_id)) {
        $value = $self->fetchSingle('GET_LIBRARY_STAT');
    }
    $self->endQuery('GET_LIBRARY_STAT');

    return $value
}
=cut

=over

=item B<< $success = $glk->setLibraryCloneSys($leid, $csid) >>

This function will set the CloningSystem_id of the given library
  On success, C<1> is returned. On error, C<undef> is returned.

=back

=cut

sub setLibraryCloneSys {
    my ($self, $leid, $csid) = @_;

    $self->_cleanValue(\$leid, 'setLibraryCloneSys()', '$leid');
    $self->_cleanValue(\$csid, 'setLibraryCloneSys()', '$csid');

    my $added = undef;

    if ($self->runQuery('SET_LIBRARY_CLONE_SYS', $csid, $leid)) {
        $added = 1;
    }
    $self->endQuery('SET_LIBRARY_CLONE_SYS');

    return $added
}

=item B<< $success = $glk->setLibraryDescription($leid, $descr) >>

This function will set the description of the given library
  On success, C<1> is returned. On error, C<undef> is returned.

=back

=cut

sub setLibraryDescription {
    my ($self, $leid, $descr) = @_;

    $self->_cleanValue(\$leid,  'setLibraryDescription()', '$leid');
    $self->_cleanValue(\$descr, 'setLibraryDescription()', '$descr');

    my $added = undef;

    if ($self->runQuery('SET_LIBRARY_DESCR', $descr, $leid)) {
        $added = 1;
    }
    $self->endQuery('SET_LIBRARY_DESCR');

    return $added
}

=comment NO LONGER USED 2017-04-20



#
#  ---------------- Extent-Library Mappings --------------------
#

=head3 Extent-Library Mappings

These functions control the mapping of Extents to Libraries.

=over

#=cut

=over

=item B<< $success = $glk->linkExtentLibrary($eid, $libib) >>

This function links an Extent to a given Library.  This assigns a Library
relationship to the Extent and any SequenceReads belonging to that Extent.  If
the linking failed, or if another link exists, C<undef> is returned.

=back

#=cut

sub linkExtentLibrary {
    my ($self, $eid, $lid) = @_;

    my $success = undef;
    if ($self->runQuery('LINK_EXTENT_LIBRARY', $eid, $lid)) {
        $success = 1;
    }
    $self->endQuery('LINK_EXTENT_LIBRARY');

    return $success
}
=cut

# ##############################################################################
#
#    EXPERIMENTS
#
# ##############################################################################

=back

=head2 Experiment Management

These functions help manage the experiments which track the statistics applied
to libraries.


=comment NO LONGER USED 2017-04-20

=over

=item B<< $exp_hash = $glk->getExperiment($experiment_id) >>

This function returns a hash containing named fields associated with the given
Experiment ID.  On failure, C<undef> is returned.

=back

#=cut

sub getExperiment {
    my ($self, $expid) = @_;

    my $row = undef;
    if ($self->runQuery('GET_EXPERIMENT', $expid)) {
        $row = $self->fetchRow('GET_EXPERIMENT');
    }
    $self->endQuery('GET_EXPERIMENT');

    return $row
}
=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< $exp_hash = $glk->getExperimentByComment($comment) >>

This function functions in just the same way as C<getExperiment($experiment_id)>
except it searches for the first Experimetn with the given comment.  This is
useful in assigning one particular pre-existing experiment to a library.

=back

#=cut

sub getExperimentByComment {
    my ($self, $comment) = @_;

    $self->_cleanValue(\$comment, 'getExperimentByComment()', '$comment');

    my $row = undef;

    if ($self->runQuery('GET_EXPERIMENT_BY_COMMENT', $comment)) {
        $row = $self->fetchRow('GET_EXPERIMENT_BY_COMMENT');
    }
    $self->endQuery('GET_EXPERIMENT_BY_COMMENT');

    return $row
}
=cut


=comment NO LONGER USED 2017-04-20


=over

=item B<< $leid = $glk->linkExperiment($library_id, $experiment_id) >>

This function links an Experiment with a Library, allowing statistics to be
attached to the linkage.  On success, the ID of the link is returned. On
failure, C<undef> is returned.

=back

#=cut

sub linkExperiment {
    my ($self, $libid, $expid) = @_;

    my $leid = getEUID();

    unless (defined($leid)) {
        return undef
    }
    my $added = undef;

    if ($self->runQuery('ADD_LIBRARY_EXPERIMENT', $leid, $libid, $expid)) {
        $added = $leid;
    }
    $self->endQuery('ADD_LIBRARY_EXPERIMENT');

    return $added
}
=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< $lib_exp_hash = $glk->getLibraryExperiment($lib_exp_id) >>

This function returns a hash containing named fields associated with the given
Library-Experiment ID.  On failure, C<undef> is returned.

=back

#=cut

sub getLibraryExperiment {
    my ($self, $leid) = @_;

    my $row = undef;

    if ($self->runQuery('GET_LIBRARY_EXPERIMENT', $leid)) {
        $row = $self->fetchRow('GET_LIBRARY_EXPERIMENT');
    }
    $self->endQuery('GET_LIBRARY_EXPERIMENT');

    return $row
}
=cut


sub getLibraryExperiments {
    my ($self, $libid) = @_;

    return $self->getAllLibraryExperiments($libid)
}

sub getAllLibraryExperiments {
    my ($self, $libid) = @_;

    my @setlist = ();
    if ($self->runQuery('GET_LIBRARY_EXPERIMENTS', $libid)) {
        while(my ($libexpid) = $self->fetchListRow('GET_LIBRARY_EXPERIMENTS')) {
            push(@setlist, $libexpid);
        }
    }
    $self->endQuery('GET_LIBRARY_EXPERIMENTS');

    return \@setlist;
}

# ##############################################################################
#
#    CLONING SYTEMS
#
# ##############################################################################

=back

=head2 CLONING SYTEMS

A CloningSystem is a handle for associating a Library with the primers and other
attributes used to create it.  Many Libraries can share a CloningSystem, but a
Library can only have one CloningSystem.

=over

=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< $info_hashref = $glk->getCloneSysInfo($srid) >>

This function returns a reference to a hash of information about the given
CloningSystem.  If the CloningSystem is not found, a reference to an empty hash
is returned. On success, the following hash values will exist:

    id        :  The CloningSystem ID
    name      :  The name of the CloningSystem
    desc      :  An optional description of the CloningSystem

=back

#=cut

sub getCloneSysInfo {
    my ($self, $csid) = @_;

    my %info = ();
    if ($self->runQuery('GET_CLONESYS_INFO', $csid)) {
        my $row = $self->fetchRow('GET_CLONESYS_INFO');

        %info = ('id' => $row->{'CloningSystem_id'},
                 'desc' => $row->{'description'},
                 'name' => $row->{'name'},
                );
    }
    $self->endQuery('GET_CLONESYS_INFO');

    return \%info
}
=cut

=over

=item B<< $csid = $glk->getCloneSysByName($cs_name) >>

This function returns the CloningSystem ID of the CloningSystem with the given
name.  If the name is not found, C<undef> is returned.

=back

=cut

sub getCloneSysByName {
    my ($self, $name) = @_;

    $self->_cleanValue(\$name, 'getCloneSysByName()', '$name');

    my $clonesys = undef;

    if ($self->runQuery('GET_CLONESYS_BY_NAME', $name)) {
        $clonesys = $self->fetchSingle('GET_CLONESYS_BY_NAME');
    }
    $self->endQuery('GET_CLONESYS_BY_NAME');

    return $clonesys
}

=over

=item B<< @csids = @{$glk->getTrimSeq_CloneSys($ts_id) >>

Given a TrimSequence_id, it returns a reference to an array containing all the CloningSystem_id associated with it.

=back

=cut

sub getTrimSeq_CloneSys {
    my ($self, $ts_id) = @_;
    $self->_cleanValue(\$ts_id, 'getTrimSeq_CloneSys()', '$ts_id');
    my @cs_ids = ();

    if ($self->runQuery('GET_TRIMSEQ_CLONESYS', $ts_id)) {
        while (my $info = $self->fetchRow('GET_TRIMSEQ_CLONESYS')) {
            push(@cs_ids, $info->{CloningSystem_id});
        }
    }
    $self->endQuery('GET_CLONESYS_BY_NAME');
    return \@cs_ids
}

=over

=item B<< $csid = $glk->addCloningSystem($name, [$desc]) >>

This function creates a new CloningSystem with the given data.  If not supplied,
the description is set to NULL.  On success, the CloningSystem ID of the new
CloningSystem is returned.  On error, C<undef> is returned.

=back

=cut

sub addCloningSystem {
    my ($self, $name, $desc) = @_;

    $self->_cleanValue(\$name, 'addCloningSystem()', '$name');
    $self->_cleanValue(\$desc, 'addCloningSystem()', '$desc');

    my $csid = getEUID();
    unless (defined($csid)) {
        return undef
    }
    my $added = undef;
    if ($self->runQuery('ADD_CLONESYS', $csid, $name, $desc)) {
        $added = $csid;
    }
    $self->endQuery('ADD_CLONESYS');

    return $added
}

=over

=item B<< $success = $glk->linkTrimSequence($tsid, $csid) >>

This function links a TrimSequence to a CloningSystem.  If the linking fails,
C<undef> is returned.

=back

=cut

sub linkTrimSequence {
    my ($self, $tsid, $csid) = @_;

    my $success = undef;

    # TODO: Check to see if the link already exists.

    if ($self->runQuery('LINK_TRIMSEQ_CLONESYS', $csid, $tsid))  {
        $success = 1;
    }
    $self->endQuery('LINK_TRIMSEQ_CLONESYS');

    return $success
}
=cut
# ##############################################################################
#
#    TRIM SEQUENCES
#
# ##############################################################################

=back

=head2 TRIM SEQUENCES

A TrimSequence is an object which represents a sequence of bases which can be
used to trim a SequenceRead (eg: primers or vectors)

=over

=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< $info_hashref = $glk->getTrimSequenceInfo($srid) >>

This function returns a reference to a hash of information about the given
TrimSequence.  If the TrinSequence is not found, a reference to an empty hash is
returned. On success, the following hash values will exist:

    id        :  The TrimSequence ID
    name      :  The name of the TrimSequence
    sequence  :  The string sequence of the TrimSequence
    direction :  The direction of the TrimSequence

=back

#=cut

sub getTrimSequenceInfo {
    my ($self, $tsid) = @_;

    my %info = ();

    if ($self->runQuery('GET_TRIMSEQ_INFO', $tsid)) {
        my $row = $self->fetchRow('GET_TRIMSEQ_INFO');

        %info = ('id' => $row->{'TrimSequence_id'},
                 'name' => $row->{'name'},
                 'sequence' => $row->{'sequence'},
                 'direction' => $row->{'direction'},
                );
    }
    $self->endQuery('GET_TRIMSEQ_INFO');

    return \%info
}
=cut

=over

=item B<< $tsid = $glk->getTrimSequencByName($trimseq_name) >>

This function returns a TrimSequence ID which matches the given name, or
C<undef> if the given name is not found.

=back

=cut

sub getTrimSequenceByName {
    my ($self, $name) = @_;

    $self->_cleanValue(\$name, 'getTrimSequenceByName()', '$name');
    my $tsid = undef;

    if ($self->runQuery('GET_TRIMSEQ_BY_NAME', $name)) {
        $tsid = $self->fetchSingle('GET_TRIMSEQ_BY_NAME');
    }
    $self->endQuery('GET_TRIMSEQ_BY_NAME');

    return $tsid
}

=comment NO LONGER USED 2017-04-20


=over

=item B<< $tsid_arrayref = $glk->getTrimSequencesForCloneSys($csid) >>

This function returns a reference to an array of TrimSequence IDs which belong
to the given CloningSystem.  If the CloningSystem does not exist, or contains
no TrimSequences, a reference to an empty array is returned.

=back

#=cut

sub getTrimSequencesForCloneSys {
    my ($self, $csid) = @_;

    my @tsids = ();
    if ($self->runQuery('GET_TRIMSEQ_BY_CLONESYS', $csid)) {
        while (my $tsid = $self->fetchSingle('GET_TRIMSEQ_BY_CLONESYS')) {
            push @tsids, $tsid;
        }
    }
    $self->endQuery('GET_TRIMSEQ_BY_CLONESYS');

    return \@tsids
}
=cut
=over

=item B<< $success = $glk->addTrimSequence($name, $sequence, $attr => $val...) >>

This function creates a new TrimSequence adding it and an arbitrarily long list
of attributes to the current database.  Attributes are added using the
C<addTrimSequenceAttribute()> function.  See that function's documentation for
details on how it works.  The function returns C<undef> on failure to create
the TrimSequence.  Failures in creating the Attributes are currently ignored.

=back

=cut

sub addTrimSequence {
    my ($self, $name, $direction, $sequence, %attrs) = @_;

    $self->_cleanValue(\$name,      'addTrimSequence()', '$name');
    $self->_cleanValue(\$direction, 'addTrimSequence()', '$direction');
    $self->_cleanValue(\$sequence,  'addTrimSequence()', '$sequence');
    my $tsid = getEUID();
    unless (defined($tsid)) {
        return undef
    }
    my $added = undef;
    if ($self->runQuery('ADD_TRIMSEQ', $tsid, $name, $direction, $sequence)) {
        $added = $tsid;

        foreach my $attr (keys %attrs) {
            $self->addTrimSequenceAttribute($tsid, $attr, $attrs{$attr});
        }
    }
    $self->endQuery('ADD_TRIMSEQ');

    return $added
}

#
#  ---------------- TrimSequence Attributes --------------------
#

=head3 TrimSequence Attributes

=over

=cut

=over

=item B<< $attr_hashref = $glk->getTrimSequenceAttributes($tsid) >>

This function returns a reference to a hash containing attribute/value pairs
of the Attributes of the given TrimSequence.  If the TrimSequence does not
exist or has no Attributes, a reference to an empty hash is returned.

=back

=cut

sub getTrimSequenceAttributes {
    my ($self, $tsid) = @_;

    # Pre-load types
    $self->preloadTrimSeqAttrTypes();

    my %attrs = ();
    if ($self->runQuery('GET_TRIMSEQ_ATTRS', $tsid)) {
        while (my ($attr_id, $val) = $self->fetchListRow('GET_TRIMSEQ_ATTRS')) {
            my $attr = $self->getTrimSeqAttrTypeName($attr_id);

            if (defined $attr) {
                $attrs{$attr} = $val;
            }
        }
    }
    # TODO: Return undef if the query fails
    $self->endQuery('GET_TRIMSEQ_ATTRS');

    return \%attrs
}


=over

=item B<< $success = $glk->addTrimSequenceAttribute($tsid, $type, $value) >>

This function adds an attribute of the supplied type to the given TrimSequence.
The type can be supplied as either an Attribute Type Name, or an Attribute Type
ID.  If the given type does not exist, a new type is created.  The function
returns C<undef> on failure to create the Attribute or Attribute Type.

=back

=cut

sub addTrimSequenceAttribute {
    my ($self, $tsid, $type, $value) = @_;

    $self->_cleanValue(\$type,  'addTrimSequenceAttribute()', '$type');
    $self->_cleanValue(\$value, 'addTrimSequenceAttribute()', '$value');

    my $typeid = $self->getTrimSeqAttrTypeID($type);
    unless (defined $typeid) {
        $self->addTrimSeqAttributeType($type);
        $typeid = $self->getTrimSeqAttrTypeID($type);
    }
    unless (defined $typeid) {
        return undef
    }

    my $success = undef;
    $self->logLocal("Adding TrimSequenceAttribute $type($typeid)='$value' to TrimSequence $tsid", 4);

    if ($self->runQuery('ADD_TRIMSEQ_ATTR', $tsid, $typeid, $value)) {
        $success = 1;
    }
    $self->endQuery('ADD_TRIMSEQ_ATTR');

    return $success
}

=back

=cut

# ##############################################################################
#
#    GLK TYPE CACHING
#
# ##############################################################################

=back

=head2 GLK TYPE CACHING

These functions provide cached storage and lookups of various GLK Types.  There
are significant speed improvements to caching these lists locally instead of
using queries with joins to type tables.

=over

=cut

#
#  ---------------- Cache Extent Types --------------------
#

=head3 Extent Types

=cut

=over

=item $glk->loadExtentTypes()

This will (re)load the current Extent Type cache.  This will be done
automatically if the cache does not exist.

=back

=cut

sub loadExtentTypes {
    my ($self) = @_;

    delete $self->{'extent_typeid_name'};
    delete $self->{'extent_typeid_id'};

    $self->runQuery('LOAD_EXTENT_TYPES');

    while(my $row = $self->fetchRow('LOAD_EXTENT_TYPES')) {
        $self->{'extent_type_name'}{$row->{'type'}} = $row->{'Extent_Type_id'};
        $self->{'extent_type_id'}{$row->{'Extent_Type_id'}} = $row->{'type'};
    }
    $self->endQuery('LOAD_EXTENT_TYPES');
}

=over

=item $glk->preloadExtentTypes()

This will ensure the current Extent Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

=cut

sub preloadExtentTypes {
    my ($self) = @_;

    if (exists $self->{'extent_typeid_name'} and exists $self->{'extent_typeid_id'}) {
        $self->loadExtentTypes();
    }
}

=over

=item B<< $type_name = $glk->getExtentTypeID($type_id) >>

This will translate a Extent Type ID to a Type Name.

=back

=cut

sub getExtentTypeID {
    my ($self, $type) = @_;

    unless (defined($type)) {
        $self->logError("getExtentTypeID() - called without an argument.", 1);
        return undef
    }
    $self->_cleanValue(\$type, 'getExtentTypeID()', '$type');

    unless (exists($self->{extent_type_name}) && defined($self->{extent_type_name})) {
        $self->loadExtentTypes();
    }
    my $type_id =  $self->{extent_type_name}{$type};

    unless (defined($type_id)) {
        $self->logWarn("getExtentTypeID() - Invalid Extent_Type (\"$type\")");
    }
    return $type_id
}

=over

=item B<< $type_id = $glk->getExtentTypeID($type_name) >>

This will translate a Extent Type Name to a Type ID.

=back

=cut

sub getExtentTypeName {
    my ($self, $typeid) = @_;

    unless (defined($typeid)) {
        $self->logError("getExtentTypeName() - called without a defined argument.", 1);
        return undef
    }
    unless (exists($self->{extent_type_id}) && defined($self->{extent_type_id})) {
        $self->loadExtentTypes();
    }
    my $type = $self->{'extent_type_id'}{$typeid};

    unless (defined($type)) {
        $self->logWarn("getExtentTypeName() - invalid Extent_Type_id ($typeid).");
    }
    return $type
}


#
#  ---------------- Cache Extent Attribute Types --------------------
#

=head3 Extent Attribute Types

=over

=item $glk->loadExtentAttrTypes()

This will (re)load the current Extent Attribute Type cache.  This will be done
automatically if the cache does not exist.

=back

=cut

sub loadExtentAttrTypes {
    my ($self) = @_;

    ## Throwing away the existing cache
    undef($self->{extent_attrtype_name});
    undef($self->{extent_attrtype_id});
    undef($self->{ExtentAttribute_id_ValueType});
    undef($self->{ExtentAttribute_type_ValueType});


    $self->runQuery('LOAD_EXTENT_ATTR_TYPES');

    while(my $row = $self->fetchRow('LOAD_EXTENT_ATTR_TYPES')) {
        $self->{extent_attrtype_name}{$row->{type}}                           = $row->{ExtentAttributeType_id};
        $self->{extent_attrtype_id}{$row->{ExtentAttributeType_id}}           = $row->{type};
        $self->{ExtentAttribute_id_ValueType}{$row->{ExtentAttributeType_id}} = $row->{value_type};
        $self->{ExtentAttribute_type_ValueType}{$row->{type}}                 = $row->{value_type};
        $self->{ExtentAttribute_type_CombiningRule}{$row->{type}}             = $row->{combining_rule};
        $self->{ExtentAttribute_id_Descr}{$row->{ExtentAttributeType_id}}     = $row->{description};
    }
    $self->endQuery('LOAD_EXTENT_ATTR_TYPES');
}

#
#  ---------------- Cache Extent Attribute Value Types --------------------
#

=head3 Extent Attribute Value Types

=over

=cut

=over

=item $glk->getExtentAttrValueType($atrr)

Given an ExtentAttributeType or its corresponding ID it returns the type of value expected for that given attribute.
It assumes that the value passed does not contain unnecessary spaces.
In the case the ID or attribute type is invalid, it will issue a warning and return undef();


=back

=cut

sub getExtentAttrValueType {
    my ($self, $attr) = @_;
    my $val_type;

    unless (defined($attr) && $attr =~ /^\S+$/) {
        no warnings;
        $self->bail("getExtentAttrValueType() - Called with undefined, empty, or otherwise invalid attribute (\"$attr\").");
    }

    unless (defined($self->{ExtentAttribute_id_ValueType})) {
        $self->loadExtentAttrTypes();
    }
    if ($attr =~ /^\d+$/) { ## The value passed is supposedly an ExtentAttributeType_id
        $val_type = $self->{ExtentAttribute_id_ValueType}{$attr};
    }
    else {
        $val_type = $self->{ExtentAttribute_type_ValueType}{$attr}
    }
    unless (defined($val_type)) {
        $self->logWarn("getExtentAttrValueType() - Called with an invalid ExtentAttributeType: \"$attr\"");
        return undef
    }
    return $val_type
}

=over

=item $glk->preloadExtentAttrTypes()

This will ensure that the Extent Attribute Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

=cut

sub preloadExtentAttrTypes {
    my ($self) = @_;

    unless(exists $self->{'extent_attrtype_name'} and exists $self->{'extent_attrtype_id'}) {
        $self->loadExtentTypes();
    }
}

=over

=item B<< $type_id = $glk->getExtentAttrTypeID($type_name) >>

This will translate a Extent Attribute Type Name to a Type ID.

=back

=cut

sub getExtentAttrTypeID {
    my ($self, $type) = @_;

    unless (defined($type)) {
        return undef
    }
    $self->_cleanValue(\$type, 'getExtentAttrTypeID()', '$type');

    unless (defined($self->{'extent_attrtype_name'})) {
        $self->loadExtentAttrTypes();
    }

    my $type_id = $self->{'extent_attrtype_name'}{$type};

    unless (defined($type_id)) {
        $self->logWarn("getExtentAttrTypeID() - Invalid ExtentAttributeType \"$type\"");
    }
    return $type_id
}
=over

=item B<< $type_name = $glk->getExtentAttrTypeName($type_id) >>

This will translate a Extent Attribute Type ID to a Type Name.

=back

=cut

sub getExtentAttrTypeName {
    my ($self, $type_id) = @_;

    unless (defined($type_id)) {
        return undef
    }
    unless (exists($self->{extent_attrtype_id}) && defined($self->{extent_attrtype_id})) {
        $self->loadExtentAttrTypes();
    }
    my $type = $self->{extent_attrtype_id}{$type_id};

    unless (defined($type)) {
        $self->logWarn("getExtentAttrTypeName() - Invalid ExtentAttributeType_id $type_id");
    }
    return $type
}

=over

=item B<< $glk->getExtentAttrTypeCombiningRule($extent_attr_type) >>

Given an ExtentAttributeType or an ExtentAttributeType_id, it returns the combining_rule associated with it.

=back

=cut

sub getExtentAttrTypeCombiningRule {
    my ($self, $eat) = @_;
    my ($eat_id, $ea_type);

    if (!defined($eat) || $eat =~ /^\s*$/) {
        $self->bail("getExtentAttrTypeCombiningRule() - Called with empty or undefined ExtentAttributeType(_id)");
    }
    elsif ($eat =~ /^\d+$/) {
        $eat_id = $eat;
        $ea_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ea_type)) {
            $self->bail("getExtentAttrTypeCombiningRule() - Called with an invalid ExtentAttributeType_id (\"$eat\").");
        }
    }
    else {
        $ea_type = $eat;
        $eat_id = $self->getExtentAttrTypeID($ea_type);

        unless (defined($eat_id)) {
            $self->bail("getExtentAttrTypeCombiningRule() - Called with an invalid ExtentAttributeType (\"$eat\").");
        }
    }
    if (defined($self->{ExtentAttribute_type_CombiningRule}{$ea_type})) {
        return $self->{ExtentAttribute_type_CombiningRule}{$ea_type}
    }
    else {
        $self->bail("getExtentAttrTypeCombiningRule() - Unexpected error: Undefined value for ExtentAttributeType.combining_rule.");
    }
}

=over

=item B<< $success = $glk->changeExtentAttrType($eid, $current_type, $new_type) >>

This function updates an ExtentAttribute record changing the ExtentAttributeType_id to refer to a different attribute type.
It is intended to be used when a given attribute type is declared obsolete and is getting replaced by one or more better terms.
It takes the Extent_id, the type (or type ID) of both the current and the new Extent attribute type, and return a non-zero value if the change was successful, 0 otherwise.

=back

=cut

sub changeExtentAttrType {
    my ($self, $eid, $old_attype, $new_attype) = @_;

    ## Checking the parameters...

    if (!defined($eid) || $eid !~ /^\d+$/) {
        my $eid_string = defined ($eid) ? "\"$eid\"" : "undef";
        $self->bail("changeExtentAttrType() - Called with undefined or invalid Extent_id ($eid_string)");
    }
    elsif (!defined($old_attype)) {
        $self->bail("changeExtentAttrType() - Undefined value for the current ExtentAttributeType.");
    }
    elsif (!defined($new_attype)) {
        $self->bail("changeExtentAttrType() - Undefined value for the new ExtentAttributeType.");
    }
    ## Removing the extent from the consistency check cached results
    undef($self->{ExtAttrTroubles}{$eid});
    undef($self->{AttrTypeChecked}{$eid});
    delete($self->{ExtAttrTroubles}{$eid});
    delete($self->{AttrTypeChecked}{$eid});

    $self->_cleanValue(\$old_attype, 'changeExtentAttrType()', '$old_attype');
    $self->_cleanValue(\$new_attype, 'changeExtentAttrType()', '$new_attype');

    ## Getting the attribute type IDs for both the old and the new attribute types

    my ($old_type, $old_typeid, $new_typeid);

    foreach my $stuff ([$old_attype, \$old_typeid], [$new_attype, \$new_typeid]) {
        my ($attype, $r_id) = @{$stuff};

        if ($attype =~ /\D/) {
            ${$r_id} = $self->getExtentAttrTypeID($attype);

            if ($attype eq $old_attype) {
                $old_type = $attype;
            }
        }
        else { # Checking if it is a valid type ID by retrieving the alledged attribute type
            my $att_name = $self->getExtentAttrTypeName($attype);
            unless (defined($att_name)) {
                $self->logWarn("changeExtentAttrType() - Invalid ExtentAttributeType_id ($attype).");
                return undef
            }
            ${$r_id} = defined($att_name) ? $attype : undef;

            if ($attype eq $old_attype) {
                $old_type = $att_name;
            }
        }
        unless (defined(${$r_id})) {
            $self->bail("changeExtentAttrType() - Attribute type \"$attype\" does not exists");
        }
    }
    ## Checking if the Extent has indeed the old attribute
    my $value;

    if ($self->hasExtentAttribute($eid, $old_type)) {
        $value = $self->getExtentAtrribute($eid, $old_type);
    }
    else {
        $self->logWarn("changeExtentAttrType() - Extent $eid does not have attribute \"$old_attype\".");
        return FAILURE
    }
    ## Checking if the value is of a type compatible with the new attribute type

    unless ($self->{AttrTypeChecked}{$eid}{$new_attype} = $self->isCorrectValueType($new_attype, $value, undef, $eid)) {
        $self->logWarn("changeExtentAttrType(), Extent $eid - The value (\"$value\" now associated with attribute type \"$old_attype\" is not compatible with the new type \"$new_attype\" - Aborting the reassigning to the new type.");
        return FAILURE
    }

    ## Now actually renaming the attribute

    if ($self->runQuery('CHANGE_EXTENT_ATTR_TYPE', $new_typeid, $eid, $old_typeid)) {
        $self->endQuery('CHANGE_EXTENT_ATTR_TYPE');
        return SUCCESS
    }
    else {
        return FAILURE
    }
}

=over

=item B<< $yes_no = $glk->checkExtentAttTypeName($type_name) >>

Given a name for an ExtentAttributeType, it returns 1 if it is a valid attribute, 0 otherwise.

=back

=cut

sub checkExtentAttTypeName {
    my ($self, $attype_name) = @_;

    $self->_cleanValue(\$attype_name, 'checkExtentAttTypeName()', '$attype_name');

    unless (exists($self->{extent_attrtype_name}) && defined($self->{extent_attrtype_name})) {
        $self->loadExtentAttrTypes();
    }
    if (! defined($attype_name) || $attype_name !~ /\S/) {
        $self->bail("checkExtentAttTypeName() Called without required parameter.");
    }
    elsif ($attype_name !~ /\D/) {
        $self->bail("checkExtentAttTypeName() Probably called with the ExtentAttributeType_id instead of type.");
    }
    else {
        return exists($self->{extent_attrtype_name}{$attype_name}) && defined($self->{extent_attrtype_name}{$attype_name}) ? 1 : 0
    }
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $yes_no = $glk->checkExtentAttTypeID($type_id) >>

Given an ExtentAttributeType ID, it returns 1 if it is a valid ID, 0 otherwise.

=back

#=cut

sub checkExtentAttTypeID {
    my ($self, $attype_id) = @_;

    unless (exists($self->{extent_attrtype_id}) && defined($self->{extent_attrtype_id})) {
        $self->loadExtentAttrTypes();
    }
    if (! defined($attype_id) || $attype_id !~ /\S/) {
        $self->bail("checkExtentAttTypeID() - Called without required parameter.");
    }
    elsif ($attype_id !~ /^\d+$/) {
        $self->bail("checkExtentAttTypeID() - Probably called with the name of the ExtentAttributeType instead of ID.");
    }
    else {
        return exists($self->{extent_attrtype_id}{$attype_id}) && defined($self->{extent_attrtype_id}{$attype_id}) ? 1 : 0
    }
}
=cut

#
#  ---------------- Cache SeqRead Types --------------------
#

=head3 Sequence Read Types

=over

=cut

=over

=item $glk->loadSeqReadTypes()

This will (re)load the current Sequence Read Type cache.  This will be done
automatically if the cache does not exist.

=back

=cut

sub loadSeqReadTypes {
    my ($self) = @_;

    delete $self->{'seqread_typeid_name'};
    delete $self->{'seqread_typeid_id'};

    $self->runQuery('LOAD_SEQREAD_TYPES');

    while(my $row = $self->fetchRow('LOAD_SEQREAD_TYPES')) {
        $self->{'seqread_type_name'}{$row->{'type'}} = $row->{'SequenceReadType_id'};
        $self->{'seqread_type_id'}{$row->{'SequenceReadType_id'}} = $row->{'type'};
    }
    $self->endQuery('LOAD_SEQREAD_TYPES');
}

=over

=item $glk->preloadSeqReadTypes()

This will ensure the current Sequence Read Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

=cut

sub preloadSeqReadTypes {
    my ($self) = @_;

    unless (exists $self->{'seqread_typeid_name'} and exists $self->{'seqread_typeid_id'}) {
        $self->loadSeqReadTypes();
    }
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $type_name = $glk->getSeqReadTypeID($type_id) >>

This will translate a Sequence Read Type ID to a Type Name.

=back

#=cut

sub getSeqReadTypeID {
    my ($self, $type) = @_;

    $self->_cleanValue(\$type, 'getSeqReadTypeID()', '$type');

    unless (defined($type)) {
        return undef
    }
    unless (exists($self->{'seqread_type_name'})) {
        $self->loadSeqReadTypes();
    }
    return $self->{'seqread_type_name'}{$type}
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $type_id = $glk->getSeqReadTypeID($type_name) >>

This will translate a Sequence Read Type Name to a Type ID.

=back

#=cut

sub getSeqReadTypeName {
    my ($self, $typeid) = @_;

    unless (defined($typeid)) {
        return undef
    }
    unless (exists($self->{'seqread_type_id'})) {
        $self->loadSeqReadTypes();
    }
    return $self->{'seqread_type_id'}{$typeid}
}

=cut

#
#  ---------------- Cache SeqReadAttr Types --------------------
#

=head3 Sequence Read Attribute Types

=comment NO LONGER USED 2017-04-20

=over

=item $glk->loadSeqReadAttrTypes()

This will (re)load the current Sequence Read Attribute Type cache.  This will be done
automatically if the cache does not exist.

=back

#=cut

sub loadSeqReadAttrTypes {
    my ($self) = @_;

    delete $self->{'seqreadattr_typeid_name'};
    delete $self->{'seqreadattr_typeid_id'};

    $self->runQuery('LOAD_SEQREADATTR_TYPES');

    while(my $row = $self->fetchRow('LOAD_SEQREADATTR_TYPES')) {
        $self->{'seqreadattr_type_name'}{$row->{'type'}} = $row->{'SequenceReadAttributeType_id'};
        $self->{'seqreadattr_type_id'}{$row->{'SequenceReadAttributeType_id'}} = $row->{'type'};
    }
    $self->endQuery('LOAD_SEQREADATTR_TYPES');
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item $glk->preloadSeqReadAttrTypes()

This will ensure the current Sequence Read Attribute Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

#=cut

sub preloadSeqReadAttrTypes {
    my ($self) = @_;

    unless (exists $self->{'seqreadattr_typeid_name'} and exists $self->{'seqreadattr_typeid_id'}) {
        $self->loadSeqReadAttrTypes();
    }
}
=cut


=comment NO LONGER USED 2017-04-20

=over

=item B<< $type_id = $glk->getSequenceReadAttrTypeID($type_name) >>

This will translate a Sequence Read Attribute Type Name to a Type ID.

=back

#=cut

sub getSequenceReadAttrTypeID {
    my ($self, $type) = @_;

    unless (defined($type)) {
        return undef
    }
    $self->_cleanValue(\$type, 'getSequenceReadAttrTypeID()', '$type');

    unless (exists($self->{'seqreadattr_type_name'})) {
        $self->loadSeqReadAttrTypes();
    }
    return $self->{'seqreadattr_type_name'}{$type}
}
=cut

=comment NO LONGER USED 2017-04-20


sub getSeqReadAttrTypeID {
    my ($self, $type) = @_;

    return $self->getSequenceReadAttrTypeID($type)
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $type_name = $glk->getSequenceReadAttrTypeName($type_id) >>

This will translate a Sequence Read Attribute Type ID to a Type Name.

=back

#=cut

sub getSequenceReadAttrTypeName {
    my ($self, $typeid) = @_;

    unless (defined($typeid)) {
        return undef
    }
    unless (exists $self->{'seqreadattr_type_id'}) {
        $self->loadSeqReadAttrTypes();
    }
    return $self->{'seqreadattr_type_id'}{$typeid}
}
=cut

=comment NO LONGER USED 2017-04-20


sub getSeqReadAttrTypeName {
    my ($self, $typeid) = @_;

    return $self->getSequenceReadAttrTypeName($typeid)
}
=cut
#
#  ---------------- Cache Library Stat Types --------------------
#

=head3 Library Stat Types

=over

=cut

=comment NO LONGER USED 2017-04-20

=over

=item $glk->loadLibraryStatTypes()

This will (re)load the current Library Stat Type cache.  This will be done
automatically if the cache does not exist.

=back

#=cut

sub loadLibraryStatTypes {
    my ($self) = @_;

    delete $self->{'libstats_typeid_name'};
    delete $self->{'libstats_typeid_id'};

    $self->runQuery('LOAD_LIB_STAT_TYPES');

    while(my $row = $self->fetchRow('LOAD_LIB_STAT_TYPES')) {
        $self->{'libstats_typeid_name'}{$row->{'name'}} = $row->{'id'};
        $self->{'libstats_typeid_id'}{$row->{'id'}} = $row->{'name'};
    }
    $self->endQuery('LOAD_LIB_STAT_TYPES');
}
=cut

=over

=comment NO LONGER USED 2017-04-20


=item $glk->preloadLibraryStatTypes()

This will ensure the current Library Stat Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

#=cut

sub preloadLibraryStatTypes {
    my ($self) = @_;

    unless( exists $self->{'libstats_typeid_name'} and exists $self->{'libstats_typeid_id'}) {
        $self->loadLibraryStatTypes();
    }
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $stat_name = $glk->getLibStatTypeID($stat_id) >>

This will translate a Library Stat ID to a Stat Name.

=back

#=cut

sub getLibraryStatID {
    my ($self, $type) = @_;

    unless (defined($type)) {
        return undef
    }
    $self->_cleanValue(\$type, 'getLibraryStatID()', '$type');

    unless (exists($self->{'libstats_typeid_name'})) {
        $self->loadLibraryStatTypes();
    }
    return $self->{'libstats_typeid_name'}{$type}
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $stat_id = $glk->getLibraryStatTypeName($stat_name) >>

This will translate a Library Stat Name to a Stat ID.

=back

#=cut

sub getLibraryStatName {
    my ($self, $typeid) = @_;

    unless(defined($typeid)) {
        return undef
    }
    unless (exists($self->{'libstats_typeid_id'})) {
        $self->loadLibraryStatTypes();
    }
    return $self->{'libstats_typeid_id'}{$typeid}
}
=cut

=over

=item B<< $locus_tag_pfix = $glk->getSampleLocusTagPrefix($eid) >>

Given a sample-level Extent_id, it returns the locus_tag prefix associated with it.
Exceptions are thrown if more than one (fatal) or zero (just error) locus_tag prefix are found.
This tool will query vir_common..BioProject table

=back

=cut

sub getSampleLocusTagPrefix {
    my ($self, $eid) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        no warnings;
        $self->bail("getSampleLocusTagPrefix() - Called with empty, undefined, or invalid Extent_id (\"$eid\")");
    }
    my $r_info = $self->getExtentInfo($eid);

    unless (defined($r_info->{type})) {
        $self->bail("getSampleLocusTagPrefix() - Unable to find any information about Extent $eid in the current database (" . $self->getDbName() .").");
    }
    if ($r_info->{type} ne 'SAMPLE') {
        $self->bail("getSampleLocusTagPrefix() - Extent $eid in is of type \"$r_info->{type}\") and not of type \"SAMPLE\" as expected.");
    }
    my $r_pjs = $self->getBioprojectsList($eid);

    unless (scalar(@{$r_pjs})) {
        $self->bail("getSampleLocusTagPrefix() - Unable to find any bioproject_id associated with Extent $eid.");
    }
    my @pfixs = ();

    foreach my $pj_code (@{$r_pjs}) {
        my $pfix = $self->getBioProjectLocusTagPfix($pj_code);

        if (defined($pfix) && $pfix =~ /\S/) {
            push(@pfixs, $pfix);
        }
    }
    if (scalar(@pfixs) == 0) {
        $self->error("getSampleLocusTagPrefix() - Unable to find any locus_tag prefix for sample $r_info->{'ref'}.");
        return undef
    }
    elsif (scalar(@pfixs) > 1) {
        $self->bail("getSampleLocusTagPrefix() - Found too many locus_tag prefixes for sample $r_info->{'ref'}: \"" . join('", "', @pfixs) . '"');
    }
    return $pfixs[0]
}

=back

=cut

#
#  ---------------- Cache TrimSequence Attribute Types --------------------
#

=head3 TrimSequence Attribute Types

=over

=cut

=over

=item $glk->loadTrimSeqAttrTypes()

This will (re)load the current Trim Sequence Attribute Type cache.  This will be done
automatically if the cache does not exist.

=back

=cut

sub loadTrimSeqAttrTypes {
    my ($self) = @_;

    delete $self->{'trimseq_attrtype_name'};
    delete $self->{'trimseq_attrtype_id'};

    my $count = 0;

    if ($self->runQuery('LOAD_TRIMSEQ_ATTR_TYPES')) {
        while (my $row = $self->fetchRow('LOAD_TRIMSEQ_ATTR_TYPES')) {
            $self->{'trimseq_attrtype_name'}{$row->{'name'}} = $row->{'TrimSequenceAttributeType_id'};
            $self->{'trimseq_attrtype_id'}{$row->{'TrimSequenceAttributeType_id'}} = $row->{'name'};
        }
    }
    $self->endQuery('LOAD_TRIMSEQ_ATTR_TYPES');
}

=over

=item $glk->preloadTrimSeqAttrTypes()

This will ensure the current Trim Sequence Attribute Type cache is loaded.  Repeated calls to this
will not cause the cache to be emptied and re-filled.

=back

=cut

sub preloadTrimSeqAttrTypes {
    my ($self) = @_;

    if (exists $self->{'trimseq_attrtype_name'} and exists $self->{'trimseq_attrtype_id'}) {
        $self->loadTrimSeqAttrTypes()
    }
}

=over

=item B<< $type_id = $glk->getTrimSeqAttrTypeID($type_name) >>

This will translate a Trim Sequence Attribute Type name to a Type ID.

=back

=cut

sub getTrimSeqAttrTypeID {
    my ($self, $type) = @_;

    unless (defined($type)) {
        return undef
    }
    $self->_cleanValue(\$type, 'getTrimSeqAttrTypeID()', '$type');

    unless (exists($self->{trimseq_attrtype_name})) {
        $self->loadTrimSeqAttrTypes();
    }
    return $self->{trimseq_attrtype_name}{$type}
}

=over

=item B<< $type_name = $glk->getTrimSeqAttrTypeName($type_id) >>

This will translate a Trim Sequence Attribute Type ID to a Type Name.

=back

=cut

sub getTrimSeqAttrTypeName {
    my ($self, $typeid) = @_;

    unless (defined($typeid)) {
        return undef
    }
    unless (exists($self->{'trimseq_attrtype_id'})) {
        $self->loadTrimSeqAttrTypes();
    }
    return $self->{'trimseq_attrtype_id'}{$typeid}
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $info_hashref = $glk->getPlateInfo($plateId) >>

This function returns a reference to a hash of information about the given
Plate.  If the plate is not found, a reference to an empty hash is returned.
On success, the following hash values will exist:

    id      :  The Plate ID
    name     :  The plate name (ex: T48)
    created  :  datetime of plate creation
    desc    :  The Plate description

=back

#=cut

sub getPlateInfo {
    my ($self, $eid) = @_;

    my %info = ();

    if ($self->runQuery('GET_PLATE_INFO', $eid)) {
        my $row = $self->fetchRow('GET_PLATE_INFO');

        %info = ('id' => $row->{'id'},
                 'name' => $row->{'name'},
                 'created' => $row->{'created'},
                 'desc' => $row->{'description'},
                );
    }
    $self->endQuery('GET_PLATE_INFO');

    return \%info
}
=cut


=comment NO LONGER USED 2017-04-20

=over

=item B<< $srid = $glk->getPlateByName($plateName) >>

This function will return a Plate ID for the supplied plate name.  If
the plate name does not exist, then C<undef> is returned.

=back

#=cut

sub getPlateByName {
    my ($self, $plateName) = @_;

    $self->_cleanValue(\$plateName, 'getPlateByName()', '$plateName');
    my $srid = undef;

    if ($self->runQuery('GET_PLATE_ID_BY_NAME', $plateName)) {
        $srid = $self->fetchSingle('GET_PLATE_ID_BY_NAME');
    }
    $self->endQuery('GET_PLATE_ID_BY_NAME');

    return $srid
}
=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< @plates_ids = $glk->getAllWellsFromPlate($plateId) >>

Gets a list of all wellIds (numeric 10) for all the wells in the given plate.

=back

#=cut
sub getAllWellsFromPlate {
    my ($self, $plateId) = @_;
    my @wells = ();

    if ($self->runQuery('GET_ALL_WELLS_BY_PLATE', $plateId)) {
        while (my ($well) = $self->fetchListRow('GET_ALL_WELLS_BY_PLATE')) {
            push @wells, $well;
        }
    }
    $self->endQuery('GET_ALL_WELLS_BY_PLATE');

    return \@wells
}
=cut


=comment NO LONGER USED 2017-04-20

=over

=item B<< $info_hashref = $glk->getWellInfo($wellId) >>

This function returns a reference to a hash of information about the given
Well.  If the well is not found, a reference to an empty hash is returned.
On success, the following hash values will exist:

    id                :  The Well ID
    Plate_id          :  The id of the plate that this well belongs
    row               :  row offset of this well in the plate (0-7)
    col               :  col offset of this well in the plate (0-11)
    name              :  The well name (ex: A04)
    CloningSystem_id  :  The CloningSystem id this well uses
    external_id       :  The External Id of this well which may be used by another system.

=back

#=cut

sub getWellInfo {
    my ($self, $eid) = @_;

    my %info = ();

    if ($self->runQuery('GET_WELL_INFO', $eid)) {
        my $row = $self->fetchRow('GET_WELL_INFO');
        %info = ('id' => $row->{'id'},
                 'Plate_id' => $row->{'Plate_id'},
                 'row' => $row->{'row'},
                 'col' => $row->{'col'},
                 'external_id' => $row->{'external_id'},
                 'CloningSystem_id' => $row->{'CloningSystem_id'},
                 'name' => $self->convertToWellName($row->{'row'},$row->{'col'}),
                );
    }
    $self->endQuery('GET_WELL_INFO');

    return \%info
}
=cut

=comment NO LONGER USED 2017-04-20


=over

=item B<< @plates_ids = $glk->convertToWellName($row,$col) >>
converts 0-based row and col well offsets into the well name (EX: 0,4 -> A05)

=back

sub convertToWellName($$$){
    my ($self, $row, $col) = @_;
    return sprintf("%s%02d", chr(65+$row),$col+1);
}
=cut

# ##############################################################################
#
#    PUBLIC LOW-LEVEL DATABASE API
#
# ##############################################################################

=back

=head2 PUBLIC LOW-LEVEL DATABASE API

These routines are used by GLKLib to interact with the database.  In most
situations clients will not have to use them at all.  However, they can
be used to take advantage of GLKLib's on-demand compilation, Statement handle
caching, and data grouping functions for user supplied queries.

=over

=cut

=over

=item $glk->addSQL($query_name, $sql_template, $arg...)

Adds a named query to GLKLib's list of compileable statements. The SQL template
is a string in the form used by C<sprintf()>.  The arbitrary list of arguments
is used to fill in the template.  This function does not compile the statement.
The SQL will only be compiled when it is needed, and it will only be compiled
once.

=back

=cut

sub addSQL {
    my ($self, $query_name, $sql_template, @args) = @_;

    $self->{'query_lookup'}{$query_name} = sprintf($sql_template, @args);
}

=over

=item B<< $success = $glk->runQuery($query_name, $arg...) >>

This function executes a query against the database, using the arguments
supplied.  The query is fetched from the query cache by the given name.  The
query result is available for fetching via the normal fetch operations
(C<fetchSingle()>, C<fetchRow()>, C<fetchListRow()>) using the same name.  On
error, C<undef> is returned.  The success or failure of this function, along
with any status messages, can be fetched by C<getResult()> and its related
functions.

NOTE: In order to keep access to the database clean and efficient.  All queries
executed with C<runQuery()> should be ended with C<endQuery()> as soon as
possible.

=back

=cut

sub runQuery {
    my ($self, $query_name, @args) = @_;

    my $query = $self->getQueryObject($query_name);

    unless (defined($query)) {
        return $self->result(0, "runQuery() - Query '$query_name' was not found.")
    }
    eval {$query->execute(@args)};

    if ($@) {
        my $db_name = $self->getDbName();
        $self->logWarn("runQuery() - Problems with query: \"$query_name\" (database \"$db_name\"):\n\"$@\"");
        return $self->result(0, "runQuery() - Database: $db_name - Error while running '$query_name': \n     %s", $self->{'db'}->errstr)
    }

    return $self->result(1, "runQuery() - Query '$query_name' executed.")
}

=over

=item B<< $success = $glk->endQuery($query_name) >>

This function cleans up an executed query, reclaiming memory and resources and
allowing the query to be called again in a clean manner.  This function should
be called after completing interactions with any query executed by
C<runQuery()>.

=back

=cut

sub endQuery {
    my ($self, $query_name) = @_;

    my $query = $self->{'query'}{$query_name};

    unless (defined $query) {
        return $self->result(0, "Query '$query_name' was not found.")
    }

    unless ($query->finish()) {
        $self->logWarn("endQuery() - Unable to issue the finish() instruction to query \"$query_name\".");
        return $self->result(0, "Erorr while ending '$query_name': \n     %s", $self->{'handle'}->errstr)
    }
    return $self->result(1, "Query '$query_name' finished.")
}

=over

=item B<< ($success, $value) = $glk->runSimple($query_name, $arg...) >>

This function executes a query which returns a single result.  This is
equivalent to running the query with runQuery()/fetchSingle()/endQuery().
The one difference is that the return value is a success/value pair.  On error,
$success and $value will be C<undef>.  If the query succeeds, $success will
evaluate true and $value will be set to the value of the first column of the
first result row of the query.

=back

=cut

sub runSimple {
    my ($self, $query, @args) = @_;

    my $success = undef;
    my $result = undef;

    if ($self->runQuery($query, @args)) {
        $result = $self->fetchSingle($query);
        $success = 1;
    }
    $self->endQuery($query);

    return ($success, $result)
}

=comment NO LONGER USED 2017-04-20

=over

=item $glk->closeDB()

This function will close the database connection and clean up resources.

=back

#=cut

sub closeDB {
    my $self = shift();
    $self->disconnect();
}
=cut

=head3 Query Result Fetching

These are a set of helper functions to retrieve neat sets of data from the
result sets of query executions.

=over

=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $row_hashref = $glk->getIterator($query_name) >>

This function returns an iterator of results for any given query.  Its designed
to minimize database communication from any client.

=back

#=cut

sub getRowIterator {
    my $self = shift;comment NO LONGER USED
    my $query_name = shift;
    # Avoid a perl method call per row
    my $max_rows = shift || 50000;

    my $query = $self->getQueryObject($query_name);

    unless (defined($query)) {
        return undef
    }
    my $row_cache = [];

    return sub {
            $row_cache = $query->fetchall_arrayref(undef, $max_rows) unless scalar @$row_cache;
            return shift @$row_cache
    }
}
=cut

=over

=item B<< $data = $glk->fetchSingle($query_name) >>

This function returns a scalar value equal to the value of the first column in
the first row of the result set of the given query.  This is designed to make
it simple to fetch queries written to fetch a single piece of information.

=back

=cut

sub fetchSingle {
    my ($self, $query_name) = @_;

    my $query = $self->getQueryObject($query_name);

    unless (defined($query)) {
        return undef
    }
    my ($value) = $query->fetchrow_array();
    return $value
}

=over

=item B<< $row_hashref = $glk->fetchRow($query_name) >>

This function returns a row of data from a named query as a reference to a
hash.  The keys of the hash are set to the column names of the result set of
the query.  If the query does not exist or has not been executed, C<undef>
is returned.

=back

=cut

sub fetchRow {
    my ($self, $query_name) = @_;

    my $query = $self->getQueryObject($query_name);

    unless (defined $query) {
        return undef
    }
    return $query->fetchrow_hashref()
}

=over

=item B<< @row = $glk->fetchListRow($query_name) >>

This function fetches a row of data from a named query in list or array context.
The returned array is ordered to match the columns in the result set of the
query.  If the query does not exist or has not been executed, C<undef>
is returned.

=back

=cut

sub fetchListRow {
    my ($self, $query_name) = @_;

    my $query = $self->getQueryObject($query_name);

    unless (defined $query) {
        return undef
    }
    return $query->fetchrow_array()
}

=back

=head3 Database Transaction Functions

These functions allow clients simple use of transactions while interacting with
the database.

=over

=cut

=over

=item B<< @row = $glk->fetchAllArrayRef($query_name) >>

This function fetches a reference to an array that contains one array reference
per row of data from a named query. For reach row, the columns are maintained
in the same order as the named query.

=back

=cut

sub fetchAllArrayRef {
    my ($self, $query_name) = @_;
    my $query = $self->getQueryObject($query_name);

    unless (defined $query) {
        return undef
    }
    return $query->fetchall_arrayref()
}

=back

=head3 Database Transaction Functions

These functions allow clients simple use of transactions while interacting with
the database.

=over

=cut

=over


=item $glk->startTransaction()

This function initiates a transaction on the current database.  Be sure to call
C<finishTransaction> when finished or C<abortTransaction> if you wish to roll
back the transaction.

=back

=cut

sub startTransaction {
    my ($self) = @_;

    $self->{'db'}->{'RaiseError'} = 1;
    $self->{'db'}->begin_work();
}

=over

=item $glk->abortTransaction()

This function ends a transaction, rolling back any changes.

=back

=cut

sub abortTransaction {
    my ($self) = @_;

    my $rc = $self->{'db'}->rollback();
    $self->{'db'}->{'RaiseError'} = 0;

    return $rc
}

=over

=item $glk->finishTransaction()

This function ends a transaction, committing any changes permanently.

=back

=cut

sub finishTransaction {
    my ($self) = @_;

    $self->{'db'}->commit();
    $self->{'db'}->{'RaiseError'} = 0;
}

# ##############################################################################
#
#    ERROR AND STATUS CODE CHECKING
#
# ##############################################################################

=back

=head2 ERROR AND STATUS CODE CHECKING

These functions allow you to check the results and errors from the lower
level database functions.  If a function fails, you can use these functions
to try and discover just what caused the failure.

=over

=cut

=over

=item B<< $yes_no = $glk->isCorrectValueType($ext_attr_type, $value); >>
      B<< $yes_no = $glk->isCorrectValueType($ext_attr_type, $value, \$error_message); >>

Given the ExtentAttributeType (or ExtentAttributeType_id) and the value, it returns 1 if the value conforms the validation criteria, 0 otherwise.
It will raise an error if the ExtentAttributeType (or correspondent numeric ID) is not valid.
It takes a reference to a scalar as optional third argument. If passed, this variable is populated with the error messages generated in case of invalid arguments.

=back

=cut

sub isCorrectValueType {
    my ($self, $eat_type, $val, $r_msg, $eid) = @_;
    my $eat_id;
    my $val_type;
    my $min_phone_ln    = 7; ## Minimum number of digits for a phone number to be considered valid
    my $list_separators = qr/[;,\s]+/;
    my $string_pattern  = qr/[\w+=\[\]{}():?&@#\/,|.-]+/;

    unless (defined($eat_type) && $eat_type =~ /^\S+$/) {
        $self->bail("isCorrectValueType() - Called with undefined/empty ExtentAttributeType/id.");
    }
    elsif ($eat_type =~ /^\d+$/) {
        $eat_id = $eat_type;
        $eat_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($eat_type) ) {
            $self->bail("isCorrectValueType() - Called with invalid ExtentAttributeType_id ($eat_id).");
        }
        $val_type = $self->getExtentAttrValueType($eat_id);
    }
    elsif (defined($r_msg) && ref($r_msg) ne 'SCALAR') {
        $self->bail("isCorrectValueType() - Called with improper optional third argument - Expected reference to a scalar.");
    }
    elsif (defined($eid) && $eid !~ /^\d+$/) {
        $self->bail("isCorrectValueType() - Called with improper optional fourth argument - Expected an Extent_id found \"$eid\" instead.");
    }
    else {
        $eat_id = $self->getExtentAttrTypeID($eat_type);

        unless (defined($eat_id)) {
            $self->bail("isCorrectValueType() - Called with invalid ExtentAttributeType (\"$eat_type\").");
        }
        $val_type = $self->getExtentAttrValueType($eat_id);
    }
    ${$r_msg} = '' unless defined($r_msg); ## Since we initialize the variable at each first usage, this shouldn't be necessary. but still is a good measure, in the case we add code that appends text to the message without initializing.

    ## End of preliminary checking. The cleaning and checking of the value is done mainly after we establish that it isn't a flag attribute.
    my $igsp_flu = $self->_is_flu_db();

    ### 'flag' Attributes
    if ($val_type eq 'flag' || $val_type eq 'ignore') {
        return TRUE
    }
    if (!defined($val) || $val =~ /^\s*$/) {
        ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Undefined/empty value for the attribute value on a non-flag attribute \"$eat_type\".";
        $self->logWarn(${$r_msg});
        return FALSE
    }
    elsif ($val eq EXT_ATTR_MISSING_VAL && exists($eat_allowing_missing{$eat_type})) {
        return TRUE
    }
    elsif (lc($val) eq lc(EXT_ATTR_MISSING_VAL) && exists($eat_allowing_missing{$eat_type})) {
        ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - Found keyword for knowingly-missing value, but in the wrong case (found: \"$val\", expected:\"" . EXT_ATTR_MISSING_VAL . "\")";
        $self->_correctDbIfPossible($eid, $eat_type, EXT_ATTR_MISSING_VAL, $r_msg);
        $self->logWarn(${$r_msg});
        return TRUE
    }
    ### Attributes accepting 'Unknown' as a valid value
    if ($val eq EXT_ATTR_UNKNOWN_VAL && exists($eat_allowing_unknown{$eat_type})) {
        return TRUE
    }
    ### Attributes accepting 'Unknown' as a valid value, but another spelling (i.e. different case usage) of 'Unknown' has been used -> raising a warning
    elsif (lc($val) eq lc(EXT_ATTR_UNKNOWN_VAL) && exists($eat_allowing_unknown{$eat_type})) {
        ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - Improper spelling (\"$val\") of '" . EXT_ATTR_UNKNOWN_VAL . "'.";
        $self->_correctDbIfPossible($eid, $eat_type, EXT_ATTR_UNKNOWN_VAL, $r_msg);
        $self->logWarn(${$r_msg});
        return TRUE
    }
    if ($val eq EXT_ATTR_NOT_APPL_VAL && exists($eat_allowing_na{$eat_type})) {
        return TRUE
    }
    ### Attributes accepting 'Unknown' as a valid value, but another spelling (i.e. different case usage) of 'Unknown' has been used -> raising a warning
    elsif (lc($val) eq lc(EXT_ATTR_NOT_APPL_VAL) && exists($eat_allowing_na{$eat_type})) {
        ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - Improper spelling (\"$val\") of '" . EXT_ATTR_NOT_APPL_VAL . "'.";
        $self->_correctDbIfPossible($eid, $eat_type, EXT_ATTR_NOT_APPL_VAL, $r_msg);
        $self->logWarn(${$r_msg});
        return TRUE
    }
    ## If we're here, we know that we have a non-blank value.

    ### 'age' Attributes
    elsif ($val_type eq 'age') {
        my %acceptable_age_text = ('after hatch year'   => undef,
                                   'After hatch year'   => undef,
                                   'After Hatch Year'   => undef,
                                   'hatch year'         => undef,
                                   'Hatch year'         => undef,
                                   'Hatch Year'         => undef,
                                   juvenile             => undef,
                                   Juvenile             => undef,
                                   fledgling            => undef,
                                   Fledgling            => undef,
                                   infant               => undef,
                                   Infant               => undef,
                                   adult                => undef,
                                   Adult                => undef,
                                   elderly              => undef,
                                   Elderly              => undef);
 
        my %accepted_units = (day    => undef,
                              days   => undef,
                              d      => undef,
                              D      => undef,
                              week   => undef,
                              weeks  => undef,
                              w      => undef,
                              W      => undef,
                              month  => undef,
                              months => undef,
                              m      => undef,
                              M      => undef,
                              year   => undef,
                              years  => undef,
                              y      => undef,
                              Y      => undef);
        if (!$igsp_flu) {
            return TRUE
        }
        elsif ($val =~ /^\d+(?:\.5)?\s*(\w+)$/ && exists($accepted_units{$1}) || ($val =~ /^\d+(?:\.5)?(\w+)\s?\d+(?:\.5)?(\w+)$/ || $val =~ /^\d+(?:\.5)?(\w+)\s?\d+(?:\.5)?(\w+)$/ || $val =~ /^\d+(?:\.5)?(\w+)\s?\d+(?:\.5)?(\w+)$/) && exists($accepted_units{$1}) && exists($accepted_units{$2}) || exists($acceptable_age_text{$val})) {
            return TRUE
        }
        ## Some diagnostics
        elsif (exists($acceptable_age_text{lc($val)})) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - Word(s) used to describe the age (\"$val\") must be all lower-case.";
            $self->logWarn(${$r_msg});
        }
        elsif (uc($val) =~ /^\d+(:?\.5)?[YMDW]$/ || uc($val) =~ /^\d+(:?\.5)?Y\s?\d+(:?\.5)?M$/ || uc($val) =~ /^\d+(:?\.5)?M\s?\d+(:?\.5)?[DW]$/ || $val =~ /^\d+(:?\.5)?W\s?\d+(:?\.5)?D$/) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - The correct one-letter abbreviations for year, month, week, and day are all upper-case (\"$val\").";
            $self->logWarn(${$r_msg});
        }
        elsif ($val =~ /^\d+(:?\.5)?\s*[YyMmDdWw]$/ || $val =~ /^\d+(:?\.5)?\s?[Yy]\s?\d+(:?\.5)?\s?[Mm]$/ || $val =~ /^\d+(:?\.5)?\s?[Mm]\s?\d+(:?\.5)?\s?[DdWw]$/ || $val =~ /^\d+(:?\.5)?\s?[Ww]\s?\d+(:?\.5)?\s?[Dd]$/) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - The correct one-letter abbreviations for year, month, week, and day are all upper-case and there shall be no space between the digits and the unit (\"$val\").";
            $self->logWarn(${$r_msg});
        }
        elsif ($val =~ /^\d+(:?\.\d+)?\s*[YyMmDdWw]$/ || $val =~ /^\d+(:?\.\d+)?\s?[Yy]\s?\d+(:?\.\d+)?\s?[Mm]$/ || $val =~ /^\d+(:?\.d+)?\s?[Mm]\s?\d+(:?\.\d+)?\s?[DdWw]$/ || $val =~ /^\d+(:?\.\d+)?\s?[Ww]\s?\d+(:?\.\d+)?\s?[Dd]$/) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" (value type \"$val_type\") - The Only '.5' is allowed as decimal part, one dingle decimal figure. (\"$val\").";
            $self->logWarn(${$r_msg});
        }
        return FALSE
    }
    ### 'auth_list' Attributes ##TODO need double checking to make sure we capture any real problem
    elsif ($val_type eq 'auth_list') {
        my @bad_stuff = ();
        my @authors = split(/,\s+|;\s*/, $val);

        for (my $n = 0; $n < @authors; ++$n) {
            my $auth = $authors[$n];

            if (!$auth =~ /\S/ && $n < $#authors) { ## There is an empty space in the middle of the list
                push(@bad_stuff, 'Empty location in the beginning/middle of the author list');
            }
            elsif ($auth !~ /[A-Z][A-Za-z\s-]*,[A-Z]+\./ && !$igsp_flu) {
                push(@bad_stuff, "The following author name is not in the form \"Last_name,FM.\": \"$auth\".");
            }
        }
        if (scalar(@bad_stuff)) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad author string (\"$val\"):\n" . join("\n", @bad_stuff);
            $self->logWarn(${$r_msg});
            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'boolean' Attributes
    elsif ($val_type eq 'boolean') {
        if ($val == 0 || $val == 1) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad boolean value (\"$val\")";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'country' Attributes
    elsif ($val_type eq 'country') {
        unless(exists($self->{INSDC_Countries}) && defined($self->{INSDC_Countries})) {
            $self->_loadCountries();
        }
        if (exists($self->{INSDC_Countries}{$val})) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad country value (\"$val\").";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'date' Attributes
    elsif ($val_type eq 'date') {

        if ($self->NCBI_Date()) {
            my $candidate_date = $self->_ConvertToNCBIDate($val, 1);
            if (defined($candidate_date)) {
                return TRUE
            }
            else {
                ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad date value (\"$val\")";
                $self->logWarn(${$r_msg});
                return FALSE
            }
        }
        else { ## in the case we aren't converting to NCBI dates, for the moment we do not perform any other check.
            return TRUE
        }
    }
    ### 'date_list' Attributes
    elsif ($val_type eq 'date_list') {

        ### Paolo Amedeo 2012 11 28 - Temporary fix: Perl modules to parse dates and times are currently unreliable. Turning off any date and time validation till implementing a proper replacement within the ProcessingObject library.
        # return TRUE

        if ($self->NCBI_Date()) { ## If we do not convert to NCBI dates, for now we don't perform any other check.
            my @dates = split(/$list_separators/, $val);
            my @problems = ();

            for (my $n = 0; $n < @dates; ++$n) {
                my $date = $dates[$n];

                if ($n == $#dates && $date !~ /\S/) {
                    last; ## If the list ends with a terminator, the very last element of the split is empty, but we don't make much fuss about it.
                }
                my $candidate_date = $self->_ConvertToNCBIDate($date, 1);

                unless (defined($candidate_date)) {
                    push(@problems, 'Position ' . ($n + 1) . " - date \"$date\" is not recognized as being valid.");
                }
            }
            if (@problems) {
                ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad list of dates (\"$val\"):\n" . join("\n", @problems);
                $self->logWarn(${$r_msg});
                return FALSE
            }
            else {
                return TRUE
            }
        }
        else {
            return TRUE
        }
    }
    ### 'datetime' Attributes
    elsif ($val_type eq 'datetime') {

        ### Paolo Amedeo 2012 11 28 - Temporary fix: Perl modules to parse dates and times are currently unreliable. Turning off any date and time validation till implementing a proper replacement within the ProcessingObject library.
        #return TRUE

        if ($self->NCBI_Date()) {## If we do not convert to NCBI dates, for now we don't perform any other check.
            if ($val =~ /^([A-Z][a-z]{2}[\s-]\d{1,2}[\s-]\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)$/ || $val =~ /^([A-Z][a-z]{2}[\s-]\d{1,2}[\s-]\d{4})\s+(\d{2}:\d{2}:\d{2}\s*(?:EST)?)$/) {
                my ($date, $time) = ($1, $2);
                my $candidate_date = $self->_ConvertToNCBIDate($date, 1);

                if (defined($candidate_date)) {
                    my ($hh, $mm, $ss) = split(':', $time);

                    if (defined($ss)) { ## hh:mm:ss 24-hours format
                        if ($hh < 24 && $mm < 60 && $ss < 60) {
                            return TRUE
                        }
                    }
                    else {
                        $mm =~ s/[AP]M$//;

                        if ($hh > 0 && $hh < 13 && $mm < 60) {
                            return TRUE
                        }
                    }
                }
                else {
                    $self->logQuiet("isCorrectValueType() - datetime: unable to parse the string \"$date\" as a valid date.");
                }
            }
            ## From here on we have only bad datetime formats
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad datetime value (\"$val\")";

            # $self->logWarn(${$r_msg}); Temporary solution while figuring out what's wrong with the dates.
            $self->logQuiet(${$r_msg});

            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'e-mail' Attributes
    elsif ($val_type eq 'e-mail') {
        my @addresses = split(/$list_separators/, $val);
        my @problems = ();

        for (my $n = 0; $n < @addresses; ++$n) {
            my $email = $addresses[$n];

            if ($n == $#addresses && $email !~ /\S/) {
                last; ## If the list ends with a terminator, the very last element of the split is empty, but we don't make much fuss about it.
            }
            elsif ($email !~ /[\w.-]+\@[\w.-]+\.[a-zA-Z]+/) {
                push(@problems, 'Position ' . ($n + 1) . " - E-mail address \"$email\" is not recognized as being valid.");
            }
        }
        if (@problems) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad list of e-mail addresses (\"$val\"):\n" . join("\n", @problems);
            $self->logWarn(${$r_msg});
            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'file_path' Attributes
    elsif ($val_type eq 'file_path') {
        if (-e $val || $val =~ /^\(.+\)$/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value for file_path (\"$val\"). Impossible to find the file, directory, or link in the filesystem.";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'gender' Attributes
    elsif ($val_type eq 'gender') {
        if ($val =~ /[FfMm]/ || $val eq 'N/A' || $val eq 'n/a' || $val eq 'unknown' || $val eq 'Not applicable') {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value for gender (\"$val\"). Accepted values: 'M', 'm', 'F', 'f', 'N/A', 'n/a', 'unknown', and 'Not applicable'.";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'int' Attributes
    elsif ($val_type eq 'int') {
        if ($val =~ /^[+-]?\d+$/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value for int datatype (\"$val\").";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'int_list' Attributes
    elsif ($val_type eq 'int_list') {
        my @problems = ();
        my @numbers = split(/$list_separators/, $val);

        for (my $n = 0; $n < @numbers; ++$n) {
            my $int = $numbers[$n];

            if ($n == $#numbers && $int !~ /\S/) { ## The list ends with a record separator, therefore the element after the last separator is empty, but we don't make any fuss about it.
                last;
            }
            elsif ($int !~ /^[+-]?\d+$/) {
                push(@problems, "Position " . $n + 1 . " Bad value (\"$int\").");
            }
        }
        if (@problems) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value(s) in the list of int (\"$val\"):\n" . join("\n", @problems);
            $self->logWarn(${$r_msg});
            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'rational' Attributes
    elsif ($val_type eq 'rational') {
        if ($val =~ /^[+-]?\d+(?:\.\d+)?$/ || $val =~ /^[+-]?\d*\.\d+$/ || $val =~ /^[+-]?\d*(?:\.\d+)?[eE][+-]?\d+$/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad rational number (\"$val\"):\n";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'sra_list' Attributes (semicolon-separated list of colon-separated triplets of strings)
    elsif ($val_type eq 'sra_list') {
        my @triplets = split(/;\s*/, $val);
        
        foreach my $triplet (@triplets) {
            my @elems = split(/:\s*/, $triplet);
            
            unless (scalar(@elems) == 3) {
                ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad rsa_list (\"$val\"):\n";
                $self->logWarn(${$r_msg});
                return FALSE
            }
        }
        return TRUE
    }
    ### 'string' Attributes
    elsif ($val_type eq 'string') {
        if ($val =~ /^$string_pattern$/) {
            return TRUE
        }
        else {
            (my $unacceptable = $val) =~ s/$string_pattern//g;
            my @tmp = split('', $unacceptable);
            my %bad = ();

            foreach my $bad_char (@tmp) {
                if ($bad_char eq "\n") {
                    $bad_char = '[new-line-char]';
                }
                elsif ($bad_char eq "\r") {
                    $bad_char = '[carriage-returnm]';
                }
                undef($bad{$bad_char});
            }

            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value for string (\"$val\").\nIllegal character(s): \"" . join('" "', sort(keys(%bad))) . '".';
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'string_list' Attributes
    elsif ($val_type eq 'string_list') {
        my @problems = ();
        my @strings = split(/$list_separators/, $val);

        for (my $n = 0; $n < @strings; ++$n) {
            my $string = $strings[$n];

            if ($string !~ /^$string_pattern$/) {
               (my $unacceptable = $val) =~ s/$string_pattern//g;
               my %bad = map({$_ => undef} split('', $unacceptable));
               push(@problems, 'Position ' . $n + 1 . ' found one or more occurence of the following illegal character(s): "' . join('", "', sort(keys(%bad))) . '".');
            }
        }
        if (@problems) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value for list of strings (\"$val\"):\n" . join("\n", @problems);
            $self->logWarn(${$r_msg});
            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'taxo_list' Attributes
    elsif ($val_type eq 'taxo_list') {
        ## ordered list of recognized taxonomical terms
        my @taxo_term = (qw(superkingdom
                            kingdom
                            order
                            family
                            subfamily
                            genus
                            species
                            subspecies
                            strain
                            isolate
                            type
                            subtype));
        my %classifier = ();
        my @problems = ();

        for (my $n = 0; $n < @taxo_term; ++$n) {
            $classifier{$taxo_term[$n]} = $n;
        }
        my @strings = split(/$list_separators/, $val);
        my $last_pos = -1;

        foreach my $token (@strings) {
            my $term = (split /\s*=\s*/)[0];

            if (exists($classifier{$term})) {
                if ($classifier{$term} <= $last_pos) { ## Elements repeated or out of order
                    push(@problems, "Taxonomic classifier \"$term\" repeated or out of order - found after \"$taxo_term[$last_pos]\".");
                    $last_pos = $classifier{$term};
                } ## Else everything is fine.
            }
            elsif (exists($classifier{lc($term)})) { ## Wrong character case
                my $msg = "Taxonomic classifiers should be all-lower-case. - Wrong text case (\"$term\").";

                if ($classifier{lc($term)} <= $last_pos) { ## Elements repeated or out of order too
                    $msg .= " Taxonomic classifier \"$term\" repeated or out of order - found after \"$taxo_term[$last_pos]\".";
                    $last_pos = $classifier{$term};
                }
                push(@problems, $msg);
            }
            else { ## Completely unknown term...
                push(@problems, "Unrecognized taxonomic classifier \"$term\".");
            }
        }
        if (@problems) {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value(s):\n" . join("\n", @problems);
            $self->logWarn(${$r_msg});
            return FALSE
        }
        else {
            return TRUE
        }
    }
    ### 'tel' Attributes
    elsif ($val_type eq 'tel') {
        if ($val =~ /^(:?\+?\d+[.\s-]])?\d+([.\s-])\d+\1\d+$/ || $val =~ /^(:?\+?\d+[.\s-])?\(\d+\)\s*\d+[.\s-]?\d+$/ || $val =~ /^\+?\d+[.\s-]?\d+[.\s-]?\d+$/) {
            (my $stripped = $val) =~ s/\D+//g;
            my $length = length($stripped);

            if ($length >= $min_phone_ln) {
                return TRUE
            }
            else {
                ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - The phone number (\"$val\") appears to be to short ($length digits vs. minimum $min_phone_ln).";
                $self->logWarn(${$r_msg});
                return FALSE
            }
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad phone number (\"$val\").";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'text' Attributes
    elsif ($val_type eq 'text') {
        if ($val =~ /\S/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() - Empty value in attribute \"$eat_type\" requiring a value of type \"text\".";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'silent_warning' Attributes
    elsif ($val_type eq 'silent_warning') {
        if ($val =~ /\S/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() - Empty value in attribute \"$eat_type\" Empty value for \"silent_warning\" attribute.";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'unsig_int' Attributes
    elsif ($val_type eq 'unsig_int') {
        if ($val =~ /^\+?\d+$/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad value (\"$val\") for unsign_int.";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ### 'unsig_rational' Attributes
    elsif ($val_type eq 'unsig_rational') {
        if ($val =~ /^\+?\d*(?:\.\d+)?$/ || $val =~ /^\+?\d*\.\d+$/ || $val =~ /^\+?\d*(?:\.\d+)?[eE][+-]?\d+$/) {
            return TRUE
        }
        else {
            ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Bad rational number (\"$val\"):\n";
            $self->logWarn(${$r_msg});
            return FALSE
        }
    }
    ## Unrecognized value type
    else {
        ${$r_msg} = "isCorrectValueType() ExtentAttribute type: \"$eat_type\" - Unexpected vaulue_type \"$val_type\" (\"$val\").";
        $self->logWarn(${$r_msg});
        return FALSE
    }

    sub _correctDbIfPossible {
        my ($self, $eid, $eat_type, $val, $r_msg) = @_;

        if (defined($eid)) {
            if ($self->setExtentAttribute($eid, $eat_type, $val)){
                ${$r_msg} .= ' - Updated in the database.';
            }
            else {
                $self->logWarn("isCorrectValueType()->_correctDbIfPossible() - Extent: $eid, Attribute: \"$eat_type\", Value: \"$val\" - Unable to update the value in the database - make sure you have the right credentials.");
                ${$r_msg} .= ' - Unable to correct it in the database - possibly you do not have the required permissions.';
            }
        }
        else {
            ${$r_msg} .= ' - Returning as correct (unable to change it in the database at this time: specific Extent_id missing).';
        }
    }

}

=over

=item B<< ($flag, $message, $time) = $glk->getResult() >>

This function returns a three item list of the most recently reported database
interaction event.  The flag is a boolean success value, the message is the
message generated by the even, and the time is a timestamp in seconds since the
Epoch (a UNIX time value).

=back

=cut

sub getResult {
    my ($self) = @_;

    return ($self->{'result'}{'flag'}, $self->{'result'}{'mesg'}, $self->{'result'}{'time'})
}

=over

=item B<< $code = $glk->getResultCode() >>

This function returns the result code of the most recent event.

=back

=cut

sub getResultCode {
    my ($self) = @_;

    return $self->{'result'}{'flag'}
}

=over

=item B<< $message = $glk->getResultMessage() >>

This function returns the result message of the most recent event.

=back

=cut

sub getResultMessage {
    my ($self) = @_;

    return $self->{'result'}{'mesg'}
}

=over

=item B<< $timestamp = $glk->getResultTime() >>

This function returns the result time of the most recent event in seconds since
the Epoch (a UNIX time value).

=back

=cut

sub getResultTime {
    my ($self) = @_;

    return $self->{'result'}{'time'}
}

=over

=item B<< $message = $glk->getLastErrorMessage() >>

This function returns the message from the last event which was associated with
a failure success code.

=back

=cut

sub getLastErrorMessage {
    my ($self) = @_;

    return $self->{'error'}{'mesg'}
}

=over

=item B<< $message = $glk->getLastErrorTime() >>

This function returns the time of the last event which was associated with
a failure success code in seconds since the Epoch (a UNIX time value).

=back

=cut

sub getLastErrorTime {
    my ($self) = @_;

    return $self->{'error'}{'time'}
}

# ##############################################################################
#
#    PRIVATE API FUNCTIONS
#
# ##############################################################################

=back

=head2 PRIVATE API FUNCTIONS

These functions are documented only to provide a reference for the internal
workings of GLKLib.  Clients should avoid calling these functions directly as
they are part of the internal implementation and not guaranteed to remain
stable.

=over

=cut

=over

=item B<< $success = $glk->result($code, $message, $arg...) >>

This function returns a boolean success value based on the code provided.  The
message is rendered using C<sprintf()> and the arguments supplied and stored
for future retrieval.

=back

=cut

sub result {
    my ($self, $code, $message, @args) = @_;
    undef $message if ($message eq "");

    unless (defined $message){
        if ($code == 0) {
            $message = "(Failure)";
        }
        else {
            $message = "(Success)";
        }
    }
    $self->{'result'}{'flag'} =  $code;
    $self->{'result'}{'mesg'} = sprintf($message, @args);
    $self->{'result'}{'time'} = time;

    unless ($code) {
        $self->{'error'}{'mesg'} = sprintf($message, @args);
        $self->{'error'}{'time'} = time;
        print STDERR "!!--- ERROR: " . $self->{'error'}{'mesg'} . "\n";
    }
    return $code
}

=over

=item $glk->setQueryLookup($queryTable_hashref)

This function sets the query lookup table.  This will replace the old table, and
should not be used unless you know what you're doing.

=back

=cut

sub setQueryLookup {
    my ($self, $query_hashref) = @_;

    $self->{'query_lookup'} = $query_hashref;
}

=over

=item B<< $statement_handle = $glk->getQueryObject($query_name) >>

This function returns a DBI Statement handle suitable for DBI operations or
other GLKLib functions like C<runQuery>.  If the query name is not found,
C<undef> is returned.

=back

=cut

sub getQueryObject {
    my ($self, $query_name) = @_;
    my $query = $self->{'query'}{$query_name};

    unless (defined $query) {
        if (exists $self->{'query_lookup'}{$query_name}) {
            # Add the query
            my $result = $self->addQuery($query_name, $self->{'query_lookup'}{$query_name});
            $query = $self->{'query'}{$query_name};
        }
        else {
            #print("Query not found in lookup table.\n");
        }
    }

    unless (defined $query) {
        $self->result(0, "Query '$query_name' not found.");
        return undef
    }
    $self->result(1, "Query '$query_name' fetched.");
    return $query
}

=over

=item $glk->addQuery($query_name, $sql_template, $arg...)

Adds a query to the compiled query store.   The SQL template is a string in the
form used by C<sprintf()>.  The arbitrary list of arguments is used to fill in
the template.  This function will make the resulting query available for
immediate use against the database.

=back

=cut

sub addQuery {
    my ($self, $query_name, $sql_template, @args) = @_;

    my $st = $self->compile($sql_template, @args);

    if (defined $st) {
        $self->{'query'}{$query_name} = $st;
        return (1, "Query '$query_name' compiled and ready.")
    }
    else {
        my ($code, $mesg) = $self->getResult();
        return $self->result(0, "Failed to compile query '$query_name':\n     %s", $mesg)
    }

    return $self->result(1, "Query '$query_name' added.")
}

=over

=item B<< $statment_handle = $glk->compile($sql_template, $arg...) >>

This function renders the SQL template and compiles it.  The SQL template is a
string in the form used by C<sprintf()>.  The arbitrary list of arguments is
used to fill in the template.  If compilation succeeds, a DBI Statement handle
is returned.  If it fails, C<undef> is returned.

=back

=cut

sub compile {
    my ($self, $sql_template, @args) = @_;

    unless (defined $self->{'db'}) {
        $self->result(0, "Not connected to a database.");
        return undef
    }
    my $st;

    eval {$st= $self->{'db'}->prepare(sprintf($sql_template, @args));};

    if ($@) {
        $self->result(0, "Error compiling query (%s)", $self->{'db'}->errstr);
        print STDERR $self->{'db'}->errstr . "\n";
        $self->bail("Problems compiling the following query template: \"$sql_template\" with the following arguments: \"" . join('", "', @args) . "\".");
    }
    $self->result(1, "Compilation successful.");
    return $st
}


# ##############################################################################
#
#    SAMPLE STATUS
#
# ##############################################################################

=back

=head2 SAMPLE STATUS

A sample is treated as any extent, which can be associated with a status using
the Status and StatusType tables.  A Status is defined by the extent, tpe, creator,
date status was assigned, and a description.

=over

=cut


=comment NO LONGER USED 2017-04-20

=over

=item B<< $info_hashref = $glk->getStatusInfo($extent_id) >>

This function returns a reference to a hash of information about the status of
any extent (i.e. sample).  If the sample is not found, a reference to empty hash
is returned. On success, the following hash values will exist:

    eid           :  The Extent ID
    type          :  The status type
    creator       :  The person who set the status
    date_assigned :  The date the status was assigned
    desc          :  An optional description of the status

=back

#=cut

sub getStatusInfo {
    my ($self, $extent_id) = @_;

    my %info = ();

    if ($self->runQuery('GET_STATUS_INFO_BY_EXTENT_ID', $extent_id)) {
        my $row = $self->fetchRow('GET_STATUS_INFO_BY_EXTENT_ID');

        %info = ('extent_id'      => $row->{'Extent_id'},
                 'status_id'      => $row->{'Status_id'},
                 'status_type_id' => $row->{'StatusType_id'},
                 'status_type'    => $row->{'type'},
                 'creator'        => $row->{'creator'},
                 'date_assigned'  => $row->{'create_date'},
                 'desc'           => $row->{'description'},
                );
    }
    $self->endQuery('GET_STATUS_INFO_BY_EXTENT_ID');

    return \%info
}
=cut

=over

=item B<< $yes_no = $glk->hasRequiredExtentAttributes($extent_id) >>
      B<< $yes_no = $glk->hasRequiredExtentAttributes($extent_id, \$msg) >>

It checks if the given Extent and all its ancestors have all the required attributes. No searching is done on the children extents.
It returns 1 if all the required attributes are present, 0 otherwise. If the extent is deprecated, it will return undef.
Warning messages are triggered in the following cases:
a) The extent is directly or indirectly flagged as deprecated;
b) The absence of one or more required attributes at any level;
c) The presence of a combination of Extent_Type and ExtentAttributeType not listed among the allowed ones.

The function will raise a fatal exception if called without or with a non-existing Extent ID.

Optionally, hasRequiredExtentAttributes() can take a reference to a scalar as second parameter and will populate it with the error messages generated

=back

=cut

sub hasRequiredExtentAttributes {
    my ($self, $eid, $r_msg) = @_;
    unless (defined($r_msg) && defined(${$r_msg})) {
        ${$r_msg} = '';
    }
    if (!defined($eid) || $eid !~ /^\d+$/) {
        $eid = 'blank, not defined' unless defined($eid);
        $self->bail("hasRequiredExtentAttributes() - Called with invalid/undefined Extent ID ($eid).");
    }
    elsif (!$self->extentExists($eid)) {
        $self->bail("hasRequiredExtentAttributes() - Called with a non-existing Extent ID ($eid).");
    }
    elsif ($self->isDeprecated($eid)) { ## Deprecated extents do not need attributes, therefore we return 1 after raising a warning message.
        ${$r_msg} .= "Extent $eid is deprecated.";
        $self->logWarn("hasRequiredExtentAttributes() - Called with deprecated Extent ($eid).");
        return undef
    }

    ## So far, so good: if we get here, it means that the extent exists and isn't deprecated.
    my @troubles = ();

    my $r_ancestors = $self->getAncestors($eid);
    my $bad_stuff = 0;

    foreach my $extent_id (@{$r_ancestors}) {
        if (exists($self->{ExtAttrTroubles}{$extent_id})) {
            if (scalar(@{$self->{ExtAttrTroubles}{$extent_id}})) { # if bad stuff has been found earlier
                $bad_stuff += scalar(@{$self->{ExtAttrTroubles}{$extent_id}});
                push(@troubles, "\nRepeating problems with ancestor Extents:", @{$self->{ExtAttrTroubles}{$extent_id}}, '-----*- End of cached ancestor problems -*-----');
            }
            next
        }
        else {
            $self->{ExtAttrTroubles}{$extent_id} = [];
        }
        my @illegal = ();
        my $seg_info = $self->getExtentInfo($extent_id);
        my $type = $seg_info->{type};
        my $ref_id = $seg_info->{'ref'};

        if ($type eq 'SEGMENT') {
            unless ($self->hasSeqTechnologies($extent_id)) {
                my $msg = "Segment Extent ID $extent_id - Ref ID: \"$ref_id\" does not have any of Sequence Technologies attributes.";
                $self->logWarn("hasRequiredExtentAttributes() - $msg");
                push(@troubles, $msg);
                push(@{$self->{ExtAttrTroubles}{$extent_id}}, $msg);
                ++$bad_stuff;
            }
        }
        my %required = map({$_ => undef} @{$self->getRequiredExtentAttributes($type)});
        my $r_attrs = $self->getExtentAttributes($extent_id);

        while (my ($att_type, $val) = each(%{$r_attrs})) {
            if (!$self->isLegalExtAttrCombo($type, $att_type)) {
                push(@illegal, $att_type);
            }
            elsif (exists($required{$att_type})) {
                unless (exists($self->{AttrTypeChecked}{$extent_id}{$att_type})) { ## this specific record has not yet been checked in this session, therefore the result is not yet cached
                    $self->{AttrTypeChecked}{$extent_id}{$att_type} = $self->isCorrectValueType($att_type, $val, undef, $extent_id);

                    unless ($self->{AttrTypeChecked}{$extent_id}{$att_type}) { ## If there is a mismatch between the attribute type and the existing value...
                        my $msg = "Extent $extent_id, type: \"$type\", Ref ID \"$ref_id\" (called; with Extent $eid) - Invalid value (\"$val\") for attribute type \"$att_type\".";
                        $self->logWarn("hasRequiredExtentAttributes() - $msg");
                        push(@troubles, $msg);
                        push(@{$self->{ExtAttrTroubles}{$extent_id}}, $msg);
                        ++$bad_stuff;
                    }
                }
                if ($self->{AttrTypeChecked}{$extent_id}{$att_type}) {
                    undef($required{$att_type});
                    delete($required{$att_type});
                }
            }
        }
        if (my $nogood = scalar(@illegal)) {
            $bad_stuff += $nogood;
            my $msg = "Extent $extent_id, type: \"$type\", Ref ID \"$ref_id\" (called with Extent $eid) - Has the following \"illegal\" attribute(s):\n\t\"". join("\"\n\t\"", sort({lc($a) cmp lc($b)} @illegal)) . '"';
            $self->logWarn("hasRequiredExtentAttributes() - $msg");
            push(@troubles, $msg);
            push(@{$self->{ExtAttrTroubles}{$extent_id}}, $msg);
        }
        if (my @missing = sort({lc($a) cmp lc($b)} keys(%required))) {
            $bad_stuff += scalar(@missing);
            my $msg = "Extent $extent_id, type: \"$type\", Ref ID \"$ref_id\" (called with Extent $eid) - Missing the following required Extent Attribute Type(s):\n\t\"" . join("\"\n\t\"", sort({lc($a) cmp lc($b)} @missing)) . '"';
            $self->logWarn("hasRequiredExtentAttributes() - $msg");
            push(@{$self->{ExtAttrTroubles}{$extent_id}}, $msg);
            push(@troubles, $msg);
        }

    }
    if ($bad_stuff) {
        unless (@troubles) {
            $self->bail("I'm screwd!");
        }

        ${$r_msg} .= join("\n", @troubles);
        return FALSE
    }
    else {
        return TRUE
    }
}

=over

=item B<< $yes_no = $glk->isLegalExtAttrCombo($extent_type, $extent_attr_type) >>

Given the Extent_Type.type (or Extent_Type_id) and the ExtentAttributeType.type (or ExtentAttributeType_id), it returns 1 if such combination is legal in the current database, 0 otherwise.
It raises fatal exceptions if either parameter is null or not valid.

=back

=cut

sub isLegalExtAttrCombo {
    my ($self, $ext_type, $ext_attr_type, $db) = @_;
    my ($et_id, $eat_id);

    if (!defined($ext_type) || $ext_type !~/\S/) {
        $self->bail("isLegalExtAttrCombo() - Called with empty/undefined Extent type.");
    }
    elsif ($ext_type =~ /^\d+$/) {
        $et_id = $ext_type;
        $ext_type = $self->getExtentTypeName($et_id);

        unless (defined($ext_type)) {
            $self->bail("isLegalExtAttrCombo() - Called with invalid Extent_Type_id ($et_id). No correspondent type found.");
        }
    }
    else {
        $et_id = $self->getExtentTypeID($ext_type);
        unless (defined($et_id)) {
            $self->bail("isLegalExtAttrCombo() - Called with invalid Extent_Type.type (\"$ext_type\").");
        }
    }
    if (!defined($ext_attr_type) || $ext_attr_type !~ /\S/) {
        $self->bail("isLegalExtAttrCombo() - Called with empty/undefined ExtentAttribue type.");
    }
    elsif ($ext_attr_type =~ /^\d+$/) {
        $eat_id = $ext_attr_type;
        $ext_attr_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ext_attr_type)) {
            $self->bail("isLegalExtAttrCombo() - Called with invalid ExtentAttributeType_id ($eat_id). No correspondent type found.");
        }
    }
    else {
        $eat_id = $self->getExtentAttrTypeID($ext_attr_type);

        unless (defined($eat_id)) {
            $self->bail("isLegalExtAttrCombo() - Called with invalid ExtentAttributeType.type (\"$ext_attr_type\").");
        }
    }
    unless (defined($db)) {
        $db = $self->getDbName();
    }
    ## End of input validation and conversion, now down to real business

    unless (defined($self->{ExtAttrRegister}{$db})) { ## We check directly if it is defined, since we're going to populate it anyway.
        $self->loadExtAttrRegister($db);
    }
    if (exists($self->{ExtAttrRegister}{$db}{$et_id}) && exists($self->{ExtAttrRegister}{$db}{$et_id}{$eat_id})) {
        return TRUE
    }
    else {
        return FALSE
    }
}

=over

=item B<< $true_or_false = $glk->isValidBioProject(\$candidate_bioproject_id) >>

Given a reference to a BioProject ID, it reformat the value, in the most complete form (e.g. "PRJNA183620" instead of just "183620")
If the BioProject ID is recognized as being valid , the function returns TRUE (i.e. 1), otherwise it returns FALSE (i.e. 0)
=back

=cut

sub isValidBioProject {
    my ($self, $r_bp_id) = @_;
    $self->_loadBioProjects();

    if (${$r_bp_id} =~ /^\d+$/ && !exists($self->{BIOPROJECT}{${$r_bp_id}})) {
        ${$r_bp_id} = BIOPROJECT_ID_PREFIX . ${$r_bp_id};
    }
    if (exists($self->{BIOPROJECT}{${$r_bp_id}})) {
        return TRUE
    }
    else {
        return FALSE
    }
}


=over

=item B<< $yes_no = $glk->isUmbrellaBioProject($bioproject_id) >>

Given a BioProject ID, it returns 1 if it is recognized as an Umbrella BioProject, 0 otherwise.
=back

=cut

sub isUmbrellaBioProject {
    my ($self, $bp_id) = @_;
    $self->_loadBioProjects();

    unless (defined($bp_id) && $bp_id =~ /\w/) {
        $self->bail("isUmbrellaBioProject() - Called with undefined/empty BioProject ID.");
    }
    if ($self->isValidBioProject(\$bp_id)) {
        return $self->{BIOPROJECT}{$bp_id}{is_umbrella}
    }
    else {
        $self->bail("isUmbrellaBioProject() - Called with an invalid BioProject ID (\"$bp_id\").");
    }
}


=over

=item B<< $yes_no = $glk->existsCollection($collection_name) >>

Given the name of a collection, it returns 1 if it exists at least a collection with that name, 0 otherwise.

=back

=cut

sub existsCollection {
    my ($self, $coll_name) = @_;

    unless (defined($coll_name) && $coll_name =~ /^\S+$/) {
        no warnings;
        $self->bail("existsCollection() - called with undefined or invalid collection name (\"$coll_name\")");
    }
    unless (defined($self->{COLLECTION})) {
        $self->_loadVcCollections();
    }
    foreach my $data (values(%{$self->{COLLECTION}})) {
        foreach my $name (keys(%{$data})) {
            if ($name eq $coll_name) {
                return TRUE
            }
        }
    }
    return FALSE
}

=over

=item B<< @db_names = @{$glk->getCollectionDbs($collection_name)} >>

Given the name of a collection, it returns a reference to a list with all the names of the databases where that collection exists.

=back

=cut

sub getCollectionDbs {
    my ($self, $coll_name) = @_;

    unless (defined($coll_name) && $coll_name =~ /^\S+$/) {
        no warnings;
        $self->bail("existsCollection() - called with undefined or invalid collection name (\"$coll_name\")");
    }
    unless (defined($self->{COLLECTION})) {
        $self->_loadVcCollections();
    }
    my @dbs = ();

    while (my ($db, $data) = each(%{$self->{COLLECTION}})) {
        foreach my $name (keys(%{$data})) {
            if ($name eq $coll_name) {
                push(@dbs, $db);
                last;
            }
        }
    }
    return \@dbs
}


=over

=item B<< $yes_no = $glk->existsExtAttrPlace($extent_type_id, $extent_attr_type_id) >>

Given the Extent_Type_id and the ExtentAttributeType_id, it returns 1 if such combination exists in vir_common..ExtAttrPlace, 0 otherwise.
It raises fatal exceptions if either parameter is null or not valid.

=back

=cut

sub existsExtAttrPlace {
    my ($self, $et_id, $eat_id) = @_;

    if (!defined($et_id)) {
        $self->bail("existsExtAttrPlace() - Called with undefined Extent_Type_id");
    }
    elsif ($et_id =~ /^\d+$/) {
        my $ext_type = $self->getExtentTypeName($et_id);

        unless (defined($ext_type)) {
            $self->bail("existsExtAttrPlace() - Called with invalid Extent_Type_id ($et_id). No correspondent type found.");
        }
    }
    else {
            $self->bail("existsExtAttrPlace() - Called with invalid Extent_Type_id (\"$et_id\").");
    }
    if (!defined($eat_id) || $eat_id !~ /\S/) {
        $self->bail("existsExtAttrPlace() - Called with empty/undefined ExtentAttribueType_id.");
    }
    elsif ($eat_id =~ /^\d+$/) {
        my $ext_attr_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ext_attr_type)) {
            $self->bail("existsExtAttrPlace() - Called with invalid ExtentAttributeType_id ($eat_id). No correspondent type found.");
        }
    }
    else {
        $self->bail("existsExtAttrPlace() - Called with invalid ExtentAttributeType_id ($eat_id).");
    }
    ## End of input validation and conversion, now down to real business

    unless (defined($self->{ExtAttrPlace_id})) { ## We check directly if it is defined, since we're going to populate it anyway.
        $self->loadExtAttrPlace();
    }
    if (exists($self->{ExtAttrPlace_id}{$et_id}) && exists($self->{ExtAttrPlace_id}{$et_id}{$eat_id})) {
        return TRUE
    }
    else {
        return FALSE
    }
}

=over

=item B<< $$glk->_addVcCollection($collection_eid, $collection_name, $db, $force_insert) >>

Given the the Extent ID of the collection, its name (Extent.ref_id), and the database to which it belongs, it creates the corresponding record in vir_common..Collection
As regular behavior, except for the XX collection, a collection should exist in only a database. Unless the parameter $force_insert is set to anything mapping to TRUE, the method will raise a fatal exception when trying to insert an already existing collection.

=back

=cut

sub _addVcCollection {
    my ($self, $eid, $name, $db, $force_insert) = @_;

    if (!defined($eid) || $eid !~ /^\d+$/) {
        no warnings;
        $self->bail("_addVcCollection() - Called with missing/invalid collection Extent_id (\"$eid\")");
    }
    elsif (!defined($db) || $db !~ /^\S+$/) {
        no warnings;
        $self->bail("_addVcCollection() - Called with missing/invalid database name (\"$db\")");
    }
    elsif (!defined($name) || $name !~ /^\S+$/) {
        no warnings;
        $self->bail("_addVcCollection() - Called with missing/invalid collection name (\"$name\")");
    }
    elsif ($name ne 'XX' && $self->existsCollection($name)) {
        my $r_coll_db = $self->getCollectionDbs($name);

        foreach my $c_db (@{$r_coll_db}) {
            if ($c_db eq $db) {
                $self->logError("_addVcCollection() - Attempting to insert an already existing collectiion (\"$name\") in database $db.");
                FAILURE;
            }
        } ## We pass through here only if the collection exists only in other databases
        unless ($force_insert) {
            $self->bail("_addVcCollection() - Attempting to insert an aready existing collectiion (\"$name\") in database $db. The collection already exists in the following database(s): " . join(", ", @{$r_coll_db}));
        }
    }
    if ($self->runQuery('ADD_VC_COLLECTION', $name, $db, $eid)) {
        $self->endQuery('ADD_VC_COLLECTION');
        return SUCCESS
    }
    else {
        $self->bail("_addVcCollection() - Problems running the ADD_VC_COLLECTION query with the following values: \"$name\", \"$db\", \"$eid\".");
    }
}
=over

=item B<< $$glk->addExtAttrPlace($extent_type, $extent_attr_type) >>

Given the Extent_Type.type (or Extent_Type_id) and the ExtentAttributeType.type (or ExtentAttributeType_id), it registers the combination as legal in vir_common..ExtAttrCombo

=back

=cut

sub addExtAttrPlace {
    my ($self, $et_id, $eat_id) = @_;

    if (!defined($et_id)) {
        $self->bail("addExtAttrPlace() - Called with undefined Extent_Type_id");
    }
    elsif ($et_id =~ /^\d+/) {
        my $ext_type = $self->getExtentTypeName($et_id);

        unless (defined($ext_type)) {
            $self->bail("addExtAttrPlace() - Called with invalid Extent_Type_id ($et_id). No correspondent type found.");
        }
    }
    else {
            $self->bail("addExtAttrPlace() - Called with invalid Extent_Type_id (\"$et_id\").");
    }
    if (!defined($eat_id) || $eat_id !~ /\S/) {
        $self->bail("addExtAttrPlace() - Called with empty/undefined ExtentAttribueType_id.");
    }
    elsif ($eat_id =~ /^\d+$/) {
        my $ext_attr_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ext_attr_type)) {
            $self->bail("addExtAttrPlace() - Called with invalid ExtentAttributeType_id ($eat_id). No correspondent type found.");
        }
    }
    else {
        $self->bail("addExtAttrPlace() - Called with invalid ExtentAttributeType_id ($eat_id).");
    }
    ## End of input validation and conversion, now down to real business

    unless (defined($self->{ExtAttrPlace_id})) { ## We check directly if it is defined, since we're going to populate it anyway.
        $self->loadExtAttrPlace();
    }
    if (exists($self->{ExtAttrPlace_id}{$et_id}) && exists($self->{ExtAttrPlace_id}{$et_id}{$eat_id})) {
        $self->logInfo("addExtAttrPlace() - Called to add the Estent_Type ($et_id) - ExtentAttributeType ($eat_id) combination which already exists.");
    }
    else {
        if ($self->runQuery('ADD_EXT_ATTR_PLACE', $et_id, $eat_id)) {
            $self->endQuery('ADD_EXT_ATTR_PLACE');
            undef($self->{ExtAttrPlace_id});
            undef($self->{ExtAttrPlace_combo});
            undef($self->{ExtAttrRegister});
        }
        else {
            $self->bail("addExtAttrPlace() - Unable to add the Extent_Type ($et_id) - ExtentAttributeType ($eat_id) combination.");
        }
    }
}

=over

=item B<< $$glk->removeExtAttrPlace_by_ET_EAT($extent_type, $extent_attr_type) >>

Given the Extent_Type.type (or Extent_Type_id) and the ExtentAttributeType.type (or ExtentAttributeType_id), it removes the combination from vir_common..ExtAttrCombo

It takes care of purging the correspondent records out of each VGD-type database.

=back

=cut

sub removeExtAttrPlace_by_ET_EAT {
    my ($self, $et_id, $eat_id) = @_;

    if (!defined($et_id)) {
        $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with undefined Extent_Type_id");
    }
    elsif ($et_id =~ /^\d+$/) {
        my $ext_type = $self->getExtentTypeName($et_id);

        unless (defined($ext_type)) {
            $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with invalid Extent_Type_id ($et_id). No correspondent type found.");
        }
    }
    else {
            $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with invalid Extent_Type_id (\"$et_id\").");
    }
    if (!defined($eat_id) || $eat_id !~ /^\S+$/) {
        $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with empty/undefined ExtentAttribueType_id.");
    }
    elsif ($eat_id =~ /^\d+$/) {
        my $ext_attr_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ext_attr_type)) {
            $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with invalid ExtentAttributeType_id ($eat_id). No correspondent type found.");
        }
    }
    else {
        $self->bail("removeExtAttrPlace_by_ET_EAT() - Called with invalid ExtentAttributeType_id ($eat_id).");
    }
    ## End of input validation and conversion, now down to real business

    unless (defined($self->{ExtAttrPlace_id})) { ## We check directly if it is defined, since we're going to populate it anyway.
        $self->loadExtAttrPlace();
    }
    if (!exists($self->{ExtAttrPlace_id}{$et_id}) || !exists($self->{ExtAttrPlace_id}{$et_id}{$eat_id})) {
        $self->logInfo("removeExtAttrPlace_by_ET_EAT() - Called to remove the Estent_Type ($et_id) - ExtentAttributeType ($eat_id) combination which does not exists.");
    }
    else {
        my $r_dbs = $self->getAllVgdDbs();
        my $eap_id = $self->{ExtAttrPlace_id}{$et_id}{$eat_id};

        foreach my $db (@{$r_dbs}) {
            $self->removeExtAttrRegister($eap_id, $db);
        }

        if ($self->runQuery('DELETE_EXT_ATTR_PLACE_BY_ET_EAT', $et_id, $eat_id)) {
            $self->endQuery('DELETE_EXT_ATTR_PLACE_BY_ET_EAT');
            undef($self->{ExtAttrPlace_id});
            undef($self->{ExtAttrPlace_combo});
            undef($self->{ExtAttrRegister}); ## We want to wipe out for all the databases at once.
            $self->loadExtAttrPlace();
        }
        else {
            $self->bail("removeExtAttrPlace() - Unable to remove the Extent_Type ($et_id) - ExtentAttributeType ($eat_id) combination.");
        }
    }
}

=over

=item B<< $glk->addExtAttrRegister($ext_attr_place_id, $val, $db) >>

Given a ExtAttrPlace_id and the associate value, it inserts a record in ExtAttrRegister.
If a database is given as the last optional argument, the record is inserted in the given database, instead of the current one.

=back

=cut

sub addExtAttrRegister {
    my ($self, $eap_id, $val, $db) = @_;

    unless (defined($db)) {
        $db = $self->getDbName();
    }
    if (!$self->_isVgdDb($db)) {
        $self->bail("addExtAttrRegister() - Called on a non-VGD-type database (\"$db\").");
    }
    elsif (!defined($eap_id) || $eap_id !~ /^\d+$/) {
        $self->bail("addExtAttrRegister() - Missing or invalid required attribute ExtAttrPlace_id.");
    }
    elsif (!defined($val) || $val !~ /^\d+$/) {
        $self->bail("addExtAttrRegister() - Missing or invalid required attribute ExtAttrRegister.required.");
    }
    elsif (!defined($self->{ExtAttrPlace_combo}{$eap_id})) {
        $self->bail("addExtAttrRegister() - Attempt to insert an ExtAttrRegister record referring to a non-existant ExtAttrPlace_id ($eap_id).");
    }
    elsif (exists($self->{ExtAttrRegister_EAP}{$db}{$eap_id})) {
        $self->bail("addExtAttrRegister() - Attempt to insert an already-existing ExtAttrRegister record referring to ExtAttrPlace_id ($eap_id).");
    }
    my $qry_name = "ADD_${db}_EXTENT_ATTRIBUTE_REGISTER";

    unless (exists($self->{query}{$qry_name})) {
        $self->addQuery($qry_name, "INSERT $db..ExtAttrRegister (ExtAttrPlace_id, required) VALUES (?, ?)");
    }
    if ($self->runQuery($qry_name, $eap_id, $val)) {
        $self->endQuery($qry_name);
        $self->loadExtAttrRegister($db);
        return SUCCESS
    }
    else {
        $self->logError("addExtAttrRegister() - Impossible to insert $db..ExtAttrRegister record (ExtAttrPlace_id: $eap_id, required: $val).");
        return FAILURE
    }
}

=over

=item B<< $glk->updateExtAttrRegister($ext_attr_place_id, $val, $db) >>

Given a ExtAttrPlace_id and the associate value, it updates the correspondet record in ExtAttrRegister.
If a database is given as the last optional argument, the record is updated in the given database, instead of the current one.

=back

=cut

sub updateExtAttrRegister {
    my ($self, $eap_id, $val, $db) = @_;

    if (!defined($eap_id) || $eap_id !~ /^\d+$/) {
        $self->bail("updateExtAttrRegister() - Missing or invalid required attribute ExtAttrPlace_id.");
    }
    elsif (!defined($val) || $val !~ /^\d+$/) {
        $self->bail("updateExtAttrRegister() - Missing or invalid required attribute ExtAttrRegister.required");
    }
    elsif (!defined($self->{ExtAttrPlace_combo}{$eap_id})) {
        $self->bail("updateExtAttrRegister() - Attempt to update an ExtAttrRegister record referring to a non-existant ExtAttrPlace_id ($eap_id).");
    }
    elsif (!exists($self->{ExtAttrPlace_combo}{$eap_id})) {
        $self->bail("updateExtAttrRegister() - Attempt to update a non-existent ExtAttrRegister record referring to ExtAttrPlace_id ($eap_id).");
    }
    unless (defined($db)) {
        $db = $self->getDbName();
    }
    my $qry_name = "UPDATE_${db}_EXTENT_ATTRIBUTE_REGISTER";

    unless (exists($self->{query}{$qry_name}) && defined($self->{query}{$qry_name})) {
        $self->addQuery($qry_name, "UPDATE $db..ExtAttrRegister SET required = ? WHERE ExtAttrPlace_id = ?");
    }

    if ($self->runQuery($qry_name, $val, $eap_id)) {
        $self->endQuery($qry_name);
        $self->loadExtAttrRegister($db);
        return SUCCESS
    }
    else {
        $self->logError("updateExtAttrRegister() - Impossible to update the $db..ExtAttrRegister record for ExtAttrPlace_id $eap_id.");
        return FAILURE
    }
}

=over

=item B<< $glk->removeExtAttrRegister($ext_attr_place_id, $db) >>

Given the ExtAttrPlace_id and (optionally) the name of the database, it removes the correspondent record from ExtAttrRegister.
If the name of the database is omitted, it will delete from the current database.

=back

=cut

sub removeExtAttrRegister {
    my ($self, $eap_id, $db) = @_;

    if (!defined($eap_id) || $eap_id !~ /^\d+$/) {
        $self->bail("removeExtAttrRegister() - Missing or invalid required attribute ExtAttrPlace_id.");
    }
    elsif (!defined($db) || $db !~ /^\S+$/) {
         $db = $self->getDbName;
    }
    my $qry_name = "DELETE_EXT_ATTR_REGISTER_IN_$db";

    unless (exists($self->{query}{$qry_name}) && defined($self->{query}{$qry_name})) {
        $self->addQuery($qry_name, "DELETE $db..ExtAttrRegister WHERE ExtAttrPlace_id = ?");
    }
    if ($self->runQuery($qry_name, $eap_id)) {
        $self->endQuery($qry_name);
        $self->loadExtAttrRegister($db);
        return SUCCESS
    }
    else {
        $self->logError("removeExtAttrRegister() - Impossible to delete ExtAttrRegister record in database $db.");
        return FAILURE
    }
}

=over

=item B<< $glk->getAllExtAttrRegister($ext_attr_place_id, $db) >>

It returns a reference to an hash whose keys are the ExtAttrPlace_id and the values are the ExtAttrRegister.required values.

=back

=cut

sub getAllExtAttrRegister {
    my ($self, $db) = @_;

    if (!defined($db) || $db !~ /^\S+$/) {
         $db = $self->getDbName();
    }
    my $qry_name = "GET_ALL_EXT_ATTR_REGISTER_IN_$db";

    unless (exists($self->{ExtAttrRegister}{$db})) {
        $self->loadExtAttrRegister($db);
    }
    return $self->{ExtAttrRegister_EAP}{$db}
}

=over

=item B<< $yes_no = $glk->isMandatoryExtAttrCombo($extent_type, $extent_attr_type, $db) >>

Given the Extent_Type.type (or Extent_Type_id) and the ExtentAttributeType.type (or ExtentAttributeType_id), it returns 1 if such combination is mandatory (i.e. that type of Extent has to have that attribute in that database), 0 otherwise.
If a database name is given as the last, optional argument, the function will use that database instead of the current one.

It raises a warning and returns undef if the combination is not deemed even legal.
It raises fatal exceptions if either parameter is null or not valid.

Note: later on, when we vhave better integrate the whole validation concept, it will raise at least an error-level message for illegal combinations.

=back

=cut

sub isMandatoryExtAttrCombo {
    my ($self, $ext_type, $ext_attr_type, $db) = @_;
    my ($et_id, $eat_id);

    if (!defined($ext_type) || $ext_type !~/\S/) {
        $self->bail("isMandatoryExtAttrCombo() - Called with empty/undefined Extent type.");
    }
    elsif ($ext_type =~ /^\d+$/) {
        $et_id = $ext_type;
        $ext_type = $self->getExtentTypeName($et_id);

        unless (defined($ext_type)) {
            $self->bail("isMandatoryExtAttrCombo() - Called with invalid Extent_Type_id ($et_id). No correspondent type found.");
        }
    }
    else {
        $et_id = $self->getExtentTypeID($ext_type);
        unless (defined($et_id)) {
            $self->bail("isMandatoryExtAttrCombo() - Called with invalid Extent_Type.type (\"$ext_type\").");
        }
    }
    if (!defined($ext_attr_type) || $ext_attr_type !~ /\S/) {
        $self->bail("isMandatoryExtAttrCombo() - Called with empty/undefined ExtentAttribue type.");
    }
    elsif ($ext_attr_type =~ /^\d+$/) {
        $eat_id = $ext_attr_type;
        $ext_attr_type = $self->getExtentAttrTypeName($eat_id);

        unless (defined($ext_attr_type)) {
            $self->bail("isMandatoryExtAttrCombo() - Called with invalid ExtentAttributeType_id ($eat_id). No correspondent type found.");
        }
    }
    else {
        $eat_id = $self->getExtentAttrTypeID($ext_attr_type);

        unless (defined($eat_id)) {
            $self->bail("isMandatoryExtAttrCombo() - Called with invalid ExtentAttributeType.type (\"$ext_attr_type\").");
        }
    }
    unless (defined($db)) {
        $db = $self->getDbName();
    }
    ## End of input validation and conversion, now down to real business

    unless (defined($self->{ExtAttrRegister}{$db})) { ## We check directly if it is defined, since we're going to populate it anyway.
        $self->loadExtAttrRegister($db);
    }
    if (exists($self->{ExtAttrPlace_id}{$et_id}) && exists($self->{ExtAttrPlace_id}{$et_id}{$eat_id})) {
        my $eap_id = $self->{ExtAttrPlace_id}{$et_id}{$eat_id};

        if (exists($self->{ExtAttrRegister_EAP}{$db}{$eap_id})) {
            return $self->{ExtAttrRegister_EAP}{$db}{$eap_id}
        }
        else {
            $self->logWarn("isMandatoryExtAttrCombo() - Called with a combination of Extent Type (\"$ext_type\") and Extent Attribute Type (\"$ext_attr_type\") \"illegal\" for database $db");
            return undef
        }
    }
    else {
        $self->logError("isMandatoryExtAttrCombo() - Called with a combination of Extent Type (\"$ext_type\") and Extent Attribute Type (\"$ext_attr_type\") \"illegal\" for all the databases.");
    }
}

=comment NO LONGER USED 2017-04-20

=over

=item B<< $type = $glk->getStatusByExtentId($extent_id) >>

This function returns a the status name for any extent, given the extent id.

=back

#=cut

sub getStatusByExtentId {
    my ($self, $extent_id) = @_;
    my $query = 'GET_STATUS_TYPE_BY_EXTENT_ID';
    my $status;

    if ($self->runQuery($query, $extent_id)) {
        $status = $self->fetchSingle($query);
    }
    $self->endQuery($query);
    return $status
}
=cut 

=comment NO LONGER USED 2017-04-20

=over

=item B<< $extents_with_status_info = $glk->getStatusInfoByType($type) >>

This function returns a reference to a list of hash references, where each
hash contains the status info and extent ID for each sample that is currently
in a given state. If none are found, an empty list is returned.

=back

#=cut

sub getStatusInfoByType {
    my ($self, $type) = @_;
    $self->_cleanValue(\$type, 'getStatusInfoByType()', '$type');
    my $query = 'GET_STATUS_INFO_BY_STATUS_TYPE';

    my @extents;

    if ($self->runQuery($query, $type)) {
        # Faster to fetchall_arrayref
        my $rows = $self->fetchAllArrayRef($query);

        # store each row as a hashref using our predefined keys
        foreach (@$rows) {
            push(@extents, {'extent_id'      => $_->[0],
                            'status_id'      => $_->[1],
                            'status_type_id' => $_->[2],
                            'status_type'    => $_->[3],
                            'creator'        => $_->[4],
                            'date_assigned'  => $_->[5],
                            'desc'           => $_->[6]});
        }
    }
    $self->endQuery($query);

    return \@extents
}
=cut

=over


# ##############################################################################
#
#    DEPRECATED FUNCTIONS
#
# ##############################################################################

=back

=head2 DEPRECATED FUNCTIONS

These functions remain in the GLKLib, but are deprecated.  They should not be
used and may be removed in the future.

=over

=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $csid = $glk->getCloneSysByTSAttr($attr, $val) >>

This function non-deterministically fetches a CloningSystem which contains a
TrimSequence which has the given Attribute set to the supplied value.

B<DEPRECATED>: This is a non-deterministic search, and such lookups have been
replaces by CloningSystem naming and better design patterns.

=back

#=cut

# TODO Deprecated
sub getCloneSysByTSAttr {
    my ($self, $attr, $value) = @_;

    $self->_cleanValue(\$attr,  'getCloneSysByTSAttr()', '$attr');
    $self->_cleanValue(\$value, 'getCloneSysByTSAttr()', '$value');

    my $attr_id = $self->getTrimSeqAttrTypeID($attr);

    my $clonesys = undef;

    if ($self->runQuery('GET_CLONESYS_BY_TRIMSEQ_ATTR', $attr_id, $value)) {
        $clonesys = $self->fetchSingle('GET_CLONESYS_BY_TRIMSEQ_ATTR');
    }
    $self->endQuery('GET_CLONESYS_BY_TRIMSEQ_ATTR');

    return $clonesys
}
=cut

=comment NO LONGER USED 2017-04-20

=over

=item B<< $tsid = $glk->getTrimSequence($attr, $value, $seq) >>

This is a specialized function which searches for a TrimSequence based on a
given Attribute and the expected base sequence.

B<DEPRECATED>: Not only is this function inefficient, it is prone to failure and
and can introduce bugs which are not easily noticed. It has been superseded by
better naming of TrimSequences. It should not be used.

=back

#=cut

sub getTrimSequence {
    my ($self, $attr, $value, $seq) = @_;

    $self->_cleanValue(\$attr,  'getTrimSequence()', '$attr');
    $self->_cleanValue(\$value, 'getTrimSequence()', '$value');
    $self->_cleanValue(\$seq,   'getTrimSequence()', '$seq');

    my $tsaid = $self->getTrimSeqAttrName($attr);
    my $tsid = undef;

    if ($self->runQuery('GET_TRIMSEQ_BY_SEQ_ATTR', $tsaid, $value, $seq)) {
        $tsid = $self->fetchSingle('GET_TRIMSEQ_BY_SEQ_ATTR');
    }
    $self->endQuery('GET_TRIMSEQ_BY_SEQ_ATTR');

    return $tsid
}
=cut

=comment NO LONGER USED 2017-04-20

# TODO: Deprecated
sub getSequencesByTrackBAC {
    my ($self, $bacid) = @_;

    my @seqs = ();
    if ($self->runQuery('GET_TRACKBAC_SEQS', $bacid)) {
        while(my $seqname = $self->fetchSingle('GET_TRACKBAC_SEQS')) {
            push @seqs, $seqname;
        }
    }
    $self->endQuery('GET_TRACKBAC_SEQS');

    return \@seqs
}
=cut

=comment NO LONGER USED 2017-04-20

# TODO: Deprecated
sub getUnrepresentedReads {
    my ($self, $bacid) = @_;

    my @seqs = ();

    if ($self->runQuery('GET_UNREPRESENTED_READS', $bacid)) {
        while (my $seqname = $self->fetchSingle('GET_UNREPRESENTED_READS')) {
            push(@seqs, $seqname);
        }
    }
    $self->endQuery('GET_UNREPRESENTED_READS');

    return \@seqs
}
=cut


=comment NO LONGER USED 2017-04-20

# TODO: Deprecated
sub getUnrepresentedReadsByBAC {
    my ($self, $bacid) = @_;
    my @seqs = ();

    if ($self->runQuery('GET_UNREPRESENTED_READS_BY_BAC', $bacid)) {
        while (my $seqname = $self->fetchSingle('GET_UNREPRESENTED_READS_BY_BAC')) {
            push(@seqs, $seqname);
        }
    }
    $self->endQuery('GET_UNREPRESENTED_READS_BY_BAC');

    return \@seqs
}
=cut

#
#  DEPRECATED!!
#

sub getChildLibraries {
    my ($self, $eid) = @_;

    my @liblist = ();

    if ($self->runQuery('GET_CHILD_LIBRARIES', $eid)) {
        while(my $libid = $self->fetchSingle('GET_CHILD_LIBRARIES')) {
            push(@liblist, $libid);
        }
    }
    $self->endQuery('GET_CHILD_LIBRARIES');

    return \@liblist
}

# #########################################################################
#
#    Extra Documentation
#
# #########################################################################

=head1 USAGE

<To be completed>

=head1 AUTHORS

Jeff Sitz <jsitz@tigr.org>
Dan Katzel <dkatzel@tigr.org>

=head1 BUGS

TIGR::GLKLib is under active development and updates often.

Submit all bug reports to <bits.se@tigr.org>

=cut

=comment NO LONGER USED 2017-04-20


sub populateSequenceFeature {
    my ($self, $seq_name,$featType ) = @_;

    $self->_cleanValue(\$seq_name, 'populateSequenceFeature()', '$seq_name');
    $self->_cleanValue(\$featType, 'populateSequenceFeature()', '$featType');

    my %feature = ();

    if ($self->runQuery('GET_SEQUENCE_FEATURE_BY_TYPE', $seq_name, $featType)) {
        my $row = $self->fetchRow('GET_SEQUENCE_FEATURE_BY_TYPE');
        %feature = ('5' => $row->{'end5'},
                    '3' => $row->{'end3'});
    }
    $self->endQuery('GET_SEQUENCE_FEATURE_BY_TYPE');

    return \%feature
}
=cut

=comment NO LONGER USED 2017-04-20

sub getAvgQuality {
    my ($self,$seq_name ) = @_;

    my $ret=undef;

    if ($self->runQuery('GET_AVG_QUALITY', $seq_name)) {
        my $row = $self->fetchRow('GET_AVG_QUALITY');
        $ret = $row->{'avg_quality'};
    }
    $self->endQuery('GET_AVG_QUALITY');
    return $ret
}
=cut

=over

=item B<< $glk->logLocal($err_msg, $levels) >>

It prints the message as a warning at STDERR and, if a Logger::Log4perl object is defined,
it logs a LOG-level message through that object

=back

=cut

sub logLocal {
    my ($self, $msg, $log_level) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->log($log_level, $msg);
    }
    else {
        warn "$msg\n";
    }
}

=over

=item B<< $glk->logError($err_msg) >>

It prints the message as a warning at STDERR and, if a Logger::Log4perl object is defined,
it logs a ERROR-level message through that object.

=back

=cut

sub logError {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->error($msg);
    }
    else {
        $self->_silentWarning({Detection => 'logError()', Message => $msg});
        warn "$msg\n";
    }
}

=over

=item B<< $glk->logWarn($err_msg) >>

It writes the error message to STDERR and, if a Logger::Log4perl object is defined,
it logs a WARN-level message through that object.

=back

=cut

sub logWarn {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->warn($msg);
    }
    else {
        $self->_silentWarning({Detection => 'logWarn()', Message => $msg});

        unless($self->{WO_Warnings}) {
            warn "$msg\n";
        }
    }
}

=over

=item B<< $glk->logQuiet($err_msg) >>

It writes the error message only to the error log file and, if a Logger::Log4perl object is defined,
it logs a LOG-level message through that object.

=back

=cut

sub logQuiet {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->warn($msg);
    }
    else {
        $self->_silentWarning({Detection => 'logWarn()', Message => $msg});
    }
}

=over

=item B<< $glk->setWrittenOnlyWarnings($yes_no) >>

When set to 1 warning messages are only written to the error-log file. When set to 1 (default), warnings appear to STDERR as well.

=back

=cut

sub setWrittenOnlyWarnings {
    my ($self, $yes_no) = @_;

    unless (defined($yes_no)) {
        $self->logdie("setWrittenOnlyWarnings() - Called with undefined value.")
    }
    $self->{WO_Warnings} = $yes_no;
}

=over

=item B<< $glk->setAttrValValidation($yes_no) >>

When set to FALSE, No validation is performed on the values fo ExtentAttribute pulled from the database.

=back

=cut

sub setAttrValValidation {
    my ($self, $yes_no) = @_;

    unless (defined($yes_no)) {
        $self->logdie("setAttrValValidation() - Called with undefined value.")
    }
    $self->{VAL_VALIDATION} = $yes_no;
}

=over

=item B<< $glk->logInfo($msg) >>



=back

=cut

sub logInfo {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->info($msg);
    }
    else {
        if (DEBUG) {
            $self->_silentWarning({Detection => 'logInfo()', Message => $msg});
        }
    }
}
=over

=item B<< $glk->logTrace($msg) >>



=back

=cut

sub logTrace {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->trace($msg);
    }
    else {
        if (DEBUG) {
            $self->_silentWarning({Detection => 'logTrace()', Message => $msg});
        }
    }
}

=over

=item B<< $glk->bail($err_msg) >>

It dies printing the message at STDERR and, if a Logger::Log4perl object is defined,
it logs a LOG-level message through that object.

=back

=cut

sub bail {
    my ($self, $msg) = @_;

    if (defined($self->{logger})) {
        $self->{logger}->logdie($msg)
    }
    else {
        unless (++$bail_count > 2) { # This is to avoid deep recursions between bail() and _silentWarning().
            $self->_silentWarning({Detection => 'bail()', Message => $msg});
        }

        die "$msg\n"
    }
}

=over

=item B<< $glk->changeDb($new_db) >>

It safely change the database to use, purging all the existing compiled queries.

=back

=cut

sub changeDb {
    my ($self, $db_name) = @_;

    if (defined($db_name) && $db_name =~ /\S/) {
        unless ($self->_isVgdDb($db_name)) {
            $self->bail("changeDb() called with an invalid database name (\"$db_name\") - Not a recognized VGD database.")
        }
    }
    else {
        $self->bail('changeDb() called without valid database name')
    }
    unless (defined($self->{db})) {
        $self->bail('changeDb() - No active database connections. This function requires one.')
    }
    ## Flushing all the compiled queries...
    $self->_flushQueries();
    $self->_cleanCaches();

    $self->{db}->do("USE $db_name") || $self->bail("changeDb() - Impossible to change database connection to use database \"$db_name\".");
    $self->{db_name} = $db_name;
    $self->_set_date_format();
    if ($self->{SW_no}) { ## We print this information only if we have already any silent warning.
        $self->logWarn("changeDb() - Database changed to: \"$db_name\"");
    }
}
=over

=item B<< $glk->clearSilentWarningCounter()) >>

It zeroes the counter of silent warnings. it is used internally in GLKLib and can be used externally for suppressing unnecessary log messages in multipartite programs.

=back

=cut

sub clearSilentWarningCounter {
    my ($self) = @_;
    $self->{SW_no} = 0;
}

=item B<< $glk->NCBI_Date($true_or_false) >>

It either sets or return the value of the flag NCBI_Dates, controlling the behavior of the module in reference to the conversion of all the dates contained in ExtentAttribute values to NCBI format (non-flu project) or not (flu project)
It always return the current value of the flag.

my $yes_no = $glk->NCBI_Date();
$glk_lib->NCBI_Date(1);

=back

=cut

sub NCBI_Date {
    my ($self, $yes_no) = @_;

    if (defined($yes_no)) {
        $self->{NCBI_Dates} = $yes_no;
    }
    return $self->{NCBI_Dates}
}


=over

=item B<< $glk->_ConvertToNCBIDate($date_string) >>

It takes a string supposedly containing a date in any format and returns a string with the date formatted according to NCBI standards (dd-Mmm-yyyy) or undef if the format is not recognized.

my $iso_date = $glk->_ConvertToNCBIDate($date_string);
my $iso_date = $glk->_ConvertToNCBIDate($date_string, $strict);

If a non-zero/null value is passed as second argument, the parsinf of the date goes through stricter validation

=back

=cut

sub _ConvertToNCBIDate {
    my ($self, $date_string, $strict) = @_;
    use constant MIN_YEAR               => 1800;
    use constant MAX_YEAR               => 2100;
    use constant MIN_MONTH              => 1;
    use constant MAX_MONTH              => 12;

    my %pds_options = (WHOLE            => 1,
                       DATE_REQUIRED    => 1,
                       TIME_REQUIRED    => 0,
                       NO_RELATIVE      => 1,
                       PREFER_PAST      => 1,
                       VALIDATE         => 1);

    $pds_options{STRICT} = 1 if $strict;

    my @tri_month = (qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec));

    unless (defined($date_string) || $date_string !~ /\S/) {
        $self->bail("_ConvertToNCBIDate() - Missing required parameter date_string")
    }
    if ($date_string eq EXT_ATTR_UNKNOWN_VAL) {
        return $date_string
    }
    elsif (lc($date_string) eq lc(EXT_ATTR_UNKNOWN_VAL)) {
        $self->logWarn("Wrong spelling (\"$date_string\") for an unknown date. Returning the correct spelling: \"" . EXT_ATTR_UNKNOWN_VAL . "\".");
        return EXT_ATTR_UNKNOWN_VAL
    }
    $self->_cleanValue(\$date_string, '_ConvertToNCBIDate()', '$date_string');
    my $numeric_date;

    if ($date_string =~ /^x/i) {
        $date_string =~ s/[xX]{2,3}-//g;
    }
    if ($date_string =~ /^\d{8}$/ || $date_string =~ /^[A-Z][a-z]{2}[\s-]\d{1,2}[\s-]\d{4}$/ || $date_string =~ /^\d{1,2}([^a-zA-Z0-9])\d{1,2}\1\d{2,4}$/ || $date_string =~ /^\d{2,4}([^a-zA-Z0-9])\d{1,2}\1\d{1,2}$/ || $date_string =~ /^\d{1,2}([^a-zA-Z0-9])[A-Za-z]{3}\1\d{4}$/) {
        $numeric_date = parsedate($date_string, %pds_options);
    }
    unless(defined($numeric_date)) {
        ## Trying a few transformations to spoonfeed the transformation
        (my $trial_date = $date_string) =~ s/_/-/g; # Replacing any possible '_' with simple dashes
        my $this_year = $dt->year();

        if ($trial_date =~ /^(\d{4})(\W)(\d{2})\2?$/ && $1 > MIN_YEAR && $1 < MAX_YEAR && $3 > 0 && $3 < 13) { ## i.e. yyyy-mm
            my ($year, $month) = ($1, $3);

            if ($month >= MIN_MONTH && $month <= MAX_MONTH) { # It's a valid number to be a month...
                return $tri_month[--$month] . '-' . $year
            }
            else {
                $self->logWarn("_ConvertToNCBIDate() - Unable to parse the string \"$date_string\" into a meaningful date.");
                return undef
            }
        }
        elsif ($trial_date =~ /^(\d{2})(\W)(\d{4})$/ && $1 > 0 && $1 < 13 && $3 > MIN_YEAR && $3 < MAX_YEAR) { ## i.e. mm-yyyy
            my ($month, $year) = ($1, $3);

            if ($month >= MIN_MONTH && $month <= MAX_MONTH) { # It's a valid number to be a month...
                return $tri_month[--$month] . '-' . $year
            }
            else {
                $self->logWarn("_ConvertToNCBIDate() - Unable to parse the string \"$date_string\" into a meaningful date.");
                return undef
            }
        }
        elsif ($trial_date =~ /^(\d{4})(\d{2})$/ && $1 > MIN_YEAR && $1 <= $this_year && $2 >= MIN_MONTH && $2 <= MAX_MONTH) { ## i.e. yyyymm
            my ($year, $month) = ($1, $2);
            return $tri_month[--$month] . '-' . $year

        }
        elsif ($trial_date =~ /^(\d{2})(\d{4})$/ && $1 >MIN_MONTH && $1 <= MAX_MONTH && $2 > MIN_YEAR && $2 <= $this_year) { ## i.e. mmyyyy
            my ($month, $year) = ($1, $2);

            return $tri_month[--$month] . '-' . $year
        }
        elsif ($trial_date =~ /^(\d{4})$/ && $1 > MIN_YEAR && $1 <= $this_year) { ## i.e. yyyy
            return $1
        }
        elsif ($trial_date =~ /^(\d{1,2}([^a-zA-Z0-9])[A-Za-z]{3}\2)(\d{2})$/) {
            my ($first, $last) = ($1, $3);
            my $now = DateTime->now();
            my $candidate_year = "20$last";

            if ($candidate_year > $this_year) {
                $candidate_year -= 100;
            }
            $trial_date = $first . $candidate_year;
            $numeric_date = parsedate($trial_date, %pds_options);
        }
        elsif ($trial_date =~ /^([A-Za-z]{3})[^a-zA-Z0-9](\d{4})$/) {
            my ($month, $year) = ($1, $2);
            my $good_month = 0;

            foreach my $i_month (@tri_month) {
                if (lc($month) eq lc($i_month)) {
                    return "$i_month-$year"
                }
            }
        }
        elsif ($trial_date =~ /^([A-Z][a-z]{2})[\s-](\d{1,2})[\s-](\d{4})$/) {
            my ($month, $day, $year) = ($1, $2, $3);
            
            foreach my $i_month (@tri_month) {
                if (lc($month) eq lc($i_month)) {
                    return "$day-$month-$year"
                }
            }
        }

        if (defined($numeric_date)) {
            $self->logInfo("_ConvertToNCBIDate() - Modified date converted into numeric date $numeric_date");
        }
        else {
            $self->logWarn("_ConvertToNCBIDate() - Unable to recognize the string \"$date_string\" as a valid date format.");
            return undef
        }
    }
    ## At this point we're sure that the variable $numeric_date is defined.

    my $dt = DateTime->from_epoch(epoch => $numeric_date);
    my $iso_date = sprintf("%02d-%3s-%4d", $dt->day(), $dt->month_abbr(), $dt->year());

    if ($iso_date =~ /\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}/i) {
        return $iso_date
    }
    else {
        $self->logError("_ConvertToNCBIDate() - Unable to properly translate the alledged date \"$date_string\" into NCBI format (\"$iso_date\").");
        return undef
    }
}
=over

=item B<< $glk->ConvertToNCBIDate($date_string) >>

It takes a string supposedly containing a date in any format and returns a string with the date formatted according to NCBI standards (dd-Mmm-yyyy) or undef if the format is not recognized.

my $iso_date = $glk->ConvertToNCBIDate($date_string);
my $iso_date = $glk->ConvertToNCBIDate($date_string, $strict);

If a non-zero/null value is passed as second argument, the parsinf of the date goes through stricter validation

=back

=cut

sub ConvertToNCBIDate {
    my $self = shift();
    return $self->_ConvertToNCBIDate(@_)
}

=over

=item B<< $glk->mergeAuthors() >>

Given a reference to a list and a string of author names, it populates the list with the parsed author names, if the list is empty or it will split the authors string at the placeholder and combine everything in a list where the authors previously in the list will be in place of theplaceholder.
If the list contains already any element and the string lacks any placeholder, this method will raise a fatal exception.

$glk->mergeAuthors(\@authors, $author_string);

=back

=cut

sub mergeAuthors {
    my ($self, $r_auth, $auth_str) = @_;
    if (scalar(@{$r_auth}) && $auth_str !~ $auth_plchold_regex) {
        $self->bail("mergeAuthors() - The provided author string (\"$auth_str\") doesn't contain any placeholder. Impossible to merge it with the authors of the children features.")
    }
    my ($front, $back) = split($auth_plchold_regex, $auth_str);

    if (defined($front) && $front =~ /\S/) {
        my $r_first = $self->splitAuthors($front);

        foreach my $author (reverse(@{$r_first})) {
            unshift(@{$r_auth}, $author);
        }
        # @{$r_auth} = (@{$r_first}, @{$r_auth});
    }
    if (defined($back) && $back =~ /\S/) {
        my $r_last = $self->splitAuthors($back);
        push(@{$r_auth}, @{$r_last});
    }
}

=over

=item B<< $glk->splitAuthors() >>

Given a string of author names, it splits it in a list of hashes each containing a list of initials and the last anme of one of the authors.
The order reflects the one found in the original string. It can parse author strings in either first-name-first format or last-name-first format.

First-name-first format: A B Brown; C D White; E Black;F G H Blue; Q I
Last-name-first format: Brown,A.B., White,C.D., Black,E.,Blue,F.G.H., I, Q.

in both formats, spaces after non-consecutive separators are optional.

my @authors = @{$glk->splitAuthors($author_string)};

=back

=cut

sub splitAuthors {
    my ($self, $auth_string) = @_;
    my @authors = ();
    $auth_string =~ s/$auth_plchold_regex//g;
    my @candidates = split(/(?:\.\s*,|;)/, $auth_string);

    foreach my $auth (@candidates) {
        $auth =~ s/^\s+//;
        $auth =~ s/\s+$//;

        if ($auth =~ /^\w[^,]+,\s*\w\.?/) { ## Last-name-first format
            my ($last, $initials) = split(/,\s*/, $auth);
            push(@authors, {firstInitials => [split(/\.\s*/, $initials)], lastName => $last});
        }
        elsif ($auth =~ /^\w\s+\w/) { ## First-name-first format
            my @pieces = split(/\s+/, $auth);
            my @first = ();
            my @last = ();

            while (my $piece = shift(@pieces)) {
                if (length($piece) == 1) {
                    push(@first, $piece);
                }
                else {
                    @last = ($piece, @pieces);
                    last;
                }
            }
            unless (scalar(@last)) { ## This is true only in the unlikely case when the last name is only one letter long.
                push(@last, pop(@first));
            }
            push(@authors, {firstInitials => \@first, lastName => join(' ', @last)});
        }
        else {
            $self->bail("splitAuthors() - Unrecognized format for author: \"$auth\"\n")
        }

    }
    return \@authors
}

=over

=item B<< $glk->toFirstNameFirstAuthorList() >>

Given an array of data structures containing authors, it returns a string of authors formatted in the first-name-first format (e.g. "A B Brown; C D White; E Black;F G H Blue; Q I")

my $first_name_first_authors_string = $glk->toFirstNameFirstAuthorList(\@authors);

If the provided reference to the data structure is empty, it raises a warning and returns an empty string.

=back

=cut

sub toFirstNameFirstAuthorList {
    my ($self, $r_authors) = @_;

    unless (scalar(@{$r_authors})) {
        $self->logWarn("toFirstNameFirstAuthorList() - Called with a reference to an empty array of authors.");
        return ''
    }
    my @f_authors = ();

    foreach my $author (@{$r_authors}) {
        push(@f_authors, join(FNF_INITIALS_SEPARATOR, @{$author->{firstInitials}}) . FNF_INITIALS_SEPARATOR . $author->{lastName});
    }
    return join(FNF_AUTHOR_SEPARATOR, @f_authors)
}

=over

=item B<< $glk->toLastNameFirstAuthorList() >>

Given an array of data structures containing authors, it returns a string of authors formatted in the last-name-first format (e.g. "Brown,A.B., White,C.D., Black,E.,Blue,F.G.H., I, Q.")

my $last_name_first_authors_string = $glk->toLastNameFirstAuthorList(\@authors);

If the provided reference to the data structure is empty, it raises a warning and returns an empty string.

=back

=cut

sub toLastNameFirstAuthorList {
    my ($self, $r_authors) = @_;


    unless (scalar(@{$r_authors})) {
        $self->logWarn("toLastNameFirstAuthorList() - Called with a reference to an empty array of authors.");
        return ''
    }
    my @f_authors = ();

    foreach my $author (@{$r_authors}) {
        push(@f_authors, $author->{lastName} . LNF_LAST_N_SEPARATOR . join(LNF_INITIALS_SEPARATOR, @{$author->{firstInitials}}) . LNF_INITIALS_SEPARATOR);
    }
    return join(LNF_AUTHOR_SEPARATOR, @f_authors)
}

=over

=item B<< $glk->_is_flu_db() >>

It returns 1 if the database is one among the flu databases, 0 otherwise

my $is_flu = $glk->_is_flu_db();

=back

=cut

sub _is_flu_db {
    my $self = shift();
    my %flu_dbs = (barda    => undef,
                   giv      => undef,
                   giv2     => undef,
                   giv3     => undef,
                   givtest  => undef,
                   piv      => undef,
                   swiv     => undef,);

    unless (defined($self->{db})) {
        $self->bail("_is_flu_db() - The module has lost any database connection")
    }
    $self->{db_name} = $self->getDbName();

    return exists($flu_dbs{$self->{db_name}}) ? 1 : 0
}

=over

=item B<< $glk->_set_date_format() >>

If the database to which we are connected is not a flu database, it set the flag instructing the object to convert the dates to NCBI format.
If it is instead a flu database, it unset the flag.

$glk->_set_date_format();

=back

=cut

sub _set_date_format {
    my $self = shift();

    if ($self->_is_flu_db()) {
        $self->NCBI_Date(0);
    }
    else {
        $self->NCBI_Date(1);
    }
}

=over

=item B<< my $db_name = $glk->getDbName() >>

It retrieves the name of the current database through the dbh object (running the query "select db_name();") and sets the value in the GLKLib object attribute 'db_name'
=back

=cut

sub getDbName {
    my $self = shift();
    unless (defined($self->{db})) {
        $self->bail("getDbName() - Lost database connection")
    }
    ($self->{db_name}) = $self->{db}->selectrow_array('SELECT db_name()');

    unless (defined($self->{db_name})) {
        $self->bail("getDbName() - Unable to get the name of the current database")
    }
    return $self->{db_name}
}


=over

=item B<< $glk->_silentWarning() >>

It logs all the relevant informations of problematic events, like known Extentattributes without required value, etc.

E.g. $self->_silentWarning({'issue' => 'Blank attribute value in non-flag attribute', 'Extent ID' => $eid, 'ExtentAttributeType' => $type}) unless $is_flag;

=back

=cut


sub _silentWarning {
    my ($self, $r_info) = @_;
    unless (defined($r_info) && scalar(keys(%{$r_info}))) {
        $self->logError('_silentWarning() called without arguments', 1);
        return undef
    }
    my $message;
    my $log_fh = $self->_silentLog();
    my $dt = DateTime->now();
    $dt->set_time_zone('local');
    my $time = $dt->hms();

    $message .= "$time\n----------\n";

    unless (exists($self->{SW_no}) && defined($self->{SW_no}) && $self->{SW_no} == 0) { ## We print this info only the first time around
        my $hostname = hostname();
        my $db_name = "was: $self->{db_name}" || "undef";
        eval {my $db_name = $self->getDbName()};

        if ($@) {
            $db_name = "lost connection with any database";
        }

        my $working_dir = getcwd();
        my $user = $self->_getUser();

        $message .= "User: $user\n".
                        "Hostname: $hostname\n".
                        "Working Dir: $working_dir\n".
                        "Database: $db_name\n".
                        "-------------------------------\n";
    }

    while (my ($param, $val) = each(%{$r_info})) {
        $message .= "$param: $val\n";
    }
    $message .= "\nCalling stack:\n------------------------------------------------------\n";
    my $n = 0;

    while (my ($package, $filename, $line, $sub) = (caller($n++))[0..3]) {
        $message .= "Level: $n\tPackage: \"$package\"\tFile Name: \"$filename\"\tLine: $line\tFunction called: \"$sub\"\n";
    }
    $message .= "------------------------------------------------------\n\n";

    if (defined($self->{logger})) {
        $self->{logger}->warn($message);
    }
    else {
        print {$log_fh} $message;
    }
    return ++$self->{SW_no}
}



=over

=item B<< my $sil_log_fh = $glk->_silentLog($sil_log_filename) >>

If a filename is passed as argument, it closes the current silent log file, if any is open, opens the new one (creating the directory structure, if necessary), and returns the new file handle.
Whenever is called without arguments, it returns the current silent log file file handle, or undef if nonw is currently open.
=back

=cut

sub _silentLog {
    my ($self, $filename) = @_;

    if (!defined($filename)) {
        if (exists($self->{silLog}) && defined($self->{silLog})) { # silLog file already initialized...
            return $self->{silLog}
        }
        $filename = $silent_log_file;
    }
    my $dir_name = dirname($filename);

    unless (-d $dir_name) {
        mk_tree_safe($dir_name, 0777) || $self->bail("_silentLog() - Impossible to create the directory \"$dir_name\"")
    }
    ## Attermpting to create a log file readable despite the mask setting of the user (e.g. restrictive daemon user mask, etc.)
    open(my $fh, ">$filename") || $self->bail("_silentLog() - Impossible to open the file \"$filename\" for writing\n");
    #chmod_safe($filename, 0666);
    system("chmod 666 $filename");

    if (defined($self->{silLog})) {
        close($self->{silLog}) || $self->logWarn("_silentLog() - Problems attempting to close the old silent log file");
        $self->clearSilentWarningCounter; # If we close a log file (Not yet found a single reason for doing that), we reset the counter of the Silent Warnings too
    }
    $self->{silLog} = $fh;
    return $self->{silLog}
}

=over

=item B<< my $bad = $glk->_cleanValue(\$value, $function_name, $variable_name) >>

Given a reference to a string, it removes possible whitespaces at the beginning and at the end.
If problematic characters are found, a silent_warning is raised.
The function takes two more arguments: the name of the calling function and the name of the variable that is currently checked.
It returns a positive number if corrections have been made to the value, zero otherwise.

=back

=cut

sub _cleanValue {
    my ($self, $r_val, $funct_name, $var_name) = @_;
    my %info = (Detection   => '_clean_value()',
                'Raw value' => "'${$r_val}'",
                Function    => $funct_name,
                Variable    => $var_name);
    if (defined($funct_name)) {
        $info{Function} = $funct_name;
    }
    my $bad = 0;

    if (${$r_val} =~ s/^\s+//) {
        $info{Issues} = 'Found empty spaces before the value';
        ++$bad;
    }
    if (${$r_val} =~ s/\s+$//) {
        if (defined($info{Issues})) {
            $info{Issues} .= '; ';
        }
        $info{Issues} .= 'Found empty spaces after the value';
        ++$bad;
    }
    if ($bad) {
        $self->_silentWarning(\%info);
    }
    return $bad
}



=over

=item B<< my $username $glk->_getUser() >>

It scans all possible system variables to identify the user running the application.
=back

=cut

sub _getUser {
    my $self = shift();
    my $user = exists($ENV{USERNAME}) && defined($ENV{USERNAME}) ? $ENV{USERNAME} :
               exists($ENV{USER})     && defined($ENV{USER})     ? $ENV{USER}     :
               exists($ENV{LOGNAME})  && defined($ENV{LOGNAME})  ? $ENV{LOGNAME}  : `whoami`;
    return $user
}


sub DESTROY {
    my $self = shift();
    if (defined($self->{silLog})) {
        close($self->{silLog});
    }
    foreach my $sth (values(%{$self->{query}})) {
        $sth->finish() if defined($sth);
    }
    if (defined($self->{db}) && !$self->{inherited_dbh}) {
        $self->{db}->disconnect();
    }
    if (exists($self->{SW_no}) && defined($self->{SW_no}) && $self->{SW_no} && !defined($self->{logger})) {
        print STDERR "\n\n-------------------------------ATTENTION ATTENTION ATTENTION ---------------------------\n\n",
                     "The program has generated important error-logging information stored in the file: \"$silent_log_file\"\n\n",
                     "------------------ Please, forward this message to de developer's team. Thanks!-----------------\n\n";
    }

}

=over

=item B<< $glk->_flushQueries() >>

It finishes and destroyes all the compiled and partially-executed queries.

=back

=cut

sub _flushQueries {
    my $self = shift();
    foreach my $sth (values(%{$self->{query}})) {
        $sth->finish();
    }
    $self->{query} = {};
}
=over

=item B<< $glk->_cleanCaches() >>

It wipes out all the database-specific cached information.

=back

=cut

sub _cleanCaches {
    my $self = shift();

    foreach my $dataset (qw(daddy table_exists segMap_file seg_names seg_numbers child XX extent_root AttrTypeChecked ExtAttrTroubles Published EXTENT_EXISTS)) {
        undef($self->{$dataset});
        delete($self->{$dataset});
    }
}

=over

=item B<< my @all_VGD_dbs = @{$self->getAllVgdDbs()} >>

It returns a reference to a list of all the names of all the VGD-type databases.

=back

=cut

sub getAllVgdDbs {
    my $self = shift();

    unless (exists($self->{VGD_DBs}) && scalar(@{$self->{VGD_DBs}})) {;
        $self->runQuery('GET_DB_NAMES');

        while (my $row = $self->fetchRow('GET_DB_NAMES')) {
            push(@{$self->{VGD_DBs}}, $row->{db});
        }
        $self->endQuery('GET_DB_NAMES');
    }
    return $self->{VGD_DBs}
}

=over

=item B<< $self->_loadDeprecated() >>

It loads into memory all the records from vir_common.deprecated

=back

=cut

sub _loadDeprecated {
    my $self = shift();

    unless (exists($self->{DEPRECATED}) && scalar(keys(%{$self->{DEPRECATED}}))) {
        $self->runQuery('LOAD_DEPRECATED');

        while(my $row = $self->fetchRow('LOAD_DEPRECATED')) {
            undef($self->{DEPRECATED}{$row->{Extent_id}});
        }
        $self->endQuery('LOAD_DEPRECATED');
    }
}

=over

=item B<< $self->_loadBioProjects() >>

It loads into memory all the records from vir_common..BioProject

=back

=cut

sub _loadBioProjects {
    my $self = shift();

    unless (exists($self->{BIOPROJECT}) && scalar(keys(%{$self->{BIOPROJECT}}))) {
        $self->runQuery('LOAD_BIOPROJECTS');

        while(my $row = $self->fetchRow('LOAD_BIOPROJECTS')) {
            $self->{BIOPROJECT}{$row->{BioProject_id}} = {locus_tag_prefix  => $row->{locus_tag_prefix},
                                                          project_aim       => $row->{project_aim},
                                                          project_title     => $row->{project_title},
                                                          is_umbrella       => $row->{is_umbrella}};
        }
        $self->endQuery('LOAD_BIOPROJECTS');
    }
}

=over

=item B<< mt $combined_val = $self->_deRedundifyAttr(\@values, $val_type) >>

Given a reference to an array containing one or more values of one attribute and the value type for that attribute, it eliminates any possible redundant value.

=back

=cut

sub _deRedundifyAttr {
    my ($self, $r_attrs, $val_type, $separator) = @_;

    unless (defined($separator)) {
        $separator = ATTR_COMBINE_SEPARATOR;
    }
    my $combi_string;

    if ($val_type =~ /list/) { ## Each single value could be composed of a series of values
        my ($found_semi, $found_comma) = (0) x 2;
        my @vals = ();

        foreach my $val (@{$r_attrs}) {
            if ($val =~ /\S+;\s*\S+/) { ## In this value the separator is a semi-colon
                ++$found_semi;
                push(@vals, split(/;\s*/, $val));
            }
            elsif ($val =~ /\S+,\s*\S+/) { ## In this value the separator is a comma
                ++$found_comma;
                push(@vals, split(/,\s*/, $val));
            }
            else { ## It contains a single value, possibly with a separator at the end
                $val =~ s/[,;]\s*$//;
                push(@vals, $val);
            }
        }
        if ($found_comma && ! $found_semi) {
            $separator = ', ';
        }
        &removeDuplicates(\@vals);
        $combi_string = join($separator, @vals);
    }
    else {
        &removeDuplicates($r_attrs);
        $combi_string = join($separator, @{$r_attrs});
    }
    return $combi_string;

    ## Internal cleanup subroutine

    sub removeDuplicates {
        my ($r_list) = @_;
        my %to_remove = ();

        ## Removing possible spaces at the beginning and end of each element...
        foreach my $elem (@{$r_list}) {
            $elem =~ s/^\s+//;
            $elem =~ s/\s+$//;
        }
        for (my $n = 0; $n < $#{$r_list}; ++$n) {
           my $n_elem = lc($r_list->[$n]);

           for (my $i = $n + 1; $i < @{$r_list}; ++ $i) {
               my $i_elem = lc($r_list->[$i]);

               if ($n_elem eq $i_elem) {
                   undef($to_remove{$i});
               }
           }
        }
        foreach my $i (sort({$b <=> $a} keys(%to_remove))) {
            splice(@{$r_list}, $i, 1);
        }
    }
}
=over

=item B<< my $yes_no = $glk->isVgdDb($db) >>

It returns 1 if the given database is a VGD-schema database 0 otherwise.

=back

=cut

sub isVgdDb {
    my ($self, $db) = @_;
    return $self->_isVgdDb($db)
}

=over

=item B<< my $yes_no = $self->_isVgdDb($db) >>

It returns 1 if the given database is a VGD-schema database 0 otherwise.

=back

=cut

sub _isVgdDb {
    my ($self, $db) = @_;
    my $is_vgd = 0;
    my $r_vgds = $self->getAllVgdDbs();

    foreach my $vgd_name (@{$r_vgds}) {
        if ($db eq $vgd_name) {
            $is_vgd = 1;
            last;
        }
    }
    return $is_vgd
}

=over

=item B<< my $new_string = $st->deredundify_string($redundant_str, qr/[;,]\s*/, ',');>>

It removes identical values from a string containing a list of values. In the above example, the element separators in the string are ';' and ',' followed by zero or more spaces.
This argument should be passed as a pre-compiled regex. The third argument is the combining element to be used in the returned string.
Elements that were repeated multiple times in the string, will be represented at the position of their first occurrence.

=cut

sub deredundify_string {
    my ($self, $val, $regex, $glue) = @_;
    my $logger =  get_logger(ref($self));
    $logger->trace("Entering - Original string: \"$val\"");

    my @elems = split(/$regex/, $val);
    $self->deredundifyList(\@elems);
    my $new_string = join($glue, @elems);
    $logger->trace("Exiting - Parsed string: \"$new_string\".");
    return $new_string
}
=over

=item B<< $st->deredundifyList(\@list);>>

It removes identical values from a redundant list.
Elements that were repeated multiple times in the string, will be represented at the position of their first occurrence.
The actual list passed as parameter (by reference) is being modified, not a copy.

=cut

sub deredundifyList {
    my ($self, $r_elems) = @_;
    my %unique = ();
    my @remove = ();

    for (my $n = 0; $n < @{$r_elems}; ++$n) {
        if ($unique{$r_elems->[$n]}++) {
            push(@remove, $n);
        }
    }
    ## Removing
    foreach my $n (reverse(@remove)) {
        splice(@{$r_elems}, $n, 1);
    }
}

=over

=item B<< my @missing_fields = @{$self->_checkRequiredFields(\%data_obj, \@required_fields)};>>

Given a reference to an hash and a reference to an array with all the mandatory fields, it checks for those elements being present and defined in the hash.
It returns a reference to an array containing the list of the missing fields.

=cut

sub _checkRequiredFields {
    my ($self, $r_data, $r_req) = @_;

    unless (defined($r_data)) {
        $self->bail("_checkRequiredFields() - Missing/undefined required reference to the data object.")
    }
    unless (defined($r_req)) {
        $self->bail("_checkRequiredFields() - Missing/undefined reference to the list of required fields.")
    }
    my @missing = ();

    foreach my$field (@{$r_req}) {
        unless (exists($r_data->{$field}) && defined($r_data->{$field})) {
            push(@missing, $field);
        }
    }
    return \@missing
}



1;
