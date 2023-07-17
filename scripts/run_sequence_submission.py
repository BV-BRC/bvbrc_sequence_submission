#!/usr/bin/env python

import argparse
import csv
import codecs
from datetime import datetime
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import xml.dom.minidom as minidom
import xml.etree.cElementTree as ET
import zipfile

#
#Determine paths.
#
top = os.getenv("KB_TOP")

template_deployed = os.path.join(top, "lib", "templates", "template.sbt")
template_dev = os.path.join(top, "modules", "bvbrc_sequence_submission", "lib", "templates", "template.sbt")
template_local = os.path.join("/home", "ac.mkuscuog", "git", "bvbrc_sequence_submission", "lib", "templates", "template.sbt")
if os.path.exists(template_deployed):
  SBT_TEMPLATE = template_deployed
elif os.path.exists(template_dev):
  SBT_TEMPLATE = template_dev
else:
  SBT_TEMPLATE = template_local

elvira_deployed = os.path.join(top, "lib", "Elvira", "bin", "fluValidator2")
elvira_dev = os.path.join(top, "modules", "bvbrc_sequence_submission", "lib", "Elvira", "bin", "fluValidator2")
elvira_local = os.path.join("/home", "ac.mkuscuog", "git", "bvbrc_sequence_submission", "lib", "Elvira", "bin", "fluValidator2")
if os.path.exists(elvira_deployed):
  FLUVALIDATOR = elvira_deployed
elif os.path.exists(elvira_dev):
  FLUVALIDATOR = elvira_dev
else:
  FLUVALIDATOR = elvira_local

VIGOR_REF_DB = os.path.join("/opt", "patric-common", "runtime", "vigor-4.1.20220621-163707-2385247", "VIGOR_DB", "Reference_DBs")

DATABASE_MAP = {"influenza a virus": "flua", "influenza b virus": "flub", "influenza c virus": "fluc"}
SEQUENCE_VALIDATION_FOLDER_NAME = "SequenceValidation"
GENBANK_SUBMISSION_FOLDER_NAME = "Genbank_submission_files"
SUBMISSION_FOLDER_NAME = "Submission"
MANUAL_SUBMISSION_FOLDER_NAME = "ManualSubmission"
METADATA_FILE_NAME = "metadata.csv"
SUBMISSION_REPORT_FILE_NAME = "Sequence_Validation_Report.csv"
SUBMISSION_FILE_HEADER = ["Unique_Sequence_Identifier", "Segment", "Serotype", "Status", "Messages"]
SEGMENT_MAP = {"1": "PB2", "2": "PB1", "3": "PA", "4": "HA", "5": "NP", "6": "NA", "7": "MP", "8": "NS"}
SRC_FILE_HEADER = ["Sequence_ID", "Organism", "Strain", "Country", "Host", "Collection-date", "Isolation-source", "Serotype"]

def createFASTAFile(output_dir, job_data):
  input_file = os.path.join(output_dir, "input.fasta")
  if job_data["input_source"] == "fasta_file":
    #Fetch input file from workspace
    try:
      fetch_fasta_cmd = ["p3-cp", "ws:%s" %(job_data["input_fasta_file"]), input_file]
      subprocess.check_call(fetch_fasta_cmd, shell=False)
    except Exception as e:
      print("Error copying fasta file from workspace:\n %s" %(e))
      sys.exit(-1)
  elif job_data["input_source"] == "fasta_data":
    #Copy user data to input file
    try:
      with open(input_file, "w+") as input:
        input.write(job_data["input_fasta_data"])
    except Exception as e:
      print("Error copying fasta data to input file:\n %s" %(e))
      sys.exit(-1)

  return input_file

def parseMetadataFile(metadata_file):
  data = {} 

  mf = open(metadata_file)

  reader = csv.DictReader(codecs.EncodedFile(mf, 'utf8', 'utf_8_sig'))
  for row in reader:
    val = {"fasta": [], "row": row, "header": reader.fieldnames}
    data[row["Sample Identifier"].strip()] = val

  mf.close()
  
  return data

def parseFASTAFile(fasta_data):
  for line in fasta_data:
    if line[0] == ">":
      header = line[1:].rstrip()
      ids = header.split("|")
      sample_id = ids[0].replace("Unique_Sample_Identifier:", "").strip()
      sequence_id = ids[1].replace("Unique_Sequence_Identifier:", "").strip()
      break

  data = []
  for line in fasta_data:
    if line[0] == ">":
      yield {"sample_id": sample_id, "sequence_id": sequence_id, "header": header, "data": "".join(data).replace(" ", "").replace("\r", "")}
      data = []
      header = line[1:].rstrip()
      ids = header.split("|")
      sample_id = ids[0].replace("Unique_Sample_Identifier:", "").strip()
      sequence_id = ids[1].replace("Unique_Sequence_Identifier:", "").strip()
      continue
    data.append(line.rstrip())

  yield {"sample_id": sample_id, "sequence_id": sequence_id, "header": header, "data": "".join(data).replace(" ", "").replace("\r", "")}
  #yield {"sample_id": sample_id, "sequence_id": sequence_id, "header": header, "data": data}

def runFluValidator(fasta_file, validator_file):
  try:
    flu_validator_cmd = [FLUVALIDATOR, "-fasta", fasta_file]
    output = subprocess.check_output(flu_validator_cmd, shell=False)
    with open(validator_file, "w") as f:
      f.write(output) 
  except Exception as e:
    print("Error running flu validator for %s:\n %s" %(fasta_file, e))
    sys.exit(-1)

  return ("java" in output and "Exception" in output) == False

def runFluValidatorPerSegment(fasta_file):
  output_file_basename = os.path.splitext(os.path.basename(fasta_file))[0] + "_FLAN"
  try:
    #Generates $output_file_basename$.report and $output_file_basename$.tbl files 
    flu_validator_cmd = [FLUVALIDATOR, "-fasta", fasta_file, "-v", "-tbl", "-o", output_file_basename]
    subprocess.check_output(flu_validator_cmd, shell=False)
  except Exception as e:
    print("Error running flu validator per segment for %s:\n %s" %(fasta_file, e))
    sys.exit(-1)

  return output_file_basename

def parseFluValidatorPerSegmentResult(report_file_path):
  result_map = {}
  message = ''

  try:
    with open(report_file_path, "r") as f:
      for line in f:
        if not(line.startswith("\x1b[0m")):
          line = line.strip()

          if "Fasta" in line:
            result_map["fasta_name"] = line.split(' ')[1]
          elif "WARNING" in line or "ERROR" in line:
            message += line + "\n"
          elif "serotype" in line.lower():
            result_map["serotype"] = line.split(':')[1].strip()
          else:
            segment_line = line.split()
            
            result_map["result"] = re.sub("\x1b\[[0-9]*m", "", segment_line[1])
            result_map["segment"] = re.sub("[\[\]]", "", re.sub("\x1b\[[0-9]*m", "", segment_line[5]))
  except Exception as e:
    print("Error parsing flu validator report file %s:\n %s" %(report_file_path, e))

  result_map["message"] = re.sub("\/0B", "", re.sub("\x1b\[[0-9]*m", "", message))
  return result_map

def runVIGOR4(fasta_file, organism):
  fasta_file_name = os.path.splitext(os.path.basename(fasta_file))[0] 
  database = DATABASE_MAP[organism.lower()]

  #Run VIGOR4 for the database
  try:
    vigor4_cmd = ["vigor4", "-i", fasta_file, "-o", fasta_file_name, "-d", database] 
    subprocess.check_call(vigor4_cmd, shell=False)
  except Exception as e:
    print("Error running VIGOR4 for %s using database %s:\n %s" %(fasta_file, database, e))
    return False

  return True

def parseCDSFile(cds_file_path):
  result = {}

  if os.path.isfile(cds_file_path):
    cf = open(cds_file_path)
    first_line = cf.readline().strip()
    cf.close()

    matches = re.findall('(\S+)\=((\"([^\"]+)\")|(\S+))', first_line)
    for i in matches:
      result[i[0]] = i[1].replace("\"", "")   

  return result

def createSubmissionXML(submission_file, sample_identifier, date):
  submission = ET.Element("Submission")
  
  #Create description part
  description = ET.SubElement(submission, "Description")
  ET.SubElement(description, "Comment").text = "BVBRC Submission:%s" %(sample_identifier)
  organization = ET.SubElement(description, "Organization", type="center", role="owner")
  ET.SubElement(organization, "Name").text = "Bacterial And Viral Bioinformatics Resource Center (BVBRC)"

  #Create action part
  action = ET.SubElement(submission, "Action")
  add_files = ET.SubElement(action, "AddFiles", target_db="GenBank")
  file = ET.SubElement(add_files, "File", file_path="submission.zip")
  ET.SubElement(file, "DataType").text = "genbank-submission-package"
  ET.SubElement(add_files, "Attribute", name="wizard").text = "BankIt_influenza_api"
  status = ET.SubElement(add_files, "Status")
  ET.SubElement(status, "Release")
  identifier = ET.SubElement(add_files, "Identifier")
  ET.SubElement(identifier, "SPUID", spuid_namespace="BVBRC").text = "%s.%s" %(date, sample_identifier)

  #Write xml to submission file
  pretty_xml = minidom.parseString(ET.tostring(submission)).toprettyxml(indent = "   ")
  with open(submission_file, "wb") as sf:
    sf.write(pretty_xml)

def createSBTFile(sbt_file, metadata, affiliation, consortium, first_name, last_name):
  template = open(SBT_TEMPLATE, "r")
  sbt_string = template.read()
  template.close()

  auth_name_template = ("{\n" 
                   "          name name {\n"  
                   "            last \"%s\",\n" 
                   "            first \"%s\",\n" 
                   "            middle \"%s\"\n" 
                   "          }\n"
                   "        },") 

  #cit > authors > names | affil
  cit_auth_names = auth_name_template %(last_name, first_name, "") 

  authors_affiliation = ""
  if authors_affiliation:
    ci_auth_affil = (",\n"
            "        affil \"%s\"\n"
            "      }\n") %(affiliation)

  publication_title = metadata.get("Publication Title", "")

  #Handle pub info based on published or unpublished
  pub_info = ""
  if publication_title == "" or publication_title == 'NA' or publication_title.lower() == 'unpublished':
    #pub > gen
    pub_gen_template = ("gen {\n"
                   "       cit \"unpublished\",\n"
                   "       authors {\n"
                   "         names std {\n"
                   "           %pub_auth_names%\n"
                   "         }\n"
                   "       },\n"
                   "       title \"Direct Submission (BVBRC)\"\n"
                   "     }\n")
    #pub > gen > authors
    authors = metadata.get("Authors", "").split(",")
    pub_auth_names = ""
    for author in authors:
      names = author.strip().split(" ")
      middle = ""
      if len(names) > 2:
        middle = names[1][0] + "." 
      pub_auth_names += auth_name_template %(names[len(names)-1], names[0], middle)

    if consortium:
      pub_auth_names += ("{\n"
                     "      name\n"
                     "        consortium \"%s\"\n"
                     "    },") %(consortium)

    pub_info = pub_gen_template.replace("%pub_auth_names%", pub_auth_names[:-1])
  else:
    pmid = metadata.get("Publication PMID", "")
    if pmid == "":
      pub_info = ""
    else:
      pub_info = "pmid %s" %(pmid)

  #Write data to sbt file
  with open(sbt_file, "wb") as sf:
    sf.write(sbt_string.replace("%cit_authors_names%", cit_auth_names[:-1])
                       .replace("%authors_affil%", authors_affiliation)
                       .replace("%pub_info%", pub_info)
            )

def createZipFile(submission_folder, is_manual_submission):
  zf = zipfile.ZipFile(os.path.join(submission_folder, "submission.zip"), "w", zipfile.ZIP_DEFLATED)

  extensions = (".fsa", ".src", ".sbt", ".tbl", ".sqn") if is_manual_submission else (".fsa", ".src", ".sbt")
  for filename in os.listdir(submission_folder):
    if filename.endswith(extensions):
      f = os.path.join(submission_folder, filename)

      #Add file to submission zip file
      zf.write(f, filename)
   
      #Remove file after zipped
      os.remove(f)

  zf.close()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Sequence Submission Script")
  parser.add_argument("-j", "--jfile", help="json file for job", required=True)
  parser.add_argument("-o", "--output", help="Output directory. defaults to current directory", required=False, default=".")

  args = parser.parse_args()

  #Load job data
  job_data = None
  try:
    with open(args.jfile, "r") as j:
      job_data = json.load(j)
  except Exception as e:
    print("Error in opening job file:\n %s" %(e))
    sys.exit(-1)

  if not job_data:
    print("job_data is null")
    sys.exit(-1)
  print(job_data)

  #Setup output directory
  output_dir = args.output
  output_dir = os.path.abspath(output_dir)
  if not os.path.exists(output_dir):
    os.mkdir(output_dir)
  os.chdir(output_dir)

  output_file = os.path.join(output_dir, job_data["output_file"] + ".txt")

  first_name = job_data["first_name"]
  last_name = job_data["last_name"]
  email = job_data["email"]
  authors_affiliation = job_data["affiliation"] if "affiliation" in job_data else ""
  consortium = job_data["consortium"] if "consortium" in job_data else ""

  #Create input file
  input_file = createFASTAFile(output_dir, job_data)

  if os.path.getsize(input_file) == 0:
    print("Input fasta file is empty")
    sys.exit(-1)

  #Define metadata file
  metadata_file = os.path.join(output_dir, METADATA_FILE_NAME)
  #Fetch input file from workspace
  try:
    fetch_metadata_cmd = ["p3-cp", "ws:%s" %(job_data["metadata"]), metadata_file]
    subprocess.check_call(fetch_metadata_cmd, shell=False)
  except Exception as e:
    print("Error copying metadata file from workspace:\n %s" %(e))
    sys.exit(-1)

  if os.path.getsize(metadata_file) == 0:
    print("Metadata file is empty")
    sys.exit(-1)

  #Read metadata file
  sample_info = parseMetadataFile(metadata_file)

  #Read fasta file
  with open(input_file) as fasta_data:
    for values in parseFASTAFile(fasta_data):
      sample_id = values["sample_id"]
      if sample_id in sample_info.keys():
        sample_info[sample_id]["fasta"].append(values) 
      else:
        print("Sequence id %s does not exist in the metadata. Passing.\n" %(sample_id))

  #Create submission report file
  submission_report_file_path = os.path.join(output_dir, SUBMISSION_REPORT_FILE_NAME)
  submission_report_file = open(submission_report_file_path, 'w')
  submission_report_writer = csv.DictWriter(submission_report_file, fieldnames=SUBMISSION_FILE_HEADER)
  submission_report_writer.writeheader()

  #Create sequence validation folder for initial validation process
  sequence_validation_dir = os.path.join(output_dir, SEQUENCE_VALIDATION_FOLDER_NAME)
  os.mkdir(sequence_validation_dir)

  #Create genbank submission folder 
  genbank_submission_dir = os.path.join(output_dir, GENBANK_SUBMISSION_FOLDER_NAME) 
  os.mkdir(genbank_submission_dir)

  #Create submission folder
  submission_dir = os.path.join(genbank_submission_dir, SUBMISSION_FOLDER_NAME)
  os.mkdir(submission_dir) 

  #Create manual submission folder
  manual_submission_dir = os.path.join(genbank_submission_dir, MANUAL_SUBMISSION_FOLDER_NAME)
  os.mkdir(manual_submission_dir)

  #Process sample submissions
  for sample_identifier, value in sample_info.items():
    print("Processing sample " + sample_identifier)

    #Create sample folder for internal review
    sample_dir = os.path.join(sequence_validation_dir, sample_identifier) 
    os.mkdir(sample_dir)

    #Create sample submission folder for genbank
    sample_submission_dir = os.path.join(submission_dir, sample_identifier)
    os.mkdir(sample_submission_dir)

    #Create manual sample submission folder
    manual_sample_submission_dir = os.path.join(manual_submission_dir, sample_identifier)
    os.mkdir(manual_sample_submission_dir)

    #Change working directory to sample submission folder
    os.chdir(sample_dir)

    segments = []
    #Create individual fasta file for the sample
    fasta_file = os.path.join(sample_dir, sample_identifier + ".fasta")
    fsa_file_ms = os.path.join(manual_sample_submission_dir, sample_identifier + ".fsa")
    with open(fasta_file, "w") as ff, open(fsa_file_ms, "w") as ffm:
      for fasta in value["fasta"]:
        ff.write(">" + fasta["header"] + "\n")

        sequence_id = fasta["sequence_id"]
        ffm.write(">" + sequence_id + "\n")

        #Create individual fasta files for segments
        segments.append(sequence_id.split("-")[1])
        segment_file = os.path.join(sample_dir, sequence_id + ".fasta") 
        with open(segment_file, "w") as sf:
          sf.write(">" + sequence_id + "\n")
    
          #Write data to files
          ff.write(fasta["data"] + "\n")
          ffm.write(fasta["data"] + "\n")
          sf.write(fasta["data"])

    #Copy fsa file to genbank folder
    shutil.copy(fsa_file_ms, os.path.join(sample_submission_dir, sample_identifier + ".fsa"))

    #Create individual metadata file for the sample
    sample_metadata_file = os.path.join(sample_submission_dir, sample_identifier + ".src")
    with open(sample_metadata_file, "wb") as smf:
      writer = csv.DictWriter(smf, delimiter='\t', fieldnames=SRC_FILE_HEADER)
      writer.writeheader()
      
      # Parse date in the correct format
      date = value["row"]["Collection Date"]
      dashCount = date.count('-')
      if dashCount == 2:
          date = datetime.strptime(date, '%d-%b-%y').strftime('%d-%b-%Y')
      elif dashCount == 1:
          date = datetime.strptime(date, '%b-%y').strftime('%b-%Y')
      elif dashCount == 0 and len(date) == 2 and date != 'U':
          date = datetime.strptime(date, '%y').strftime('%Y')

      for fasta in value["fasta"]:
          writer.writerow({"Sequence_ID": fasta["sequence_id"],
                           "Organism": value["row"]["Organism"], 
                           "Strain": value["row"]["Strain Name"], 
                           "Country": value["row"]["Collection Country"], 
                           "Host": value["row"]["Host"], 
                           "Collection-date": date,
                           "Isolation-source": value["row"]["Isolation Source"], 
                           "Serotype": value["row"]["Subtype"]})

    #Copy metadata file to manual submission folder
    shutil.copy(sample_metadata_file, os.path.join(manual_sample_submission_dir, sample_identifier + ".src"))

    #Validate sample FASTA file with FLAN
    flan_validator_file = os.path.join(sample_dir, sample_identifier + ".flu")
    isFLANSuccessful = runFluValidator(fasta_file, flan_validator_file)

    #Annotate sample FASTA file with VIGOR4
    runVIGOR4(fsa_file_ms, value["row"]["Organism"])

    #Copy tbl file to manual submission folder
    sample_tbl_file = sample_identifier + ".tbl"
    shutil.copy(os.path.join(sample_dir, sample_tbl_file), os.path.join(manual_sample_submission_dir, sample_tbl_file)) 

    #Run VIGOR4 and FLAN for each segment
    for segment in segments:
      segment_file = os.path.join(sample_dir, "%s-%s.fasta" %(sample_identifier, segment))

      #Run VIGOR4
      print("SF: " + segment_file)
      isVIGOR4Successful = runVIGOR4(segment_file, value["row"]["Organism"]) 

      vigor_status = "Failed"
      if isVIGOR4Successful:
        #Check .tbl file for validation
        tbl_file = os.path.join(sample_dir, "%s-%s.tbl" %(sample_identifier, segment))
        if os.path.isfile(tbl_file):
          if  os.path.getsize(tbl_file) > 0:
            vigor_status = "Processed"
          else: 
            vigor_status = "No Annotation"
        else:
          vigor_status = "Error"

      #Run FLAN
      flan_output_basename = runFluValidatorPerSegment(segment_file)

      #FLAN output files
      flan_tbl_file = os.path.join(sample_dir, flan_output_basename + ".tbl")
      flan_report_file = os.path.join(sample_dir, flan_output_basename + ".report")

      #Validate FLAN result
      flan_status = "Error"
      flan_segment = ""
      flan_message = ""
      if os.path.isfile(flan_tbl_file) and os.path.isfile(flan_report_file):
        #Parse FLAN result for validation
        result = parseFluValidatorPerSegmentResult(flan_report_file)
        flan_status = result["result"]
        flan_message = result["message"]
        flan_segment = result["segment"]
        flan_serotype = result["serotype"]

        fasta_segment = os.path.splitext(result["fasta_name"])[0].split("-")[1]
        if SEGMENT_MAP[fasta_segment] != result["segment"]:
          flan_message += "ERROR: Sequence segment id (%s) doesn't match with flu annotation segment result (%s)" %(fasta_segment, result["segment"]) 
          flan_status = "Failed"

      cds_result = parseCDSFile(os.path.join(sample_dir, "%s-%s.cds" %(sample_identifier, segment)))  
      segment_result = flan_segment if flan_segment == cds_result.get("gene", "") else "VIGOR:%s, FLAN:%s" %(cds_result.get("gene", ""), flan_segment)
      status_result = flan_status if flan_status == "VALID" and vigor_status == "Processed" else "VIGOR:%s, FLAN:%s" %(vigor_status, flan_status)
      if len(cds_result) > 0:
        submission_report_writer.writerow({"Unique_Sequence_Identifier": "%s-%s" %(sample_identifier, segment),
                                           "Segment": segment_result,
                                           "Serotype": flan_serotype,
                                           "Status": status_result,
                                           "Messages": flan_message.replace("\n", ", ")})
      else:
        submission_report_writer.writerow({"Unique_Sequence_Identifier": "%s-%s" %(sample_identifier, segment),
                                           "Segment": "VIGOR: , FLAN:%s" %(flan_segment),
                                           "Serotype": flan_serotype,
                                           "Status": "VIGOR:ERROR, FLAN:%s" %(flan_status),
                                           "Messages": flan_message.replace("\n", ", ")})

    #Create template file with authour information
    sbt_file = os.path.join(sample_submission_dir, sample_identifier + ".sbt")
    createSBTFile(sbt_file, value["row"], authors_affiliation, consortium, first_name, last_name)

    #Copy template file to manual submission folder
    sbt_file_ms = os.path.join(manual_sample_submission_dir, sample_identifier + ".sbt")
    shutil.copy(sbt_file, sbt_file_ms)

    #Create sqn file for manual submission
    try:
      os.chdir(manual_sample_submission_dir)
      #tb2asn_cmd = ["tbl2asn", "-p", manual_sample_submission_dir, "-i", fsa_file_ms, "-t", sbt_file_ms, "-o", os.path.join(manual_sample_submission_dir, sample_identifier + ".sqn"), "-V", "bvg", "-a", "d", "-X", "C"] 
      #Use os.system instead of subprocess until figure out why subporcess is failing
      os.system("tbl2asn -i " + sample_identifier + ".fsa -t " + sample_identifier + ".sbt -o " + sample_identifier + ".sqn -V bvg -a d -X C")
      os.remove(sample_identifier + ".gbf")
      os.remove(sample_identifier + ".t2g")
      os.remove(sample_identifier + ".val")
    except Exception as e:
      print("Error creating sqn file:\n %s" %(e))
      sys.exit(-1)

    os.chdir(sample_dir)

    #Create submission.zip files
    createZipFile(sample_submission_dir, False)
    createZipFile(manual_sample_submission_dir, True)

    #Create submission.xml file
    submission_file = os.path.join(sample_submission_dir, "submission.xml")
    submission_date = datetime.today().strftime('%Y-%m-%d')
    createSubmissionXML(submission_file, sample_identifier, submission_date)

    #Copy submission.xml file to manual submission folder
    shutil.copy(submission_file, os.path.join(manual_sample_submission_dir, "submission.xml"))

    #Create submit.ready files
    with open(os.path.join(sample_submission_dir, "submit.ready"), "w") as sr:
      pass
    with open(os.path.join(manual_sample_submission_dir, "submit.ready"), "w") as sr:
      pass

  #Change working directory back to output folder
  os.chdir(output_dir)
  
  #Close file
  submission_report_file.close()
