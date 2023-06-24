# Sequence Submission Service
The Sequence Submission service allows user to validate and submit virus sequences to NCBI Genbank. User-provided metadata and FASTA sequences are validated against the Genbank data submission standards to identify any sequence errors before submission. Sequences are also annotated using the VIGOR4 and FLAN annotation tools for internal use by users. The Sequence Submission service provides a validation report that should be reviewed by the user before submitting the sequences to the Genbank. 

> Current pipeline only supports Influenza A, B, or C virus family.

This service expects a submitter information, sequence or FASTA file and metadata file in CSV format. Metadata standards, metadata template and example FASTA files are available [here](https://www.bv-brc.org/workspace/BVBRC@patricbrc.org/BV-BRC%20Templates). 

Example JSON input for the service:

```
{
  "country": "USA",
  "numberOfSequences": 1,
  "output_file": "bvbrc_sequence_submission_example",
  "consortium": "",
  "output_path": "/olson@patricbrc.org/home/testn",
  "first_name": "",
  "last_name": "",
  "email": "",
  "affiliation": "",
  "input_source": "fasta_file",
  "metadata": "/olson@patricbrc.org/PATRIC-QA/DataSets/SequenceSubmission/sequence_submission_metadata_test.csv",
  "phoneNumber": "",
  "input_fasta_file": "/olson@patricbrc.org/PATRIC-QA/DataSets/SequenceSubmission/sequence_submission_test.fasta"
}
```