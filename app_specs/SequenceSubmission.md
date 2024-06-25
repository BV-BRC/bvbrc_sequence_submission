
# Application specification: SequenceSubmission

This is the application specification for service with identifier SequenceSubmission.

The backend script implementing the application is [App-SequenceSubmission.pl](../service-scripts/App-SequenceSubmission.pl).

The raw JSON file for this specification is [SequenceSubmission.json](SequenceSubmission.json).

This service performs the following task:   Sequence Submission

It takes the following parameters:

| id | label | type | required | default value |
| -- | ----- | ---- | :------: | ------------ |
| input_source | Source of input (id_list, fasta_data, fasta_file, genome_group) | enum  | :heavy_check_mark: |  |
| input_fasta_data | Input sequence in fasta formats | string  |  |  |
| input_fasta_file | Input sequence as a workspace file of fasta data | wsid  |  |  |
| input_genome_group | Input sequence as a workspace genome group | wsid  |  |  |
| metadata | Metadata as a workspace file of csv | wsid  | :heavy_check_mark: |  |
| affiliation | Affiliation info of the submitter | string  |  |  |
| first_name | First name of the submitter | string  | :heavy_check_mark: |  |
| last_name | Last name of the submitter | string  | :heavy_check_mark: |  |
| email | Email of the submitter | string  | :heavy_check_mark: |  |
| consortium | Consortium info of the submitter | string  |  |  |
| country | Country info of the submitter | string  |  |  |
| phoneNumber | Phone number of the submitter | string  |  |  |
| street | Street of the submitter location | string  |  |  |
| postal_code | Postal code of the submitter location | string  |  |  |
| city | City of the submitter location | string  |  |  |
| state | State of the submitter location | string  |  |  |
| numberOfSequences | Number of sequences in the submission | string  |  |  |
| output_path | Output Folder | folder  | :heavy_check_mark: |  |
| output_file | File Basename | wsid  | :heavy_check_mark: |  |

