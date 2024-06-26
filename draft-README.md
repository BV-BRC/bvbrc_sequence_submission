# Sequence Submission Service

## Overview

The Sequence Submission service allows user to validate and submit virus sequences to NCBI Genbank. User-provided metadata and FASTA sequences are validated against the Genbank data submission standards to identify any sequence errors before submission. Sequences are also annotated using the VIGOR4 and FLAN annotation tools for internal use by users. The Sequence Submission service provides a validation report that should be reviewed by the user before submitting the sequences to the Genbank. 

Note: Current pipeline only supports Influenza A, B, or C virus family.



## About this module

This module is a component of the BV-BRC build system. It is designed to fit into the
`dev_container` infrastructure which manages development and production deployment of
the components of the BV-BRC. More documentation is available [here](https://github.com/BV-BRC/dev_container/tree/master/README.md).

This module provides the following application specfication(s):
* [SequenceSubmission](app_specs/SequenceSubmission.md)


## See also

* [Sequence Submission Service Quick Reference](https://www.bv-brc.org/docs/quick_references/services/sequence_submission_service.html)
  * [Sequence Submission Service](https://www.bv-brc.org/docs/https://bv-brc.org/app/SequenceSubmission.html)
  * [Sequence Submission Service Tutorial](https://www.bv-brc.org/docs//tutorial/sequence_submission/sequence_submission.html)



## References

