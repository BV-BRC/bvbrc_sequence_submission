Submit-block ::= {
  contact {
    contact {
      name name {
        last "Support",
        first "BV-BRC"
      },
      affil std {
        affil "Bacterial and Viral Bioinformatics Resource Center (BV-BRC)",
        div "University of Chicago",
        city "Chicago",
        sub "IL",
        country "USA",
        email "gbsubmit@bvbrc.gov",
        postal-code "60637"
      }
    }
  },
  cit {
    authors {
      names std {
        %cit_authors_names%  
      },
      affil std {
        affil "%authors_affil%"
      }
    }
  },
  subtype new
}
Seqdesc ::= pub {
  pub {
    gen {
      cit "unpublished",
      authors {
        names std {
          %cit_authors_names%
        }
      },
      title "Direct Submission (BVBRC)"
    }
  }
}
Seqdesc ::= user {
  type str "Submission",
  data {
    {
      label str "AdditionalComment",
      data str "ALT EMAIL:gbsubmit@bvbrc.gov"
    }
  }
}
Seqdesc ::= user {
  type str "Submission",
  data {
    {
      label str "AdditionalComment",
      data str "Submission Title:Direct Submission (BVBRC)"
    }
  }
}

