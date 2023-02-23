Submit-block ::= {
  contact {
    contact {
      name name {
        last "Doe",
        first "John",
        middle "",
        initials "",
        suffix "",
        title ""
      },
      affil std {
        affil "NIH",
        div "NCBI",
        city "Bethesda",
        sub "MD",
        country "USA",
        street "10 Center Dr",
        email "jdoe@nih.gov",
        phone "301-402-8219",
        postal-code "20895"
      }
    }
  },
  cit {
    authors {
      names std {
        %cit_authors_names%  
      },
      affil std {
        %cit_authors_affil%
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
      data str "ALT EMAIL:jdoe@nih.gov"
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

