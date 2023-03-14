Submit-block ::= {
  contact {
    contact {
      name name {
        last "Support",
        first "BV-BRC",
        middle "",
        initials "",
        suffix "",
        title ""
      },
      affil std {
        affil "Bacterial and Viral Bioinformatics Resource Center (BV-BRC)",
        div "University of Chicago",
        city "Chicago",
        sub "IL",
        country "USA",
        street "5801 S Ellis Ave",
        email "gbsubmit@bvbrc.org",
        postal-code "60637"
      }
    }
  },
  cit {
    authors {
      names std {
        %cit_authors_names%  
      }%authors_affil%
    }
  },
  subtype new
}
Seqdesc ::= pub {
  pub {
    %pub_info% 
  }
}
Seqdesc ::= user {
  type str "Submission",
  data {
    {
      label str "AdditionalComment",
      data str "ALT EMAIL:gbsubmit@bvbrc.org"
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

