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
        affil "%affiliation%",
        div "",
        city "%city%",
        sub "%state%",
        country "%country%",
        street "%street%",
        email "gbsubmit@bvbrc.org",
        postal-code "%zipcode%"
      }
    }
  },
  cit {
    authors {
      names std {
        %cit_authors_names%  
      },
      affil std {
        affil "%affiliation%",
        div "",
        city "%city%",
        sub "%state%",
        country "%country%",
        street "%street%",
        postal-code "%zipcode%"
      }
    }
  },
  subtype new
}
Seqdesc ::= pub {
  pub {
    %pub_info% 
  }
}
Seqdesc ::= comment "This submission was made by the Bacterial and Viral Bioinformatics Resource Center (BV-BRC) on behalf of %affiliation%. This work was supported by the National Institute of Allergy and Infectious Diseases, National Institutes of Health, Department of Health and Human Services, under Contract No. 75N93019C00076, awarded to the University of Chicago."
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

