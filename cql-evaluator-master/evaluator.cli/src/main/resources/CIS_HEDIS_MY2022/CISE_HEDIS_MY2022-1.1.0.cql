library CISE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Immunization version '1.1.0' called Immunization
include NCQA_Status version '1.1.0' called Status

codesystem "SNOMEDCT": 'http://snomed.info/sct'

valueset "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2240'
valueset "Disorders of the Immune System": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1139'
valueset "DTaP Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1744'
valueset "DTaP Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1745'
valueset "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2241'
valueset "Haemophilus Influenzae Type B (HiB) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1753'
valueset "Haemophilus Influenzae Type B (HiB) Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1754'
valueset "Hepatitis A": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1117'
valueset "Hepatitis A Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1757'
valueset "Hepatitis A Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1758'
valueset "Hepatitis B": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1266'
valueset "Hepatitis B Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1759'
valueset "Hepatitis B Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1760'
valueset "HIV": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1110'
valueset "HIV Type 2": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1406'
valueset "Inactivated Polio Vaccine (IPV) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1765'
valueset "Inactivated Polio Vaccine (IPV) Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1766'
valueset "Influenza Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1767'
valueset "Influenza Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1768'
valueset "Influenza Virus LAIV Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1974'
valueset "Influenza Virus LAIV Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1973'
valueset "Intussusception": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1415'
valueset "Malignant Neoplasm of Lymphatic Tissue": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1319'
valueset "Measles": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1171'
valueset "Measles, Mumps and Rubella (MMR) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1773'
valueset "Measles, Mumps and Rubella (MMR) Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1774'
valueset "Mumps": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1181'
valueset "Newborn Hepatitis B Vaccine Administered": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1397'
valueset "Pneumococcal Conjugate Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1781'
valueset "Pneumococcal Conjugate Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1782'
valueset "Rotavirus (2 Dose Schedule) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1785'
valueset "Rotavirus (3 Dose Schedule) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1786'
valueset "Rotavirus Vaccine (2 Dose Schedule) Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1787'
valueset "Rotavirus Vaccine (3 Dose Schedule) Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1788'
valueset "Rubella": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1232'
valueset "Severe Combined Immunodeficiency": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1416'
valueset "Varicella Zoster": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1258'
valueset "Varicella Zoster (VZV) Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1793'
valueset "Varicella Zoster (VZV) Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1794'

code "Anaphylaxis due to Haemophilus influenzae type b vaccine (disorder)": '433621000124101' from "SNOMEDCT" display 'Anaphylaxis due to Haemophilus influenzae type b vaccine (disorder)'
code "Anaphylaxis due to Hepatitis B vaccine (disorder)": '428321000124101' from "SNOMEDCT" display 'Anaphylaxis due to Hepatitis B vaccine (disorder)'
code "Anaphylaxis due to rotavirus vaccine (disorder)": '428331000124103' from "SNOMEDCT" display 'Anaphylaxis due to rotavirus vaccine (disorder)'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)


context Patient

define "Date of First Birthday":
  Patient.birthDate + 1 year

define "Date of Second Birthday":
  Patient.birthDate + 2 years

define "Vaccine Administration Interval - On or Between 42 Days and Second Birthday":
  Interval[Patient.birthDate + 42 days, "Date of Second Birthday"]

define "Vaccine Administration Interval - On or Between 6 Months and Second Birthday":
  Interval[Patient.birthDate + 6 months, "Date of Second Birthday"]

define "Vaccine Administration Interval - On or Between First Birthday and Second Birthday":
  "Date of First Birthday to Date of Second Birthday"

define "Participation Period":
  "Date of First Birthday to Date of Second Birthday"

define "Date of First Birthday to Date of Second Birthday":
  Interval["Date of First Birthday", "Date of Second Birthday"]

define "Initial Population 1":
  "Enrolled During Participation Period"
    and "Date of Second Birthday" in "Measurement Period"

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Initial Population 4":
  "Initial Population 1"

define "Initial Population 5":
  "Initial Population 1"

define "Initial Population 6":
  "Initial Population 1"

define "Initial Population 7":
  "Initial Population 1"

define "Initial Population 8":
  "Initial Population 1"

define "Initial Population 9":
  "Initial Population 1"

define "Initial Population 10":
  "Initial Population 1"

define "Initial Population 11":
  "Initial Population 1"

define "Initial Population 12":
  "Initial Population 1"

define "Initial Population 13":
  "Initial Population 1"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", "Date of Second Birthday", "Participation Period", 45 )

define "Member Coverage":
  [Coverage] C
    where Interval[date from start of FHIRBase."Normalize Interval" ( C.period ), date from 
    end of FHIRBase."Normalize Interval" ( C.period )]overlaps "Participation Period"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 1"

define "Denominator 3":
  "Initial Population 1"

define "Denominator 4":
  "Initial Population 1"

define "Denominator 5":
  "Initial Population 1"

define "Denominator 6":
  "Initial Population 1"

define "Denominator 7":
  "Initial Population 1"

define "Denominator 8":
  "Initial Population 1"

define "Denominator 9":
  "Initial Population 1"

define "Denominator 10":
  "Initial Population 1"

define "Denominator 11":
  "Initial Population 1"

define "Denominator 12":
  "Initial Population 1"

define "Denominator 13":
  "Initial Population 1"

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"
    or "Has Severe Combined Immunodeficiency"
    or "Has Immunodeficiency"
    or "Has HIV"
    or "Has Lymphoreticular Cancer, Multiple Myeloma or Leukemia"
    or "Has Intussusception"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Exclusions 4":
  "Exclusions 1"

define "Exclusions 5":
  "Exclusions 1"

define "Exclusions 6":
  "Exclusions 1"

define "Exclusions 7":
  "Exclusions 1"

define "Exclusions 8":
  "Exclusions 1"

define "Exclusions 9":
  "Exclusions 1"

define "Exclusions 10":
  "Exclusions 1"

define "Exclusions 11":
  "Exclusions 1"

define "Exclusions 12":
  "Exclusions 1"

define "Exclusions 13":
  "Exclusions 1"

define "Has Severe Combined Immunodeficiency":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Severe Combined Immunodeficiency"] ), "Date of Second Birthday" )

define "Has Immunodeficiency":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Disorders of the Immune System"] ), "Date of Second Birthday" )

define "Has HIV":
  exists ( Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "HIV Type 2"] ), "Date of Second Birthday" )
      union Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "HIV"] ), "Date of Second Birthday" )
  )

define "Has Lymphoreticular Cancer, Multiple Myeloma or Leukemia":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Malignant Neoplasm of Lymphatic Tissue"] ), "Date of Second Birthday" )

define "Has Intussusception":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Intussusception"] ), "Date of Second Birthday" )

define "Numerator 1":
  "Meets DTaP Vaccination Requirement"
    or "Has Anaphylaxis Due to DTap Vaccine"
    or "Has Encephalitis Due to DTap Vaccine"

define "Meets DTaP Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "DTaP Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 4 )

define "DTaP Vaccinations":
  Status."Completed Immunization" ( [Immunization: "DTaP Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "DTaP Vaccine Procedure"] )

define "Has Anaphylaxis Due to DTap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), "Date of Second Birthday" )

define "Has Encephalitis Due to DTap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), "Date of Second Birthday" )

define "Numerator 2":
  "Meets IPV Vaccination Requirement"

define "Meets IPV Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "IPV Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 3 )

define "IPV Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Inactivated Polio Vaccine (IPV) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Inactivated Polio Vaccine (IPV) Procedure"] )

define "Numerator 3":
  "Meets MMR Vaccination Requirement"
    or "Has History of Measles, Mumps and Rubella Illness"

define "Meets MMR Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold" ( "MMR Vaccinations", "Vaccine Administration Interval - On or Between First Birthday and Second Birthday", 1 )

define "MMR Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Measles, Mumps and Rubella (MMR) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Measles, Mumps and Rubella (MMR) Vaccine Procedure"] )

define "Has History of Measles, Mumps and Rubella Illness":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Measles"] ), "Date of Second Birthday" )
    and exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Mumps"] ), "Date of Second Birthday" )
    and exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Rubella"] ), "Date of Second Birthday" )

define "Numerator 4":
  "Meets HiB Vaccination Requirement"
    or "Has Anaphylaxis Due to HiB Vaccine"

define "Meets HiB Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "HiB Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 3 )

define "HiB Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Haemophilus Influenzae Type B (HiB) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Haemophilus Influenzae Type B (HiB) Vaccine Procedure"] )

define "Has Anaphylaxis Due to HiB Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis due to Haemophilus influenzae type b vaccine (disorder)"] ), "Date of Second Birthday" )

define "Numerator 5":
  "Meets HepB Vaccination Requirement"
    or "Has Anaphylaxis Due to Hep B Vaccine or History of Hep B"

define "Meets HepB Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "HepB Vaccinations", Interval[Patient.birthDate, "Date of Second Birthday"], 3 )
    or ( Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "HepB Vaccinations", Interval[Patient.birthDate + 8 days, "Date of Second Birthday"], 2 )
        and Immunization."Meets Required Vaccination Threshold" ( "Newborn HepB Vaccinations", Interval[Patient.birthDate, Patient.birthDate + 7 days], 1 )
        and Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( ( "HepB Vaccinations"
            union "Newborn HepB Vaccinations"
        ), Interval[Patient.birthDate, "Date of Second Birthday"], 3 )
    )

define "HepB Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Hepatitis B Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Hepatitis B Vaccine Procedure"] )

define "Newborn HepB Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Newborn Hepatitis B Vaccine Administered"] )

define "Has Anaphylaxis Due to Hep B Vaccine or History of Hep B":
  exists ( Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis due to Hepatitis B vaccine (disorder)"] ), "Date of Second Birthday" )
      union Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Hepatitis B"] ), "Date of Second Birthday" )
  )

define "Numerator 6":
  "Meets VZV Vaccination Requirement"
    or "Has History of VZV"

define "Meets VZV Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold" ( "VZV Vaccinations", "Vaccine Administration Interval - On or Between First Birthday and Second Birthday", 1 )

define "VZV Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Varicella Zoster (VZV) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Varicella Zoster (VZV) Vaccine Procedure"] )

define "Has History of VZV":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Varicella Zoster"] ), "Date of Second Birthday" )

define "Numerator 7":
  "Meets PCV Vaccination Requirement"

define "Meets PCV Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "PCV Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 4 )

define "PCV Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Pneumococcal Conjugate Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Pneumococcal Conjugate Vaccine Procedure"] )

define "Numerator 8":
  "Meets HepA Vaccination Requirement"
    or "Has History of Hepatitis A"

define "Meets HepA Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold" ( "HepA Vaccinations", "Vaccine Administration Interval - On or Between First Birthday and Second Birthday", 1 )

define "HepA Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Hepatitis A Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Hepatitis A Vaccine Procedure"] )

define "Has History of Hepatitis A":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Hepatitis A"] ), "Date of Second Birthday" )

define "Numerator 9":
  "Meets Rotavirus Vaccination Requirement"
    or "Has Anaphylaxis Due to Rotavirus Vaccine"

define "Meets Rotavirus Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "RV 2 Dose Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 2 )
    or Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "RV 3 Dose Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 3 )
    or ( Immunization."Meets Required Vaccination Threshold" ( "RV 2 Dose Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 1 )
        and Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "RV 3 Dose Vaccinations", "Vaccine Administration Interval - On or Between 42 Days and Second Birthday", 2 )
    )

define "RV 2 Dose Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Rotavirus (2 Dose Schedule) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Rotavirus Vaccine (2 Dose Schedule) Procedure"] )

define "RV 3 Dose Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Rotavirus (3 Dose Schedule) Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Rotavirus Vaccine (3 Dose Schedule) Procedure"] )

define "Has Anaphylaxis Due to Rotavirus Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis due to rotavirus vaccine (disorder)"] ), "Date of Second Birthday" )

define "Numerator 10":
  "Meets Influenza Vaccination Requirement"

define "Meets Influenza Vaccination Requirement":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "Influenza Vaccinations", "Vaccine Administration Interval - On or Between 6 Months and Second Birthday", 2 )
    or ( Immunization."Meets Required Vaccination Threshold" ( "Influenza Vaccinations", "Vaccine Administration Interval - On or Between 6 Months and Second Birthday", 1 )
        and Immunization."Meets Required Vaccination Threshold" ( "LAIV Vaccinations", Interval["Date of Second Birthday", "Date of Second Birthday"], 1 )
        and Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( ( "LAIV Vaccinations"
            union "Influenza Vaccinations"
        ), "Vaccine Administration Interval - On or Between 6 Months and Second Birthday", 2 )
    )

define "Influenza Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Influenza Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Influenza Vaccine Procedure"] )

define "LAIV Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Influenza Virus LAIV Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Influenza Virus LAIV Vaccine Procedure"] )

define "Numerator 11":
  "Numerator 1"
    and "Numerator 2"
    and "Numerator 3"
    and "Numerator 4"
    and "Numerator 5"
    and "Numerator 6"
    and "Numerator 7"

define "Numerator 12":
  "Numerator 1"
    and "Numerator 2"
    and "Numerator 3"
    and "Numerator 4"
    and "Numerator 5"
    and "Numerator 6"
    and "Numerator 7"
    and "Numerator 8"
    and "Numerator 9"

define "Numerator 13":
  "Numerator 1"
    and "Numerator 2"
    and "Numerator 3"
    and "Numerator 4"
    and "Numerator 5"
    and "Numerator 6"
    and "Numerator 7"
    and "Numerator 8"
    and "Numerator 9"
    and "Numerator 10"