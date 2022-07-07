library IMAE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Immunization version '1.1.0' called Immunization
include NCQA_Status version '1.1.0' called Status

codesystem "SNOMEDCT": 'http://snomed.info/sct'

valueset "HPV Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1763'
valueset "Meningococcal Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1777'
valueset "Tdap Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1791'
valueset "HPV Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1764'
valueset "Meningococcal Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1778'
valueset "Tdap Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1792'
valueset "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2240'
valueset "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2241'

code "Anaphylaxis due to meningococcal vaccine (disorder)": '428301000124106' from "SNOMEDCT" display 'Anaphylaxis due to meningococcal vaccine (disorder)'
code "Anaphylaxis due to human papillomavirus vaccine (disorder)": '428241000124101' from "SNOMEDCT" display 'Anaphylaxis due to human papillomavirus vaccine (disorder)'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population 1":
  AgeInYearsAt(date from 
    end of "Measurement Period"
  )= 13
    and "Enrolled During Participation Period"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", "Date of Thirteenth Birthday", "Participation Period", 45 )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Participation Period"

define "Date of Thirteenth Birthday":
  Patient.birthDate + 13 years

define "Participation Period":
  Interval[Patient.birthDate + 12 years, Patient.birthDate + 13 years]

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Initial Population 4":
  "Initial Population 1"

define "Initial Population 5":
  "Initial Population 1"

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

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Exclusions 4":
  "Exclusions 1"

define "Exclusions 5":
  "Exclusions 1"

define "Numerator 1":
  "Meets Meningococcal Vaccination Criteria"
    or "Has Anaphylaxis Due to Meningococcal Vaccine"

define "Meets Meningococcal Vaccination Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Meningococcal Vaccinations", "Vaccine Administration Interval: Between Age 11 and 13", 1 )

define "Meningococcal Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Meningococcal Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Meningococcal Vaccine Procedure"] )

define "Vaccine Administration Interval: Between Age 11 and 13":
  Interval[Patient.birthDate + 11 years, "Date of Thirteenth Birthday"]

define "Has Anaphylaxis Due to Meningococcal Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis due to meningococcal vaccine (disorder)"] ), "Date of Thirteenth Birthday" )

define "Numerator 2":
  "Meets Tdap Vaccination Criteria"
    or "Has Anaphylaxis Due to Tdap Vaccine"
    or "Has Encephalitis Due to Tdap Vaccine"

define "Meets Tdap Vaccination Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Tdap Vaccinations", "Vaccine Administration Interval: Between Age 10 and 13", 1 )

define "Tdap Vaccinations":
  Status."Completed Immunization" ( [Immunization: "Tdap Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Tdap Vaccine Procedure"] )

define "Vaccine Administration Interval: Between Age 10 and 13":
  Interval[Patient.birthDate + 10 years, "Date of Thirteenth Birthday"]

define "Has Anaphylaxis Due to Tdap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), "Date of Thirteenth Birthday" )

define "Has Encephalitis Due to Tdap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), "Date of Thirteenth Birthday" )

define "Numerator 3":
  "Meets HPV Vaccinations Criteria"
    or "Has Anaphylaxis Due to HPV Vaccine"

define "Meets HPV Vaccinations Criteria":
  Immunization."Meets Required Vaccination Threshold With 14 Day Rule" ( "HPV Vaccinations", "Vaccine Administration Interval: Between Age 9 and 13", 3 )
    or "Meets 2 Dose HPV Vaccination Criteria"

define "HPV Vaccinations":
  Status."Completed Immunization" ( [Immunization: "HPV Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "HPV Vaccine Procedure"] )

define "Vaccine Administration Interval: Between Age 9 and 13":
  Interval[Patient.birthDate + 9 years, "Date of Thirteenth Birthday"]

define "Meets 2 Dose HPV Vaccination Criteria":
  Immunization."Meets Required Threshold With Variable Days Apart" ( "HPV Vaccinations", "Vaccine Administration Interval: Between Age 9 and 13", 2, 146 )

define "Has Anaphylaxis Due to HPV Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis due to human papillomavirus vaccine (disorder)"] ), "Date of Thirteenth Birthday" )

define "Numerator 4":
  "Numerator 1"
    and "Numerator 2"

define "Numerator 5":
  "Numerator 1"
    and "Numerator 2"
    and "Numerator 3"