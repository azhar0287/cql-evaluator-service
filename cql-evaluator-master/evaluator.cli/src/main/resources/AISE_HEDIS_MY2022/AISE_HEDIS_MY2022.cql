library AISE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Immunization version '1.1.0' called Immunization
include NCQA_Status version '1.1.0' called Status

codesystem "ICD-10": 'http://hl7.org/fhir/sid/icd-10-cm'

valueset "Adult Influenza Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1913'
valueset "Adult Influenza Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1914'
valueset "Anatomic or Functional Asplenia": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1477'
valueset "Bone Marrow Transplant": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1325'
valueset "Cerebrospinal Fluid Leak": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1448'
valueset "Chemotherapy Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1519'
valueset "Chemotherapy Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1500'
valueset "Cochlear Implant": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1447'
valueset "Cochlear Implant Device": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1521'
valueset "Cochlear Implant Diagnosis": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1520'
valueset "Herpes Zoster Live Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1915'
valueset "Herpes Zoster Live Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1917'
valueset "Herpes Zoster Recombinant Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1916'
valueset "Herpes Zoster Recombinant Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1918'
valueset "Immunocompromising Conditions": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1502'
valueset "Influenza Virus LAIV Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1974'
valueset "Influenza Virus LAIV Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1973'
valueset "Pneumococcal Polysaccharide 23 Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1921'
valueset "Pneumococcal Polysaccharide 23 Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1922'
valueset "Sickle Cell Anemia and HB S Disease": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1373'
valueset "Td Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1923'
valueset "Td Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1924'
valueset "Tdap Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1791'
valueset "Tdap Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1792'
valueset "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2240'
valueset "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2241'

code "Encounter for antineoplastic radiation therapy": 'Z51.0' from "ICD-10" display 'Encounter for antineoplastic radiation therapy'
code "Encounter for antineoplastic chemotherapy": 'Z51.11' from "ICD-10" display 'Encounter for antineoplastic chemotherapy'
code "Encounter for antineoplastic immunotherapy": 'Z51.12' from "ICD-10" display 'Encounter for antineoplastic immunotherapy'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.000Z, @2022-12-31T23:59:59.000Z)

context Patient

define "Initial Population 1":
  AgeInYearsAt(date from start of "Measurement Period")>= 19
    and "Enrolled During Participation Period"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
  end of "Measurement Period"], 45 )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  AgeInYearsAt(date from start of "Measurement Period")>= 50
    and "Enrolled During Participation Period"

define "Initial Population 4":
  AgeInYearsAt(date from start of "Measurement Period")>= 66
    and "Enrolled During Participation Period"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 1"

define "Denominator 3":
  "Initial Population 3"

define "Denominator 4":
  "Initial Population 4"

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"
    or "Has Active Chemotherapy"
    or "Has Bone Marrow Transplant"
    or "Has Immunocompromising Conditions"

define "Has Active Chemotherapy":
  exists ( ( Status."Completed Procedure" ( [Procedure: "Chemotherapy Procedure"] ) ) ChemotherapyProcedure
      where FHIRBase."Normalize Interval" ( ChemotherapyProcedure.performed ) overlaps "Measurement Period"
  )
    or exists ( ( Status."Finished Encounter" ( [Encounter: "Chemotherapy Encounter"] ) ) ChemotherapyEncounter
        where FHIRBase."Normalize Interval" ( ChemotherapyEncounter.period ) overlaps "Measurement Period"
    )
    or exists ( ( Status."Active Condition" ( [Condition: "Encounter for antineoplastic radiation therapy"] )
        union Status."Active Condition" ( [Condition: "Encounter for antineoplastic chemotherapy"] )
        union Status."Active Condition" ( [Condition: "Encounter for antineoplastic immunotherapy"] ) ) AntineoplasticTherapyEncounter
        where FHIRBase."Prevalence Period" ( AntineoplasticTherapyEncounter ) overlaps "Measurement Period"
    )

define "Has Bone Marrow Transplant":
  exists ( ( Status."Completed Procedure" ( [Procedure: "Bone Marrow Transplant"] ) ) BoneMarrowTransplant
      where FHIRBase."Normalize Interval" ( BoneMarrowTransplant.performed ) overlaps "Measurement Period"
  )

define "Has Immunocompromising Conditions":
  exists ( ( Status."Active Condition" ( [Condition: "Immunocompromising Conditions"] )
      union Status."Active Condition" ( [Condition: "Cochlear Implant Diagnosis"] )
      union Status."Active Condition" ( [Condition: "Anatomic or Functional Asplenia"] )
      union Status."Active Condition" ( [Condition: "Cerebrospinal Fluid Leak"] )
      union Status."Active Condition" ( [Condition: "Sickle Cell Anemia and HB S Disease"] ) ) ExclusionDiagnoses
      where FHIRBase."Prevalence Period" ( ExclusionDiagnoses ) starts on or before 
      end of "Measurement Period"
  )
    or exists ( ( Status."Completed Procedure" ( [Procedure: "Cochlear Implant"] )
        union Status."Completed Procedure" ( [Procedure: "Cochlear Implant Device"] ) ) CochlearImplantProcedure
        where FHIRBase."Normalize Interval" ( CochlearImplantProcedure.performed ) starts on or before 
        end of "Measurement Period"
    )

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Exclusions 4":
  "Exclusions 1"

define "Numerator 1":
  "Meets Influenza Vaccination Criteria"

define "June 30 of the Measurement Period":
  DateTime((year from start of "Measurement Period"), 6, 30, 23, 59, 0, 0, 0)

define "July 1 of the Year Prior to the Measurement Period":
  DateTime((year from start of "Measurement Period" - 1), 7, 1, 0, 0, 0, 0, 0)

define "Influenza Vaccine":
  Status."Completed Immunization" ( [Immunization: "Adult Influenza Immunization"] )
    union Status."Completed Immunization" ( [Immunization: "Influenza Virus LAIV Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Adult Influenza Vaccine Procedure"] )
    union Status."Completed Procedure" ( [Procedure: "Influenza Virus LAIV Vaccine Procedure"] )

define "Between July 1 of the Year Prior to the Measurement Period and June 30 of the Measurement Period":
  Interval[date from "July 1 of the Year Prior to the Measurement Period", date from "June 30 of the Measurement Period"]

define "Meets Influenza Vaccination Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Influenza Vaccine", "Between July 1 of the Year Prior to the Measurement Period and June 30 of the Measurement Period", 1 )

define "Numerator 2":
  "Meets Td or Tdap Vaccination Criteria"
    or "Has Anaphylaxis Due to Td or Tdap Vaccine"
    or "Has Encephalitis Due to Td or Tdap Vaccine"

define "Td or Tdap Vaccine":
  Status."Completed Immunization" ( [Immunization: "Td Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Td Vaccine Procedure"] )
    union Status."Completed Immunization" ( [Immunization: "Tdap Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Tdap Vaccine Procedure"] )

define "Between Nine Years Prior to the Start of the Measurement Period and the End of the Measurement Period":
  Interval[( date from start of "Measurement Period" - 9 years ), ( date from 
    end of "Measurement Period"
  )]

define "Meets Td or Tdap Vaccination Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Td or Tdap Vaccine", "Between Nine Years Prior to the Start of the Measurement Period and the End of the Measurement Period", 1 )

define "Has Anaphylaxis Due to Td or Tdap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), date from 
  end of "Measurement Period" )

define "Has Encephalitis Due to Td or Tdap Vaccine":
  exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( Status."Active Condition" ( [Condition: "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine"] ), date from 
  end of "Measurement Period" )

define "Numerator 3":
  "Meets Herpes Zoster Live Vaccination Criteria"
    or "Meets Herpes Zoster Recombinant Vaccination Criteria"

define "Meets Herpes Zoster Live Vaccination Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Herpes Zoster Live Vaccine", "Between Age 50 and the End of the Measurement Period", 1 )

define "Herpes Zoster Live Vaccine":
  Status."Completed Immunization" ( [Immunization: "Herpes Zoster Live Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Herpes Zoster Live Vaccine Procedure"] )

define "Between Age 50 and the End of the Measurement Period":
  Interval[Patient.birthDate + 50 years, date from 
  end of "Measurement Period"]

define "Meets Herpes Zoster Recombinant Vaccination Criteria":
  Immunization."Meets Required Threshold With Variable Days Apart" ( "Herpes Zoster Recombinant Vaccine", "Between Age 50 and the End of the Measurement Period", 2, 28 )

define "Herpes Zoster Recombinant Vaccine":
  Status."Completed Immunization" ( [Immunization: "Herpes Zoster Recombinant Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Herpes Zoster Recombinant Vaccine Procedure"] )

define "Numerator 4":
  "Meets Pneumococcal Polysaccharide Vaccine 23 Criteria"

define "Meets Pneumococcal Polysaccharide Vaccine 23 Criteria":
  Immunization."Meets Required Vaccination Threshold" ( "Pneumococcal Polysaccharide Vaccine 23", "Between Age 60 and the End of the Measurement Period", 1 )

define "Pneumococcal Polysaccharide Vaccine 23":
  Status."Completed Immunization" ( [Immunization: "Pneumococcal Polysaccharide 23 Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Pneumococcal Polysaccharide 23 Vaccine Procedure"] )

define "Between Age 60 and the End of the Measurement Period":
  Interval[Patient.birthDate + 60 years, date from 
  end of "Measurement Period"]