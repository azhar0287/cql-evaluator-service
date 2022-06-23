library CWP_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Encounter version '1.1.0' called Encounters
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_Claims version '1.1.0' called Claims
include NCQA_Terminology version '1.1.0' called Terminology

valueset "Comorbid Conditions": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1066'
valueset "Competing Diagnosis": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1067'
valueset "COPD": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1053'
valueset "CWP Antibiotic Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1734'
valueset "Disorders of the Immune System": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1139'
valueset "ED": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1086'
valueset "Emphysema": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1091'
valueset "Group A Strep Tests": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1109'
valueset "HIV": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1110'
valueset "HIV Type 2": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1406'
valueset "Malignant Neoplasms": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1167'
valueset "Observation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1191'
valueset "Online Assessments": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1446'
valueset "Other Malignant Neoplasm of Skin": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1463'
valueset "Outpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1202'
valueset "Pharyngitis": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1210'
valueset "Telephone Visits": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1246'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population":
  "Qualifying Episodes Without Exclusions"

define "Qualifying Episodes Without Exclusions":
  ( FHIRBase."First Dates per 31 Day Periods" ( "Episode Without Exclusions" ) ) DeduplicatedEpisodes
    return DeduplicatedEpisodes.NewList

define "Episode Without Exclusions":
  ( "Episode Date" eDate
    where not ( "Has Encounter with Inpatient Stay"(eDate)
        or "Has Comorbid Condition History"(eDate)
        or "Has Medication History"(eDate)
        or "Has Competing Diagnosis History"(eDate)
    ) ) episodeWithoutExclusions
    return date from start of episodeWithoutExclusions
    sort asc

define "Episode Date":
  "Encounter with Pharyngitis and Antibiotic" AllowablePharynEncounter
    where AgeInYearsAt(date from start of AllowablePharynEncounter)>= 3
      and ( Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval[date from ( start of AllowablePharynEncounter - 30 days ), date from ( start of AllowablePharynEncounter + 3 days )], 0 ) )
      and ( Enrollment."Pharmacy Benefit Enrollment Criteria" ( "Member Coverage", null, Interval[date from ( start of AllowablePharynEncounter - 30 days ), date from ( start of AllowablePharynEncounter + 3 days )], 0 ) )

define "Encounter with Pharyngitis and Antibiotic":
  "Encounter with Pharyngitis" PharynClaim
    where exists "Claim with Antibiotic Medication" AbxClaim
      where start of AbxClaim during day of Interval[start of PharynClaim, start of PharynClaim + 3 days]

define "Claim with Antibiotic Medication":
  ( ( flatten ( ( ( Claims."Pharmacy Claim With Medication" ( "Member Claims", "Antibiotic Medication" ) ) AntibioticClaims
      return AntibioticClaims.ServicePeriod ) OutpatientClaims
  ) ) OutpatientClaim
    where date from start of OutpatientClaim during "Intake Period" ) PharynClaimsList
    where PharynClaimsList is not null

define "Encounter with Pharyngitis":
  ( ( flatten ( ( ( Claims."Medical Claims With Diagnosis and Procedure" ( "Member Claims", "Pharyngitis Diagnosis", "Outpatient Encounters" ) ) PharynClaims
      return PharynClaims.ServicePeriod ) OutpatientClaims
  ) ) OutpatientClaim
    where date from start of OutpatientClaim during "Intake Period" ) PharynClaimsList
    where PharynClaimsList is not null

define function "Has Encounter with Inpatient Stay"(PharynepisodeDate Interval<System.DateTime>):
  exists ( ( Claims."Medical Claims With Nonacute or Acute Inpatient Discharge" ( "Member Claims" ) ) InpatientClaims
    return InpatientClaims.InpatientDischarge ) InpatientDischargeClaim
    where exists InpatientDischargeClaim.item InpatientDischargeLineItem
      where date from 
      end of PharynepisodeDate during Interval[date from start of FHIRBase."Normalize Interval" ( InpatientDischargeLineItem.serviced ) - 1 day, date from 
      end of FHIRBase."Normalize Interval" ( InpatientDischargeLineItem.serviced )]

define "Pharyngitis Diagnosis":
  FHIRBase."VS Cast Function" ( "Pharyngitis" )

define "Antibiotic Medication":
  FHIRBase."VS Cast Function" ( "CWP Antibiotic Medications" )

define "Outpatient Encounters":
  FHIRBase."VS Cast Function" ( "Outpatient" )
    union FHIRBase."VS Cast Function" ( "Observation" )
    union FHIRBase."VS Cast Function" ( "ED" )
    union FHIRBase."VS Cast Function" ( "Telephone Visits" )
    union FHIRBase."VS Cast Function" ( "Online Assessments" )

define "Intake Period":
  Interval["July 1 of Year Prior to Measurement Period", "June 30 of Measurement Period"]

define function "Has Comorbid Condition History"(PharynepisodeDate Interval<System.DateTime>):
  ( Claims."Medical Claims With Diagnosis" ( "Member Claims", "Comorbid Conditions Diagnosis" ) ) comorbidClaimWithEpisode
    return exists comorbidClaimWithEpisode.ServicePeriod comorbidClaimServicePeriod
      where date from start of comorbidClaimServicePeriod during Interval[date from start of PharynepisodeDate - 12 months, date from start of PharynepisodeDate]

define "Comorbid Conditions Diagnosis":
  FHIRBase."VS Cast Function" ( "HIV" )
    union FHIRBase."VS Cast Function" ( "HIV Type 2" )
    union FHIRBase."VS Cast Function" ( "Malignant Neoplasms" )
    union FHIRBase."VS Cast Function" ( "Other Malignant Neoplasm of Skin" )
    union FHIRBase."VS Cast Function" ( "Emphysema" )
    union FHIRBase."VS Cast Function" ( "COPD" )
    union FHIRBase."VS Cast Function" ( "Disorders of the Immune System" )
    union FHIRBase."VS Cast Function" ( "Comorbid Conditions" )

define function "Has Medication History"(PharynepisodeDate Interval<System.DateTime>):
  "Has Antibiotic Dispensed During 30 Days Prior"(PharynepisodeDate)
    or "Has Antibiotic Dispensed Overlapping Episode and Starts More than 30 Days Prior to Episode"(PharynepisodeDate)

define function "Has Antibiotic Dispensed During 30 Days Prior"(PharynepisodeDate Interval<System.DateTime>):
  exists ( Claims."Pharmacy Claim With Medication" ( "Member Claims", "Antibiotic Medication" ) ) antibioticClaimWithEpisode
    where exists antibioticClaimWithEpisode.ServicePeriod antibioticClaimServicePeriod
      where date from start of antibioticClaimServicePeriod during Interval[date from start of PharynepisodeDate - 30 days, date from start of PharynepisodeDate - 1 day]

define function "Has Antibiotic Dispensed Overlapping Episode and Starts More than 30 Days Prior to Episode"(PharynepisodeDate Interval<System.DateTime>):
  exists ( Claims."Pharmacy Claim With Medication" ( "Member Claims", "Antibiotic Medication" ) ) antibioticClaimWithEpisode
    where exists antibioticClaimWithEpisode.CoveredDays claimMedicationDuration
      where date from start of PharynepisodeDate during claimMedicationDuration
        and date from start of claimMedicationDuration 31 days or more before date from start of PharynepisodeDate

define function "Has Competing Diagnosis History"(PharynepisodeDate Interval<System.DateTime>):
  ( Claims."Medical Claims With Diagnosis" ( "Member Claims", "Competing Condition Diagnosis" ) ) competingClaimWithEpisode
    return exists ( competingClaimWithEpisode.ServicePeriod cmClaim
        where date from start of cmClaim during Interval[date from start of PharynepisodeDate, date from start of PharynepisodeDate + 3 days]
    )

define "Competing Condition Diagnosis":
  FHIRBase."VS Cast Function" ( "Competing Diagnosis" )

define "Denominator":
  "Initial Population"

define "Exclusions":
  "Episodes with Hospice Intervention or Encounter"

define "Episodes with Hospice Intervention or Encounter":
  "Initial Population" IP
    where Hospice."Hospice Intervention or Encounter"

define "Numerator":
  "Initial Population" IP
    with ["Observation": "Group A Strep Tests"] StrepTest
      such that date from start of FHIRBase."Normalize Interval" ( StrepTest.effective ) during Interval[IP - 3 days, IP + 3 days]

define "July 1 of Year Prior to Measurement Period":
  DateTime(((year from start of "Measurement Period")- 1), 7, 1, 0, 0, 0, 0, 0)

define "June 30 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 6, 30, 23, 59, 59, 0, 0)

define "Member Claims":
  [Claim] C
    where exists C.item CItem
      where FHIRBase."Normalize Interval" ( CItem.serviced ) starts during Interval[start of "Intake Period" - 1 year, 
      end of "Measurement Period"]

define "Member Coverage":
  [Coverage] C
    where Interval[date from start of FHIRBase."Normalize Interval" ( C.period ), date from 
    end of FHIRBase."Normalize Interval" ( C.period )]overlaps Interval[date from start of "Intake Period" - 30 days, date from 
    end of "Intake Period" + 3 days]