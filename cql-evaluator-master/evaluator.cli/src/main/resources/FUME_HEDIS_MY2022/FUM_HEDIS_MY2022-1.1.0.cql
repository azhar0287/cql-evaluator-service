library FUM_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Encounter version '1.1.0' called Encounters
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Status version '1.1.0' called Status
include NCQA_Terminology version '1.1.0' called Terminology
include NCQA_Claims version '1.1.0' called Claims

valueset "BH Outpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1481'
valueset "ED": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1086'
valueset "Electroconvulsive Therapy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1294'
valueset "Inpatient Stay": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1395'
valueset "Intentional Self-Harm": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1468'
valueset "Mental Health Diagnosis": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1178'
valueset "Mental Illness": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1179'
valueset "Observation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1191'
valueset "Online Assessments": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1446'
valueset "Partial Hospitalization or Intensive Outpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1492'
valueset "Telephone Visits": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1246'
valueset "Visit Setting Unspecified": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1493'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population 1":
  "First Eligible ED Visits per 31 Day Period"

define "First Eligible ED Visits per 31 Day Period":
  ( FHIRBase."First Dates per 31 Day Periods" ( "Eligible ED Visits Not Followed by Inpatient Admission" ) ) DeduplicatedVisits
    return DeduplicatedVisits.NewList

define "Eligible ED Visits Not Followed by Inpatient Admission":
  ( "Eligible ED Visits" EDVisits
    where not "ED Visits Followed by Inpatient Admission"(EDVisits)) EDVisitsWithoutExclusions
    return all date from 
    end of EDVisitsWithoutExclusions
    sort asc

define "Eligible ED Visits":
  "ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm" EDVisits
    where AgeInYearsAt(date from 
      end of EDVisits
    )>= 6
      and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval[date from 
      end of EDVisits, date from 
      end of EDVisits + 30 days], 0 )
      and Enrollment."Mental Health Benefit Enrollment Criteria" ( "Member Coverage", null, Interval[date from 
      end of EDVisits, date from 
      end of EDVisits + 30 days], 0 )

define "ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm":
  ( flatten ( ( Claims."Medical Claims With Principal Diagnosis and Procedure" ( "Member Claims", "Mental Illness or Intentional Self-Harm", FHIRBase."VS Cast Function" ( "ED" ) ) ) ClaimList
      return ClaimList.ServicePeriod
  ) ) EDVisits
    where date from 
    end of EDVisits during Interval[date from start of "Measurement Period", date from "December 1 of the Measurement Period"]

define "Member Claims":
  [Claim] C
    where exists C.item CItem
      where FHIRBase."Normalize Interval" ( CItem.serviced ) ends during Interval[start of "Measurement Period", 
      end of "Measurement Period"]

define "Mental Illness or Intentional Self-Harm":
  FHIRBase."VS Cast Function" ( "Mental Illness" )
    union FHIRBase."VS Cast Function" ( "Intentional Self-Harm" )

define "December 1 of the Measurement Period":
  DateTime((year from start of "Measurement Period"), 12, 1, 23, 59, 59, 0, 0)

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"

define function "ED Visits Followed by Inpatient Admission"(EDVisit Interval<System.DateTime>):
  exists ( ( "Inpatient Stay Service Time"("Member Claims")) ClaimList
    return all ClaimList ) InpatientStay
    where date from start of InpatientStay during Interval[date from 
    end of EDVisit, date from 
    end of EDVisit + 30 days]

define function "Inpatient Stay Service Time"(claim List<FHIR.Claim>):
  ( flatten ( ( Claims."Medical Claims With Nonacute or Acute Inpatient Discharge" ( claim ).InpatientDischarge ) InpatientClaim
      return InpatientClaim.item
  ) ) ItemOnLine
    return if ( exists ( FHIRHelpers.ToConcept ( ItemOnLine.revenue ).codes ) rev
        where rev.code in "Inpatient Stay"
    ) then FHIRBase."Normalize Interval" ( ItemOnLine.serviced ) 
      else null

define "Initial Population 2":
  "Initial Population 1"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 1"

define "Exclusions 1":
  "ED Visits with Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "ED Visits with Hospice Intervention or Encounter":
  "Initial Population 1" EDVisits
    where Hospice."Hospice Intervention or Encounter"

define "Numerator 1":
  "Initial Population 1" EDVisits
    with "Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm" FollowUpVisits
      such that date from FollowUpVisits during Interval[date from EDVisits, date from EDVisits + 30 days]

define "Numerator 2":
  "Initial Population 2" EDVisits
    with "Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm" FollowUpVisits
      such that date from FollowUpVisits during Interval[date from EDVisits, date from EDVisits + 7 days]

define "Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm":
  ( "Follow Up Visits" Enc
    where ( Encounters."Encounter Has Principal Diagnosis" ( Enc, [Condition: "Mental Health Diagnosis"] )
        or ( Encounters."Encounter Has Principal Diagnosis" ( Enc, [Condition: "Intentional Self-Harm"] )
            and Encounters."Encounter Has Diagnosis" ( Enc, [Condition: "Mental Health Diagnosis"] )
        )
    ) ) FollowUpVisits
    return date from start of FHIRBase."Normalize Interval" ( FollowUpVisits.period )

define "Follow Up Visits":
  Status."Finished Encounter" ( [Encounter: "BH Outpatient"]
    union [Encounter: "Partial Hospitalization or Intensive Outpatient"]
    union [Encounter: "Observation"]
    union [Encounter: "Telephone Visits"]
    union [Encounter: "Online Assessments"] )
    union Encounters."Finished Encounter with Ambulatory POS" ( [Encounter: "Electroconvulsive Therapy"] )
    union Encounters."Finished Encounter with Telehealth POS" ( [Encounter: "Visit Setting Unspecified"] )
    union Encounters."Finished Encounter with Outpatient POS" ( [Encounter: "Visit Setting Unspecified"] )