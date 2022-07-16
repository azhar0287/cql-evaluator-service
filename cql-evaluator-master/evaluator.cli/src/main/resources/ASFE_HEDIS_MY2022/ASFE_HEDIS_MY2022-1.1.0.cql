library ASFE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Status version '1.1.0' called Status

codesystem "ICD-10": 'http://hl7.org/fhir/sid/icd-10-cm'
codesystem "LOINC": 'http://loinc.org'

valueset "Alcohol Counseling or Other Follow Up Care": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1437'
valueset "Alcohol Use Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1339'
valueset "Dementia": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1074'

code "Alcohol abuse counseling and surveillance of alcoholic": 'Z71.41' from "ICD-10" display 'Alcohol abuse counseling and surveillance of alcoholic'
code "Other specified counseling": 'Z71.89' from "ICD-10" display 'Other specified counseling'
code "How often have you had five or more drinks in one day during the past year [Reported]": '88037-7' from "LOINC" display 'How often have you had five or more drinks in one day during the past year [Reported]'
code "How often have you had four or more drinks in one day during the past year [Reported]": '75889-6' from "LOINC" display 'How often have you had four or more drinks in one day during the past year [Reported]'
code "Total score [AUDIT]": '75624-7' from "LOINC" display 'Total score [AUDIT]'
code "Total score [AUDIT-C]": '75626-2' from "LOINC" display 'Total score [AUDIT-C]'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "January 1 of Year Prior to Measurement Period":
  DateTime(((year from start of "Measurement Period")- 1), 1, 1, 0, 0, 0, 0, 0)

define "November 1 of the Measurement Period":
  DateTime((year from start of "Measurement Period"), 11, 1, 23, 59, 59, 0, 0)

define "Patient is 65 or Over":
  AgeInYearsAt(date from start of "Measurement Period")>= 65

define "Initial Population 1":
  AgeInYearsAt(date from start of "Measurement Period")>= 18
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

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  exists "Unhealthy Alcohol Use Screen with Positive Result Between January 1 and November 1"
  and "Numerator 1"

define "Exclusions 1":
  "Alcohol Use Disorder starting during Year Prior to Measurement Period"
    or "Dementia starting on or before end of Measurement Period"
    or Hospice."Hospice Intervention or Encounter"

define "Alcohol Use Disorder starting during Year Prior to Measurement Period":
  exists ( Status."Active Condition" ( [Condition: "Alcohol Use Disorder"] ) ) AlcoholUseDisorder
    where FHIRBase."Prevalence Period" ( AlcoholUseDisorder ) starts during Interval["January 1 of Year Prior to Measurement Period", start of "Measurement Period" )

define "Dementia starting on or before end of Measurement Period":
  exists ( Status."Active Condition" ( [Condition: "Dementia"] ) ) Dementia
    where FHIRBase."Prevalence Period" ( Dementia ) starts on or before 
    end of "Measurement Period"

define "Exclusions 2":
  "Exclusions 1"

define "Numerator 1":
  exists ( ( "AUDIT Screen with Documented Result"
      union "AUDIT-C Screen with Documented Result"
      union "Single-Question Screen with Documented Result" ) AlcoholScreenResult
      where FHIRBase."Normalize Interval" ( AlcoholScreenResult.effective ) starts during Interval[start of "Measurement Period", "November 1 of the Measurement Period"]
  )

define "AUDIT Screen with Documented Result":
  "Unhealthy Alcohol Use Screening with Documented Result"("Total score [AUDIT]")

define "AUDIT-C Screen with Documented Result":
  "Unhealthy Alcohol Use Screening with Documented Result"("Total score [AUDIT-C]")

define "Single-Question Screen with Documented Result":
  ( ( "Unhealthy Alcohol Use Screening with Documented Result"("How often have you had five or more drinks in one day during the past year [Reported]")) FiveDrinks
      where Patient.gender.value = 'male'
  )
    union ( ( "Unhealthy Alcohol Use Screening with Documented Result"("How often have you had four or more drinks in one day during the past year [Reported]")) FourDrinks
        where ( Patient.gender.value = 'female'
            or "Patient is 65 or Over"
        )
    )

define "Numerator 2":
  "Counseling or Other Follow-up Care on or 60 Days after First Positive Screen"

define "Unhealthy Alcohol Use Screen with Positive Result Between January 1 and November 1":
  ( "AUDIT Screen with Positive Result"
    union "AUDIT-C Screen with Positive Result"
    union "Single-Question Screen with Positive Result" ) AlcoholPositiveScreen
    where date from start of FHIRBase."Normalize Interval" ( AlcoholPositiveScreen.effective ) during Interval[start of "Measurement Period", "November 1 of the Measurement Period"]

define "AUDIT Screen with Positive Result":
  "Unhealthy Alcohol Use Screening with Positive Result"("Total score [AUDIT]", 8)

define "AUDIT-C Screen with Positive Result":
  ( "Unhealthy Alcohol Use Screening with Positive Result"("Total score [AUDIT]", 8)) AUDIT
    union ( ( "Unhealthy Alcohol Use Screening with Positive Result"("Total score [AUDIT-C]", 4)) AUDITCMale
        where Patient.gender.value = 'male'
    )
    union ( ( "Unhealthy Alcohol Use Screening with Positive Result"("Total score [AUDIT-C]", 3)) AUDITCFemale
        where Patient.gender.value = 'female'
    )

define "Single-Question Screen with Positive Result":
  ( ( "Unhealthy Alcohol Use Screening with Positive Result"("How often have you had five or more drinks in one day during the past year [Reported]", 1)) FiveOrMore
      where Patient.gender.value = 'male'
  )
    union ( ( "Unhealthy Alcohol Use Screening with Positive Result"("How often have you had four or more drinks in one day during the past year [Reported]", 1)) FourOrMore
        where ( Patient.gender.value = 'female'
            or "Patient is 65 or Over"
        )
    )

define "First Positive Screen":
  First("Unhealthy Alcohol Use Screen with Positive Result Between January 1 and November 1" PositiveScreen
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )

define "Counseling or Other Follow-up Care on or 60 Days after First Positive Screen":
  ( exists ( Status."Completed Procedure" ( [Procedure: "Alcohol Counseling or Other Follow Up Care"] ) ) FollowupProcedure
      with "First Positive Screen" FirstPositiveScreen
        such that date from 
        end of FHIRBase."Normalize Interval" ( FollowupProcedure.performed ) 60 days or less on or after date from start of FHIRBase."Normalize Interval" ( FirstPositiveScreen.effective )
  )
    or ( exists ( Status."Active Condition" ( [Condition: "Alcohol abuse counseling and surveillance of alcoholic"] )
        union Status."Active Condition" ( [Condition: "Other specified counseling"] ) ) FollowupDiagnosis
        with "First Positive Screen" FirstPositiveScreen
          such that date from start of FHIRBase."Prevalence Period" ( FollowupDiagnosis ) 60 days or less on or after date from start of FHIRBase."Normalize Interval" ( FirstPositiveScreen.effective )
    )

define function "Unhealthy Alcohol Use Screening with Documented Result"(assessmentDRC System.Code):
  [Observation: assessmentDRC] Obs
    where Obs.value is not null

define function "Unhealthy Alcohol Use Screening with Positive Result"(assessmentDRC System.Code, positiveScoreThreshold Integer):
  [Observation: assessmentDRC] Obs
    where Obs.value >= positiveScoreThreshold