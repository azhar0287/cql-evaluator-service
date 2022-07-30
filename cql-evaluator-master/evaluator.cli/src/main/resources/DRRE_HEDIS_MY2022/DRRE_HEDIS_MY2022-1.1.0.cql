library DRRE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Terminology version '1.1.0' called Terminology
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status

codesystem "LOINC": 'http://loinc.org'

valueset "Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1044'
valueset "Major Depression or Dysthymia": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1351'
valueset "Other Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1399'
valueset "Personality Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1355'
valueset "Pervasive Developmental Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1356'
valueset "Psychotic Disorders": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1352'

code "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]": '44261-6' from "LOINC" display 'Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]'
code "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]": '89204-2' from "LOINC" display 'Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Age":
  AgeInYearsAt(date from start of "Intake Period")

define "Initial Population 1":
  AgeInYearsAt(date from start of "Intake Period")>= 12
    and ( "Index Episode Start Date Orignal" is not null )
    and "Enrolled During Participation Period"

define "Index Episode Start Date Orignal":
  First(("PHQ-9 Assessments")PHQ
      where start of FHIRBase."Normalize Interval"(PHQ.effective)during "Intake Period"
        and FHIRHelpers.ToInteger(PHQ.value)> 9
        and exists((Status."Active Condition"([Condition: "Major Depression or Dysthymia"]))MajorDepressionDysthymia
            where start of FHIRBase."Prevalence Period"(MajorDepressionDysthymia)same day or before start of FHIRBase."Normalize Interval"(PHQ.effective)
              and not(
                end of FHIRBase."Prevalence Period"(MajorDepressionDysthymia)same day or before start of FHIRBase."Normalize Interval"(PHQ.effective)
              )
        )
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )

define "Index Episode Start Date 123456":
  First(("PHQ-9 Assessments")PHQ
      where start of FHIRBase."Normalize Interval"(PHQ.effective)during "Intake Period"
        and FHIRHelpers.ToInteger(PHQ.value)> 9
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )

define "Index Episode Start Date":
  "Index Episode Start Date 123456" PQ
      where start of FHIRBase."Normalize Interval"(PQ.effective)during "Intake Period"
        and FHIRHelpers.ToInteger(PQ.value)> 9
        and exists((Status."Active Condition"([Condition: "Major Depression or Dysthymia"]))MajorDepressionDysthymia
            where start of FHIRBase."Prevalence Period"(MajorDepressionDysthymia)same day or before start of FHIRBase."Normalize Interval"(PQ.effective)
              and not(
                end of FHIRBase."Prevalence Period"(MajorDepressionDysthymia)same day or before start of FHIRBase."Normalize Interval"(PQ.effective)
              )
        )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps Interval["May 1 of Year Prior to Measurement Period", 
    end of "Measurement Period"]

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from "May 1 of Year Prior to Measurement Period", date from 
  end of "Measurement Period" - 1 years], 0 )
    and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
    end of "Measurement Period"], 45 )

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Denominator 1":
  AgeInYearsAt(date from start of "Intake Period")>= 12
    and ( "Index Episode Start Date" is not null )

define "Denominator 2":
  AgeInYearsAt(date from start of "Intake Period")>= 12
    and ( "Index Episode Start Date" is not null )

define "Denominator 3":
  AgeInYearsAt(date from start of "Intake Period")>= 12
    and ( "Index Episode Start Date" is not null )

define "Exclusions 1":
  exists ( ( Status."Active Condition" ( [Condition: "Bipolar Disorder"]
      union [Condition: "Other Bipolar Disorder"]
      union [Condition: "Personality Disorder"]
      union [Condition: "Psychotic Disorders"]
      union [Condition: "Pervasive Developmental Disorder"] ) ) ExclusionDiagnosis
      where date from start of FHIRBase."Prevalence Period" ( ExclusionDiagnosis ) during Interval[date from start of "Intake Period", date from 
      end of "Measurement Period"]
  )
    or Hospice."Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Intake Period":
  Interval["May 1 of Year Prior to Measurement Period", "April 30 of Measurement Period"]

define "Depression Follow Up Period":
  if "Index Episode Start Date 123456" is null
    or "Index Episode Start Date 123456".effective is null
    or ( date from start of FHIRBase."Normalize Interval" ( "Index Episode Start Date 123456".effective ) = minimum DateTime ) then null
    else Interval[( date from start of FHIRBase."Normalize Interval" ( "Index Episode Start Date 123456".effective ) + 120 days ), ( date from start of FHIRBase."Normalize Interval" ( "Index Episode Start Date 123456".effective ) + 240 days )]

define "Numerator 1":
  if "Depression Follow Up Period" is null then false
    else ( exists "PHQ-9 Assessments" PHQ9Assessment
      where date from start of FHIRBase."Normalize Interval" ( PHQ9Assessment.effective ) during Interval[date from start of "Depression Follow Up Period", date from
      end of "Depression Follow Up Period"]
  )

define "PHQ-9 Assessments":
  ( ( ( [Observation: "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]"]
        union "PHQ-9 Modified For Teens" ) PHQ9
        where FHIRHelpers.ToInteger ( PHQ9.value ) is not null
    )
  )

define "PHQ-9 Modified For Teens":
  ( [Observation: "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]"] PHQ9M
      where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(PHQ9M.effective))in Interval[12, 17]
  )

define "Numerator 2":
  ( "Last PHQ9 Assessment During Depression Follow Up Period" LastPHQ9
      where FHIRHelpers.ToInteger ( LastPHQ9.value ) < 5
  ) is not null

define "Last PHQ9 Assessment During Depression Follow Up Period":
  if "Depression Follow Up Period" is null then null 
    else Last(("PHQ-9 Assessments")PHQ
      where date from start of FHIRBase."Normalize Interval"(PHQ.effective)during Interval[date from start of "Depression Follow Up Period", date from 
      end of "Depression Follow Up Period"]
      sort by date from start of FHIRBase."Normalize Interval"(effective)asc
  )

define "Numerator 3":
  ( ( "Last PHQ9 Assessment During Depression Follow Up Period" ) LastPHQ9
      with "Index Episode Start Date 123456" IndexPHQ9
        such that FHIRHelpers.ToInteger ( LastPHQ9.value ) <= FHIRHelpers.ToInteger ( IndexPHQ9.value ) / 2
  ) is not null

define "May 1 of Year Prior to Measurement Period":
  DateTime(((year from start of "Measurement Period")- 1), 5, 1, 0, 0, 0, 0, 0)

define "April 30 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 04, 30, 23, 59, 59, 0, 0)