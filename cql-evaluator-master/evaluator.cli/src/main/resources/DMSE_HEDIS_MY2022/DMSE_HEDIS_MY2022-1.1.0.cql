library DMSE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_Encounter version '1.1.0' called Encounters

codesystem "LOINC": 'http://loinc.org'

valueset "Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1044'
valueset "Interactive Outpatient Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1347'
valueset "Major Depression or Dysthymia": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1351'
valueset "Other Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1399'
valueset "Personality Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1355'
valueset "Pervasive Developmental Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1356'
valueset "Psychotic Disorders": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1352'

code "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]": '44261-6' from "LOINC" display 'Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]'
code "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]": '89204-2' from "LOINC" display 'Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Event 1":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
        where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period One"
  )

define "Initial Population 1":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
        where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period One"
  )
    and "Enrolled During Participation Period"

define "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia":
  ( Status."Finished Encounter" ( [Encounter: "Interactive Outpatient Encounter"] ) ) Encounter
    where Encounters."Encounter Has Diagnosis" ( Encounter, [Condition: "Major Depression or Dysthymia"] )
      and start of FHIRBase."Normalize Interval" ( Encounter.period ) during "Measurement Period"

define "Assessment Period One":
  Interval[start of "Measurement Period", "April 30 of Measurement Period"]

define "April 30 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 04, 30, 23, 59, 59, 0, 0)

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
  end of "Measurement Period"], 45 )

define "Event 2":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
        where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period Two"
  )

define "Initial Population 2":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
        where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period Two"
  )
    and "Enrolled During Participation Period"

define "Assessment Period Two":
  Interval["May 1 of Measurement Period", "August 31 of Measurement Period"]

define "May 1 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 05, 01, 0, 0, 0, 0, 0)

define "August 31 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 08, 31, 23, 59, 59, 0, 0)

define "Event 3":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
      where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period Three"
  )

define "Initial Population 3":
  ( AgeInYearsAt(date from start of "Measurement Period")>= 12
      and exists ( "Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia" ) DepressionEncounter
      where start of FHIRBase."Normalize Interval" ( DepressionEncounter.period ) during "Assessment Period Three"
  )
    and "Enrolled During Participation Period"

define "Assessment Period Three":
  Interval["September 1 of Measurement Period", 
  end of "Measurement Period"]

define "September 1 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 09, 01, 0, 0, 0, 0, 0)

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 2"

define "Denominator 3":
  "Initial Population 3"

define "Exclusions 1":
  exists ( ( Status."Active Condition" ( [Condition: "Bipolar Disorder"]
      union [Condition: "Other Bipolar Disorder"]
      union [Condition: "Personality Disorder"]
      union [Condition: "Psychotic Disorders"]
      union [Condition: "Pervasive Developmental Disorder"] ) ) ExclusionDiagnosis
      where FHIRBase."Prevalence Period" ( ExclusionDiagnosis ) starts during "Measurement Period"
  )
    or Hospice."Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Numerator 1":
  exists ( ( "PHQ-9 Assessment With A Score Documented" ) PHQ9Score
      where start of FHIRBase."Normalize Interval" ( PHQ9Score.effective ) during "Assessment Period One"
  )

define "PHQ-9 Assessment With A Score Documented":
  ( ( ( [Observation: "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]"]
        union "PHQ-9 Modified for Teens" ) PHQ9
        where PHQ9.value is not null
    )
  )


define "PHQ-9 Modified for Teens":
  ( [Observation: "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]"] PHQ9M
      where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(PHQ9M.effective))in Interval[12, 17]
  )

define "Numerator 2":
  exists ( "PHQ-9 Assessment With A Score Documented" PHQ9Score
      where start of FHIRBase."Normalize Interval" ( PHQ9Score.effective ) during "Assessment Period Two"
  )

define "Numerator 3":
  exists ( "PHQ-9 Assessment With A Score Documented" PHQ9Score
      where start of FHIRBase."Normalize Interval" ( PHQ9Score.effective ) during "Assessment Period Three"
  )