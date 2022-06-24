library DSFE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Encounter version '1.1.0' called Encounters
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment

codesystem "LOINC": 'http://loinc.org'
codesystem "SNOMEDCT": 'http://snomed.info/sct'
codesystem "ICD-10": 'http://hl7.org/fhir/sid/icd-10-cm'

valueset "Antidepressant Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1503'
valueset "Behavioral Health Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1383'
valueset "Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1044'
valueset "Depression": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1390'
valueset "Depression Case Management Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1389'
valueset "Depression or Other Behavioral Health Condition": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1501'
valueset "Follow Up Visit": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1385'
valueset "Other Bipolar Disorder": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1399'

code "Beck Depression Inventory Fast Screen total score [BDI]": '89208-3' from "LOINC" display 'Beck Depression Inventory Fast Screen total score [BDI]'
code "Beck Depression Inventory II total score [BDI]": '89209-1' from "LOINC" display 'Beck Depression Inventory II total score [BDI]'
code "Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]": '89205-9' from "LOINC" display 'Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]'
code "Edinburgh Postnatal Depression Scale [EPDS]": '71354-5' from "LOINC" display 'Edinburgh Postnatal Depression Scale [EPDS]'
code "Final score [DUKE-AD]": '90853-3' from "LOINC" display 'Final score [DUKE-AD]'
code "Geriatric depression scale (GDS) short version total": '48545-8' from "LOINC" display 'Geriatric depression scale (GDS) short version total'
code "Geriatric depression scale (GDS) total": '48544-1' from "LOINC" display 'Geriatric depression scale (GDS) total'
code "Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]": '55758-7' from "LOINC" display 'Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]'
code "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]": '44261-6' from "LOINC" display 'Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]'
code "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]": '89204-2' from "LOINC" display 'Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]'
code "PROMIS-29 Depression score T-score": '71965-8' from "LOINC" display 'PROMIS-29 Depression score T-score'
code "Symptoms of depression (finding)": '394924000' from "SNOMEDCT" display 'Symptoms of depression (finding)'
code "Total score [CUDOS]": '90221-3' from "LOINC" display 'Total score [CUDOS]'
code "Total score [M3]": '71777-7' from "LOINC" display 'Total score [M3]'
code "Exercise counseling": 'Z71.82' from "ICD-10" display 'Exercise counseling'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population 1":
  AgeInYearsAt(date from start of "Measurement Period")>= 12
    and "Enrolled During Participation Period"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
  end of "Measurement Period"], 45 )

define "Initial Population 2":
  "Initial Population 1"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Adolescent Depression Screening with Positive Result between January 1 and December 1"
    or "Adult Depression Screening with Positive Result between January 1 and December 1"

define "Adolescent Depression Screening with Positive Result between January 1 and December 1":
  "Has Depression Screening during Period"("Adolescent Full Length Depression Screen with Positive Result"
      union "Adolescent Brief Screen with Positive Result", Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )

define "Adult Depression Screening with Positive Result between January 1 and December 1":
  "Has Depression Screening during Period"("Adult Full Length Depression Screen with Positive Result"
      union "Adult Brief Screen with Positive Result", Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )

define "First Positive Adolescent Depression Screen between January 1 and December 1":
  { First(("Adolescent Full Length Depression Screen with Positive Result"
      union "Adolescent Brief Screen with Positive Result")PositiveScreen
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )}

define "Adolescent Full Length Depression Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]", 10)
    union "Depression Screening with Positive Result"("Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]", 10)
    union "Depression Screening with Positive Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]", 17)
    union "Depression Screening with Positive Result"("Edinburgh Postnatal Depression Scale [EPDS]", 10)
    union "Depression Screening with Positive Result"("PROMIS-29 Depression score T-score", 60)) AdolescentPositiveDepressionScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdolescentPositiveDepressionScreen.effective))<= 17
      and start of FHIRBase."Normalize Interval" ( AdolescentPositiveDepressionScreen.effective ) during Interval[start of "Measurement Period", "December 1 of Measurement Period"]

define "Adolescent Brief Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]", 3)
    union "Depression Screening with Positive Result"("Beck Depression Inventory Fast Screen total score [BDI]", 8)) AdolescentBriefPositive
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdolescentBriefPositive.effective))<= 17
      and start of FHIRBase."Normalize Interval" ( AdolescentBriefPositive.effective ) during Interval[start of "Measurement Period", "December 1 of Measurement Period"]

define "First Positive Adult Depression Screen between January 1 and December 1":
  { First(("Adult Full Length Depression Screen with Positive Result"
      union "Adult Brief Screen with Positive Result")PositiveScreen
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )}

define "Adult Full Length Depression Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]", 10)
    union "Depression Screening with Positive Result"("Beck Depression Inventory II total score [BDI]", 20)
    union "Depression Screening with Positive Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]", 17)
    union "Depression Screening with Positive Result"("Final score [DUKE-AD]", 30)
    union "Depression Screening with Positive Result"("Geriatric depression scale (GDS) total", 10)
    union "Depression Screening with Positive Result"("Edinburgh Postnatal Depression Scale [EPDS]", 10)
    union "Depression Screening with Positive Result"("Total score [M3]", 5)
    union "Depression Screening with Positive Result"("Total score [CUDOS]", 31)
    union "Depression Screening with Positive Result"("PROMIS-29 Depression score T-score", 60)) AdultPositiveDepressionScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdultPositiveDepressionScreen.effective))>= 18
      and start of FHIRBase."Normalize Interval" ( AdultPositiveDepressionScreen.effective ) during Interval[start of "Measurement Period", "December 1 of Measurement Period"]

define "Adult Brief Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]", 3)
    union "Depression Screening with Positive Result"("Beck Depression Inventory Fast Screen total score [BDI]", 8)
    union "Depression Screening with Positive Result"("Geriatric depression scale (GDS) short version total", 5)) AdultBriefPositive
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdultBriefPositive.effective))>= 18
      and start of FHIRBase."Normalize Interval" ( AdultBriefPositive.effective ) during Interval[start of "Measurement Period", "December 1 of Measurement Period"]

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"
    or exists "Bipolar Disorder Starting during Year Prior to Measurement Period"
    or exists "Depression Starting during Year Prior to Measurement Period"

define "Bipolar Disorder Starting during Year Prior to Measurement Period":
  ( Status."Active Condition" ( [Condition: "Bipolar Disorder"] )
    union Status."Active Condition" ( [Condition: "Other Bipolar Disorder"] ) ) BipolarExclusion
    where FHIRBase."Prevalence Period" ( BipolarExclusion ) starts during Interval["January 1 of Year Prior to Measurement Period", start of "Measurement Period" )

define "Depression Starting during Year Prior to Measurement Period":
  ( Status."Active Condition" ( [Condition: "Depression"] ) ) DepressionDiagnosis
    where FHIRBase."Prevalence Period" ( DepressionDiagnosis ) starts during Interval["January 1 of Year Prior to Measurement Period", start of "Measurement Period" )

define "Exclusions 2":
  "Exclusions 1"

define "Numerator 1":
  "Adolescent Depression Screening with Documented Result between January 1 and December 1"
    or "Adult Depression Screening with Documented Result between January 1 and December 1"

define "Adolescent Depression Screening with Documented Result between January 1 and December 1":
  "Has Depression Screening during Period"("Adolescent Full Length Depression Screen with Documented Result"
      union "Adolescent Brief Screen with Documented Result", Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )

define "Adolescent Full Length Depression Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]")
    union "Depression Screening with Documented Result"("Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]")
    union "Depression Screening with Documented Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]")
    union "Depression Screening with Documented Result"("Edinburgh Postnatal Depression Scale [EPDS]")
    union "Depression Screening with Documented Result"("PROMIS-29 Depression score T-score")) AdolescentDepressionScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdolescentDepressionScreen.effective))<= 17

define "Adolescent Brief Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]")
    union "Depression Screening with Documented Result"("Beck Depression Inventory Fast Screen total score [BDI]")) AdolescentBrief
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdolescentBrief.effective))<= 17

define "Adult Depression Screening with Documented Result between January 1 and December 1":
  "Has Depression Screening during Period"("Adult Full Length Depression Screen with Documented Result"
      union "Adult Brief Screen with Documented Result", Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )

define "Adult Full Length Depression Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]")
    union "Depression Screening with Documented Result"("Beck Depression Inventory II total score [BDI]")
    union "Depression Screening with Documented Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]")
    union "Depression Screening with Documented Result"("Final score [DUKE-AD]")
    union "Depression Screening with Documented Result"("Geriatric depression scale (GDS) total")
    union "Depression Screening with Documented Result"("Edinburgh Postnatal Depression Scale [EPDS]")
    union "Depression Screening with Documented Result"("Total score [M3]")
    union "Depression Screening with Documented Result"("Total score [CUDOS]")
    union "Depression Screening with Documented Result"("PROMIS-29 Depression score T-score")) AdultDepressionScreenResult
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdultDepressionScreenResult.effective))>= 18

define "Adult Brief Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]")
    union "Depression Screening with Documented Result"("Geriatric depression scale (GDS) short version total")
    union "Depression Screening with Documented Result"("Beck Depression Inventory Fast Screen total score [BDI]")) AdultBrief
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(AdultBrief.effective))>= 18

define "Numerator 2":
  exists "Follow Up Care on or 30 Days after First Positive Screen"
    or "Has Positive Brief Screen Same Day as Negative Full Length Screen"

define "Follow Up Care on or 30 Days after First Positive Screen":
  ( ( "First Positive Adolescent Depression Screen between January 1 and December 1"
    union "First Positive Adult Depression Screen between January 1 and December 1" ) Screening
    return ( Tuple {
      hasFollowUpVisit: exists ( Status."Finished Encounter" ( [Encounter: "Follow Up Visit"] ) ) FollowUpVisit
        where Encounters."Encounter Has Diagnosis" ( FollowUpVisit, [Condition: "Depression or Other Behavioral Health Condition"] )
          and date from start of FHIRBase."Normalize Interval" ( FollowUpVisit.period ) 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective ),
      hasDepressionCaseManagementEncounterWithDx: exists ( ( Status."Finished Encounter" ( [Encounter: "Depression Case Management Encounter"] ) ) dcmEnc
        where date from start of FHIRBase."Normalize Interval" ( dcmEnc.period ) 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective ) ) CaseManagementEncounterWithDx
        where Encounters."Encounter Has Diagnosis" ( CaseManagementEncounterWithDx, [Condition: "Depression or Other Behavioral Health Condition"] ),
      hasDepressionCaseManagementEncounterWithSymptom: exists ( ( Status."Finished Encounter" ( [Encounter: "Depression Case Management Encounter"] ) ) dcmEnc
        where date from start of FHIRBase."Normalize Interval" ( dcmEnc.period ) 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective ) ) CaseManagementEncounterWithSymptom
        where exists [Observation: "Symptoms of depression (finding)"] DepressionSymptoms
          where date from start of FHIRBase."Normalize Interval" ( DepressionSymptoms.effective ) ~ date from start of FHIRBase."Normalize Interval" ( CaseManagementEncounterWithSymptom.period ),
      hasBehavioralHealthEncounter: exists ( ( Status."Finished Encounter" ( [Encounter: "Behavioral Health Encounter"] ) ) BHEnc
          where date from start of FHIRBase."Normalize Interval" ( BHEnc.period ) 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective )
      )
        or ( exists ( Status."Active Condition" ( [Condition: "Exercise counseling"] ) ) ExerciseDiagnosis
            where date from start of FHIRBase."Prevalence Period" ( ExerciseDiagnosis ) 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective )
        ),
      hasAntidepressantMedication: exists ( Status."Dispensed Medication" ( [MedicationDispense: "Antidepressant Medications"] ) ) ADMeds
        where date from ADMeds.whenHandedOver 30 days or less on or after date from start of FHIRBase."Normalize Interval" ( Screening.effective )
    } ) FollowUpCare
      return if AnyTrue({ FollowUpCare.hasFollowUpVisit, FollowUpCare.hasDepressionCaseManagementEncounterWithDx, FollowUpCare.hasDepressionCaseManagementEncounterWithSymptom, FollowUpCare.hasBehavioralHealthEncounter, FollowUpCare.hasAntidepressantMedication })then Screening 
        else null ) screeningWithFollowUpCare
    where screeningWithFollowUpCare is not null

define "Has Positive Brief Screen Same Day as Negative Full Length Screen":
  exists ( "First Positive Adult Screen is Brief Screen" PositiveScreen
      with ( "Adult Full Length Depression Screen with Documented Result"
        except "Adult Full Length Depression Screen with Positive Result" ) NegativeScreen
        such that start of FHIRBase."Normalize Interval" ( NegativeScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( PositiveScreen.effective )
  )
    or exists ( "First Positive Adolescent Screen is Brief Screen" PositiveScreen
        with ( "Adolescent Full Length Depression Screen with Documented Result"
          except "Adolescent Full Length Depression Screen with Positive Result" ) NegativeScreen
          such that start of FHIRBase."Normalize Interval" ( NegativeScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( PositiveScreen.effective )
    )

define "First Positive Adult Screen is Brief Screen":
  "First Positive Adult Depression Screen between January 1 and December 1" PositiveScreen
    without "Adult Full Length Depression Screen with Positive Result" FLScreen
      such that start of FHIRBase."Normalize Interval" ( PositiveScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( FLScreen.effective )

define "First Positive Adolescent Screen is Brief Screen":
  "First Positive Adolescent Depression Screen between January 1 and December 1" PositiveScreen
    without "Adolescent Full Length Depression Screen with Positive Result" FLScreen
      such that start of FHIRBase."Normalize Interval" ( PositiveScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( FLScreen.effective )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"

define "December 1 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 12, 1, 23, 59, 59, 0, 0)

define "January 1 of Year Prior to Measurement Period":
  DateTime(((year from start of "Measurement Period")- 1), 1, 1, 0, 0, 0, 0, 0)

define function "Depression Screening with Positive Result"(assessmentDRC System.Code, positiveScoreThreshold Integer):
  [Observation: assessmentDRC] Obs
    where Obs.value >= positiveScoreThreshold

define function "Depression Screening with Documented Result"(assessmentDRC System.Code):
  [Observation: assessmentDRC] Obs
    where Obs.value is not null

define function "Has Depression Screening during Period"(Assessment List<Observation>, period Interval<DateTime>):
  exists Assessment A
    where ( date from start of FHIRBase."Normalize Interval" ( A.effective ) ) during Interval[date from start of period, date from 
    end of period]