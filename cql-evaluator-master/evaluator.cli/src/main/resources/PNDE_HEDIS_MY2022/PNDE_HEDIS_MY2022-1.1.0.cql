library PNDE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Encounter version '1.1.0' called Encounters
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment

codesystem "LOINC": 'http://loinc.org'
codesystem "SNOMEDCT": 'http://snomed.info/sct/731000124108'
codesystem "ICD-10": 'http://hl7.org/fhir/sid/icd-10-cm'

valueset "37 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1509'
valueset "38 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1510'
valueset "39 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1511'
valueset "40 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1512'
valueset "41 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1513'
valueset "42 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1514'
valueset "43 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1515'
valueset "Antidepressant Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1503'
valueset "Behavioral Health Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1383'
valueset "Deliveries": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1072'
valueset "Depression Case Management Encounter": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1389'
valueset "Depression or Other Behavioral Health Condition": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1501'
valueset "Follow Up Visit": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1385'
valueset "Weeks of Gestation Less Than 37": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1479'

code "Beck Depression Inventory Fast Screen total score [BDI]": '89208-3' from "LOINC" display 'Beck Depression Inventory Fast Screen total score [BDI]'
code "Beck Depression Inventory II total score [BDI]": '89209-1' from "LOINC" display 'Beck Depression Inventory II total score [BDI]'
code "Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]": '89205-9' from "LOINC" display 'Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]'
code "Edinburgh Postnatal Depression Scale [EPDS]": '71354-5' from "LOINC" display 'Edinburgh Postnatal Depression Scale [EPDS]'
code "Final score [DUKE-AD]": '90853-3' from "LOINC" display 'Final score [DUKE-AD]'
code "Length of gestation at birth (observable entity)": '412726003' from "SNOMEDCT" display 'Length of gestation at birth (observable entity)'
code "Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]": '55758-7' from "LOINC" display 'Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]'
code "Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]": '44261-6' from "LOINC" display 'Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]'
code "Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]": '89204-2' from "LOINC" display 'Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]'
code "PROMIS-29 Depression score T-score": '71965-8' from "LOINC" display 'PROMIS-29 Depression score T-score'
code "Symptoms of depression (finding)": '394924000' from "SNOMEDCT" display 'Symptoms of depression (finding)'
code "Total score [CUDOS]": '90221-3' from "LOINC" display 'Total score [CUDOS]'
code "Total score [M3]": '71777-7' from "LOINC" display 'Total score [M3]'
code "Exercise counseling": 'Z71.82' from "ICD-10" display 'Exercise counseling'

parameter "Measurement Period" Interval<DateTime>

context Patient

define "Initial Population 1":
  ( "Delivery" DeliveryProcedure
      where ( Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval[DeliveryProcedure.deliveryDate - 28 days, DeliveryProcedure.deliveryDate], 0 ) )
  ).deliveries

define "Delivery":
  "Delivery with Gestational Age Assessment or Diagnosis during Period"("Measurement Period")

define "Gestational Age Diagnosis":
  ( "Length of Gestation in Weeks"([Condition: "37 Weeks Gestation"], 37)
    union "Length of Gestation in Weeks"([Condition: "38 Weeks Gestation"], 38)
    union "Length of Gestation in Weeks"([Condition: "39 Weeks Gestation"], 39)
    union "Length of Gestation in Weeks"([Condition: "40 Weeks Gestation"], 40)
    union "Length of Gestation in Weeks"([Condition: "41 Weeks Gestation"], 41)
    union "Length of Gestation in Weeks"([Condition: "42 Weeks Gestation"], 42)
    union "Length of Gestation in Weeks"([Condition: "43 Weeks Gestation"], 43)
    union "Length of Gestation in Weeks"([Condition: "Weeks of Gestation Less Than 37"], null)) gaDiagnosis

define "Initial Population 2":
  "Initial Population 1"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Depression Screen with Delivery during Period"(("Adolescent Full Length Depression Screen with Positive Result"
        union "Adolescent Brief Screen with Positive Result"
    ), Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )
    union "Depression Screen with Delivery during Period"(("Adolescent Full Length Depression Screen with Positive Result"
          union "Adolescent Brief Screen with Positive Result"
      ), Interval["December 2 of Measurement Period", 
      end of "Measurement Period"]
    )
    union "Depression Screen with Delivery during Period"(("Adult Full Length Depression Screen with Positive Result"
          union "Adult Brief Screen with Positive Result"
      ), Interval[start of "Measurement Period", "December 1 of Measurement Period"]
    )
    union "Depression Screen with Delivery during Period"(("Adolescent Full Length Depression Screen with Positive Result"
          union "Adolescent Brief Screen with Positive Result"
      ), Interval["December 2 of Measurement Period", 
      end of "Measurement Period"]
    )

define "Adolescent Full Length Depression Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]", 10)
    union "Depression Screening with Positive Result"("Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]", 10)
    union "Depression Screening with Positive Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]", 17)
    union "Depression Screening with Positive Result"("Edinburgh Postnatal Depression Scale [EPDS]", 10)
    union "Depression Screening with Positive Result"("PROMIS-29 Depression score T-score", 60)) positiveFullLengthScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(positiveFullLengthScreen.effective))<= 17

define "Adolescent Brief Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]", 3)
    union "Depression Screening with Positive Result"("Beck Depression Inventory Fast Screen total score [BDI]", 8)) positiveBriefScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(positiveBriefScreen.effective))<= 17

define "Adult Full Length Depression Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]", 10)
    union "Depression Screening with Positive Result"("Beck Depression Inventory II total score [BDI]", 20)
    union "Depression Screening with Positive Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]", 17)
    union "Depression Screening with Positive Result"("Final score [DUKE-AD]", 30)
    union "Depression Screening with Positive Result"("Edinburgh Postnatal Depression Scale [EPDS]", 10)
    union "Depression Screening with Positive Result"("Total score [M3]", 5)
    union "Depression Screening with Positive Result"("PROMIS-29 Depression score T-score", 60)
    union "Depression Screening with Positive Result"("Total score [CUDOS]", 31)) positiveFullLengthScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(positiveFullLengthScreen.effective))>= 18

define "Adult Brief Screen with Positive Result":
  ( "Depression Screening with Positive Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]", 3)
    union "Depression Screening with Positive Result"("Beck Depression Inventory Fast Screen total score [BDI]", 8)) positiveBriefScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(positiveBriefScreen.effective))>= 18

define "Exclusions 1":
  "Delivery in Period at Less Than 37 Weeks Gestation"("Measurement Period")
    union "Delivery in Measurement Period with Hospice Intervention or Encounter"

define "Delivery in Measurement Period with Hospice Intervention or Encounter":
  ( "Delivery" DeliveryProcedure
      where Hospice."Hospice Intervention or Encounter"
  ).deliveries

define "Exclusions 2":
  "Exclusions 1"

define "Numerator 1":
  "Depression Screen with Delivery during Period"(("Adolescent Full Length Depression Screen with Documented Result"
        union "Adolescent Brief Screen with Documented Result"
    ), Interval[start of "Measurement Period", "December 1 of Measurement Period"]
  )
    union "Depression Screen with Delivery during Period"(("Adolescent Full Length Depression Screen with Documented Result"
          union "Adolescent Brief Screen with Documented Result"
      ), Interval["December 2 of Measurement Period", 
      end of "Measurement Period"]
    )
    union "Depression Screen with Delivery during Period"(("Adult Full Length Depression Screen with Documented Result"
          union "Adult Brief Screen with Documented Result"
      ), Interval[start of "Measurement Period", "December 1 of Measurement Period"]
    )
    union "Depression Screen with Delivery during Period"(("Adult Full Length Depression Screen with Documented Result"
          union "Adult Brief Screen with Documented Result"
      ), Interval["December 2 of Measurement Period", 
      end of "Measurement Period"]
    )

define "Adolescent Full Length Depression Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]")
    union "Depression Screening with Documented Result"("Patient Health Questionnaire-9: Modified for Teens total score [Reported.PHQ.Teen]")
    union "Depression Screening with Documented Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]")
    union "Depression Screening with Documented Result"("Edinburgh Postnatal Depression Scale [EPDS]")
    union "Depression Screening with Documented Result"("PROMIS-29 Depression score T-score")) documentedFullLengthScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(documentedFullLengthScreen.effective))<= 17

define "Adolescent Brief Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]")
    union "Depression Screening with Documented Result"("Beck Depression Inventory Fast Screen total score [BDI]")) documentedBriefScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(documentedBriefScreen.effective))<= 17

define "Adult Full Length Depression Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 9 item (PHQ-9) total score [Reported]")
    union "Depression Screening with Documented Result"("Beck Depression Inventory II total score [BDI]")
    union "Depression Screening with Documented Result"("Center for Epidemiologic Studies Depression Scale-Revised total score [CESD-R]")
    union "Depression Screening with Documented Result"("Final score [DUKE-AD]")
    union "Depression Screening with Documented Result"("Edinburgh Postnatal Depression Scale [EPDS]")
    union "Depression Screening with Documented Result"("Total score [M3]")
    union "Depression Screening with Documented Result"("PROMIS-29 Depression score T-score")
    union "Depression Screening with Documented Result"("Total score [CUDOS]")) documentedFullLengthScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(documentedFullLengthScreen.effective))>= 18

define "Adult Brief Screen with Documented Result":
  ( "Depression Screening with Documented Result"("Patient Health Questionnaire 2 item (PHQ-2) total score [Reported]")
    union "Depression Screening with Documented Result"("Beck Depression Inventory Fast Screen total score [BDI]")) documentedBriefScreen
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(documentedBriefScreen.effective))>= 18

define "Numerator 2":
  "Delivery with Follow Up Care on or 30 Days after First Positive Screen"
    union "Delivery with Positive Brief Screen Same Day as Negative Full Length Screen"

define "Delivery with Positive Brief Screen Same Day as Negative Full Length Screen":
  ( "Delivery" DeliveryProcedure
      where exists ( "First Positive Adult Screen on or before Delivery Date is Brief Screen" PositiveScreen
          with ( "Adult Full Length Depression Screen with Documented Result"
            except "Adult Full Length Depression Screen with Positive Result" ) NegativeScreen
            such that start of FHIRBase."Normalize Interval" ( NegativeScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( PositiveScreen.effective )
      )
        or exists ( "First Positive Adolescent Screen on or before Delivery Date is Brief Screen" PositiveScreen
            with ( "Adolescent Full Length Depression Screen with Documented Result"
              except "Adolescent Full Length Depression Screen with Positive Result" ) NegativeScreen
              such that start of FHIRBase."Normalize Interval" ( NegativeScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( PositiveScreen.effective )
        )
  ).deliveries

define "First Positive Adult Screen on or before Delivery Date is Brief Screen":
  "First Positive Adult Depression Screen on or before Delivery Date" PositiveScreen
    without "Adult Full Length Depression Screen with Positive Result" FLScreen
      such that start of FHIRBase."Normalize Interval" ( PositiveScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( FLScreen.effective )

define "First Positive Adult Depression Screen on or before Delivery Date":
  { First(("Adult Full Length Depression Screen with Positive Result"
      union "Adult Brief Screen with Positive Result")AdultPositiveScreenDelivery
      with "Delivery" DeliveryProcedure
        such that("Depression Screening During Pregnancy"(DeliveryProcedure.deliveryDate, AdultPositiveScreenDelivery)
            or "Depression Screening between Conception and December 1"(DeliveryProcedure.deliveryDate, AdultPositiveScreenDelivery)
        )
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )}

define "First Positive Adolescent Screen on or before Delivery Date is Brief Screen":
  "First Positive Adolescent Depression Screen on or before Delivery Date" PositiveScreen
    without "Adolescent Full Length Depression Screen with Positive Result" FLScreen
      such that start of FHIRBase."Normalize Interval" ( PositiveScreen.effective ) same day as start of FHIRBase."Normalize Interval" ( FLScreen.effective )

define "First Positive Adolescent Depression Screen on or before Delivery Date":
  { First(("Adolescent Full Length Depression Screen with Positive Result"
      union "Adolescent Brief Screen with Positive Result")AdolescentPositiveScreenDelivery
      with "Delivery" DeliveryProcedure
        such that "Depression Screening During Pregnancy"(DeliveryProcedure.deliveryDate, AdolescentPositiveScreenDelivery)
      sort by start of FHIRBase."Normalize Interval"(effective)asc
  )}

define "Delivery with Follow Up Care on or 30 Days after First Positive Screen":
  ( "Delivery" DeliveryProcedure
      where exists "Follow Up Care on or 30 days After First Positive Screen on or before Delivery Date"
  ).deliveries

define "Follow Up Care on or 30 days After First Positive Screen on or before Delivery Date":
  ( ( "First Positive Adolescent Depression Screen on or before Delivery Date"
    union "First Positive Adult Depression Screen on or before Delivery Date" ) Screening
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

define "Member Coverage":
  [Coverage] C
    where Interval[date from start of FHIRBase."Normalize Interval" ( C.period ), date from 
    end of FHIRBase."Normalize Interval" ( C.period )]overlaps Interval[date from start of "Measurement Period" - 28 days, date from 
    end of "Measurement Period"]

define "December 1 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 12, 1, 23, 59, 59, 0, 0)

define "December 2 of Measurement Period":
  DateTime((year from start of "Measurement Period"), 12, 2, 0, 0, 0, 0, 0)

define function "Conception Date from Diagnosis"(deliveryDate System.Date):
  deliveryDate - Last("Gestational Age Diagnosis" Diagnosis
      where date from start of Diagnosis.prevalencePeriod within 1 day of deliveryDate
      sort by start of prevalencePeriod
  ).value

define function "Conception Date from Assessment"(DeliveryDate System.Date):
  DeliveryDate - ( ( Tuple {
      ConceptionQuantity: Last([Observation: "Length of gestation at birth (observable entity)"] Assessment
          where date from start of FHIRBase."Normalize Interval"(Assessment.effective)within 1 day of DeliveryDate
          sort by start of FHIRBase."Normalize Interval"(effective)
      ).value
    } ) First
      return ( if ( First.ConceptionQuantity is FHIR.Quantity ) then ( convert FHIRHelpers.ToQuantity ( First.ConceptionQuantity ) to weeks ) 
          else if ( First.ConceptionQuantity is FHIR.integer ) then ( System.Quantity { value: FHIRHelpers.ToInteger ( First.ConceptionQuantity ), unit: 'weeks' } ) 
          else null
      )
  )

define function "Delivery during Period"(period Interval<DateTime>):
  ( ( Status."Completed Procedure" ( [Procedure: "Deliveries"] ) ) DeliveryProcedure
    where date from 
    end of FHIRBase."Normalize Interval" ( DeliveryProcedure.performed ) during Interval[date from start of period, date from 
    end of period]) deliveryDuringMP
    return {
      deliveries: deliveryDuringMP,
      deliveryDate: date from 
      end of FHIRBase."Normalize Interval" ( deliveryDuringMP.performed )
    }

define function "Delivery in Period at Less Than 37 Weeks Gestation"(period Interval<DateTime>):
  ( ( "Delivery with Gestational Age Assessment or Diagnosis during Period"(period)) deliveryDuringPeriod
      where exists ( deliveryDuringPeriod.assessments gaAssessments
          where gaAssessments.value < 37
      )
        or exists ( deliveryDuringPeriod.diagnoses gaDiagnoses
            where gaDiagnoses.code.coding in "Weeks of Gestation Less Than 37"
        )
  ).deliveries

define function "Delivery with Gestational Age Assessment or Diagnosis during Period"(period Interval<DateTime>):
  ( "Delivery during Period"(period)) deliveryDuringPeriod
    let Assessment: ( [Observation: "Length of gestation at birth (observable entity)"] assessment
        where date from start of FHIRBase."Normalize Interval" ( assessment.effective ) within 1 day of deliveryDuringPeriod.deliveryDate
    ),
    Diagnosis: ( "Gestational Age Diagnosis" GADx
        where date from start of GADx.prevalencePeriod within 1 day of deliveryDuringPeriod.deliveryDate
    )
    return if ( exists Assessment
        or exists Diagnosis
    ) then {
      deliveries: deliveryDuringPeriod.deliveries,
      deliveryDate: deliveryDuringPeriod.deliveryDate,
      assessments: Assessment,
      diagnoses: Diagnosis.Dx
    } 
      else null

define function "Depression Screening between Conception and December 1"(deliveryDate System.Date, Assessment Observation):
  if ( Assessment A
      where ( date from start of FHIRBase."Normalize Interval" ( A.effective ) during Interval["Conception Date from Assessment"(deliveryDate), date from "December 1 of Measurement Period"])
        or ( date from start of FHIRBase."Normalize Interval" ( A.effective ) during Interval["Conception Date from Diagnosis"(deliveryDate), date from "December 1 of Measurement Period"])
  ) is not null then true 
    else false

define function "Depression Screening During Pregnancy"(deliveryDate System.Date, Assessment Observation):
  ( Assessment A
      where ( date from start of FHIRBase."Normalize Interval" ( A.effective ) during Interval["Conception Date from Assessment"(deliveryDate), deliveryDate])
        or ( date from start of FHIRBase."Normalize Interval" ( A.effective ) during Interval["Conception Date from Diagnosis"(deliveryDate), deliveryDate])
  ) is not null

define function "Depression Screen with Delivery during Period"(depressionAssessments List<FHIR.Observation>, deliveryPeriod Interval<DateTime>):
  ( ( "Delivery" d
    where d.deliveryDate during Interval[date from start of deliveryPeriod, date from 
    end of deliveryPeriod]) DeliveryProcedure
    return ( Tuple {
      "Has Delivery prior to December 2": ( if DeliveryProcedure.deliveryDate during Interval[date from start of "Measurement Period", date from "December 1 of Measurement Period"]then DeliveryProcedure.deliveries 
          else null
      ),
      "Has Delivery on or after December 2": ( if DeliveryProcedure.deliveryDate during Interval[date from "December 2 of Measurement Period", date from 
        end of "Measurement Period"]then DeliveryProcedure.deliveries 
          else null
      )
    } ) deliveryDateCheck
      return ( Tuple {
        "Screening during Pregnancy": if deliveryDateCheck."Has Delivery prior to December 2" is not null then ( "Has Depression Screening during Period"(depressionAssessments, Interval[date from "Conception Date from Assessment"(DeliveryProcedure.deliveryDate), date from DeliveryProcedure.deliveryDate])
            or "Has Depression Screening during Period"(depressionAssessments, Interval[date from "Conception Date from Assessment"(DeliveryProcedure.deliveryDate), date from DeliveryProcedure.deliveryDate])
        ) 
          else null,
        "Screening between Conception and December 1": if deliveryDateCheck."Has Delivery on or after December 2" is not null then ( "Has Depression Screening during Period"(depressionAssessments, Interval[date from "Conception Date from Assessment"(DeliveryProcedure.deliveryDate), date from "December 1 of Measurement Period"])
            or "Has Depression Screening during Period"(depressionAssessments, Interval[date from "Conception Date from Assessment"(DeliveryProcedure.deliveryDate), date from "December 1 of Measurement Period"])
        ) 
          else null
      } ) screeningAndDelivery
        return if screeningAndDelivery."Screening during Pregnancy" = true then deliveryDateCheck."Has Delivery prior to December 2" 
          else if screeningAndDelivery."Screening between Conception and December 1" = true then deliveryDateCheck."Has Delivery on or after December 2" 
          else null ) screeningDuringPeriod
    where screeningDuringPeriod is not null

define function "Depression Screening with Documented Result"(assessmentDRC System.Code):
  [Observation: assessmentDRC] Obs
    where Obs.value is not null

define function "Depression Screening with Positive Result"(assessmentDRC System.Code, positiveScoreThreshold Integer):
  [Observation: assessmentDRC] Obs
    where Obs.value >= positiveScoreThreshold

define function "Has Depression Screening during Period"(Assessment List<Observation>, period Interval<DateTime>):
  exists Assessment A
    where ( date from start of FHIRBase."Normalize Interval" ( A.effective ) ) during Interval[date from start of period, date from 
    end of period]

define function "Length of Gestation in Weeks"(gaCondition List<FHIR.Condition>, weeksValue Integer):
  ( ( Status."Active Condition" ( gaCondition ) ) Dx
      return {
        prevalencePeriod: FHIRBase."Prevalence Period" ( Dx ),
        value: if weeksValue is null then null 
          else System.Quantity { value: weeksValue, unit: 'weeks' },
        Dx: Dx
      }
  )