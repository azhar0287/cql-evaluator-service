library PRSE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Immunization version '1.1.0' called Immunization
include NCQA_Status version '1.1.0' called Status

codesystem "SNOMEDCT": 'http://snomed.info/sct'

valueset "37 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1509'
valueset "38 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1510'
valueset "39 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1511'
valueset "40 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1512'
valueset "41 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1513'
valueset "42 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1514'
valueset "43 Weeks Gestation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1515'
valueset "Adult Influenza Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1913'
valueset "Adult Influenza Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1914'
valueset "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2240'
valueset "Deliveries": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1072'
valueset "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2241'
valueset "Tdap Immunization": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1791'
valueset "Tdap Vaccine Procedure": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1792'
valueset "Weeks of Gestation Less Than 37": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1479'

code "Length of gestation at birth (observable entity)": '412726003' from "SNOMEDCT" display 'Length of gestation at birth (observable entity)'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population 1":
  ( "Delivery" DeliveryProcedure
      where ( Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval[DeliveryProcedure.deliveryDate - 28 days, DeliveryProcedure.deliveryDate], 0 ) )
  ).deliveries

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps Interval[start of "Measurement Period" - 28 days, 
    end of "Measurement Period"]

define "Delivery":
  "Delivery with Gestational Age Assessment or Diagnosis during Period"("Measurement Period")

define "Gestational Age Diagnosis":
  ( "Length of Gestation in Weeks"(["Condition": "37 Weeks Gestation"], 37)
    union "Length of Gestation in Weeks"(["Condition": "38 Weeks Gestation"], 38)
    union "Length of Gestation in Weeks"(["Condition": "39 Weeks Gestation"], 39)
    union "Length of Gestation in Weeks"(["Condition": "40 Weeks Gestation"], 40)
    union "Length of Gestation in Weeks"(["Condition": "41 Weeks Gestation"], 41)
    union "Length of Gestation in Weeks"(["Condition": "42 Weeks Gestation"], 42)
    union "Length of Gestation in Weeks"(["Condition": "43 Weeks Gestation"], 43)
    union "Length of Gestation in Weeks"(["Condition": "Weeks of Gestation Less Than 37"], null)) gaDiagnosis

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 1"

define "Denominator 3":
  "Initial Population 1"

define "Exclusions 1":
  "Delivery in Period at Less Than 37 Weeks Gestation"("Measurement Period")
    union "Delivery in Measurement Period with Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Delivery in Measurement Period with Hospice Intervention or Encounter":
  ( "Delivery" DeliveryProcedure
      where Hospice."Hospice Intervention or Encounter"
  ).deliveries

define "Numerator 1":
  "Delivery with Influenza Vaccine Between July 1 of Year Prior to Measurement Period and Delivery Date"

define "July 1 of the Year Prior to the Measurement Period":
  DateTime(((year from start of "Measurement Period")- 1), 7, 1, 0, 0, 0, 0, 0)

define "Influenza Vaccine":
  Status."Completed Immunization" ( [Immunization: "Adult Influenza Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Adult Influenza Vaccine Procedure"] )

define "Delivery with Influenza Vaccine Between July 1 of Year Prior to Measurement Period and Delivery Date":
  ( "Delivery" DeliveryProcedure
      where "Immunization Between July 1 of Year Prior to Measurement Period and Delivery Date"(DeliveryProcedure.deliveryDate, "Influenza Vaccine")
  ).deliveries

define "Numerator 2":
  "Delivery with Tdap Vaccine during Pregnancy"
    union "Delivery with Td or Tdap Vaccine Contraindications on or before Delivery Date"

define "Tdap Vaccine":
  Status."Completed Immunization" ( [Immunization: "Tdap Immunization"] )
    union Status."Completed Procedure" ( [Procedure: "Tdap Vaccine Procedure"] )

define "Delivery with Tdap Vaccine during Pregnancy":
  ( "Delivery" DeliveryProcedure
      where "Immunization during Pregnancy"(DeliveryProcedure.deliveryDate, "Tdap Vaccine")
  ).deliveries

define "Delivery with Td or Tdap Vaccine Contraindications on or before Delivery Date":
  ( "Delivery" DeliveryProcedure
      where exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( "Td or Tdap Vaccine Contraindications", DeliveryProcedure.deliveryDate )
  ).deliveries

define "Td or Tdap Vaccine Contraindications":
  Status."Active Condition" ( [Condition: "Anaphylaxis Due to Diphtheria, Tetanus or Pertussis Vaccine"] )
    union Status."Active Condition" ( [Condition: "Encephalitis Due to Diphtheria, Tetanus or Pertussis Vaccine"] )

define "Numerator 3":
  "Delivery with Influenza Criteria and Tdap Criteria"

define "Delivery with Influenza Criteria and Tdap Criteria":
  ( "Delivery" DeliveryProcedure
      where ( "Immunization Between July 1 of Year Prior to Measurement Period and Delivery Date"(DeliveryProcedure.deliveryDate, "Influenza Vaccine")
          and ( "Immunization during Pregnancy"(DeliveryProcedure.deliveryDate, "Tdap Vaccine")
              or exists Immunization."Diagnosis on or Before Latest Possible Vaccination Date" ( "Td or Tdap Vaccine Contraindications", DeliveryProcedure.deliveryDate )
          )
      )
  ).deliveries

define function "Conception Date from Diagnosis"(DeliveryDate System.Date):
  DeliveryDate - Last("Gestational Age Diagnosis" Diagnosis
      where date from start of Diagnosis.prevalencePeriod within 1 day of DeliveryDate
      sort by start of prevalencePeriod
  ).value

define function "Conception Date from Assessment"(DeliveryDate System.Date):
  DeliveryDate - ( ( Tuple {
      ConceptionQuantity: Last(["Observation": "Length of gestation at birth (observable entity)"] Assessment
          where date from start of FHIRBase."Normalize Interval"(Assessment.effective)within 1 day of DeliveryDate
          sort by start of FHIRBase."Normalize Interval"(effective)
      ).value
    } ) First
      return ( if ( First.ConceptionQuantity is FHIR.Quantity ) then ( convert FHIRHelpers.ToQuantity ( First.ConceptionQuantity ) to weeks ) 
          else if ( First.ConceptionQuantity is FHIR.integer ) then ( System.Quantity { value: FHIRHelpers.ToInteger ( First.ConceptionQuantity ), unit: 'weeks' } ) 
          else null
      )
  )

define function "Immunization during Pregnancy"(DeliveryDate System.Date, VaccinationList List<Choice<Immunization, Procedure>>):
  exists ( VaccinationList Vaccine
      let VaccineDate: Immunization."Administered Datetime" ( Vaccine ),
      conceptionDateFromAssessment: "Conception Date from Assessment"(DeliveryDate),
      conceptionDateFromDiagnosis: "Conception Date from Diagnosis"(DeliveryDate)
      where ( conceptionDateFromAssessment is not null
          and date from VaccineDate during Interval[conceptionDateFromAssessment, DeliveryDate]
      )
        or ( conceptionDateFromDiagnosis is not null
            and date from VaccineDate during Interval[conceptionDateFromDiagnosis, DeliveryDate]
        )
  )

define function "Immunization Between July 1 of Year Prior to Measurement Period and Delivery Date"(DeliveryDate System.Date, VaccinationList List<Choice<Immunization, Procedure>>):
  exists ( VaccinationList Vaccine
      let VaccineDate: Immunization."Administered Datetime" ( Vaccine )
      where ( date from VaccineDate during Interval["July 1 of the Year Prior to the Measurement Period", DeliveryDate])
  )

define function "Delivery during Period"(period Interval<DateTime>):
  ( ( Status."Completed Procedure" ( ["Procedure": "Deliveries"] ) ) DeliveryProcedure
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
    let Assessment: ( ["Observation": "Length of gestation at birth (observable entity)"] assessment
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

define function "Length of Gestation in Weeks"(gaCondition List<FHIR.Condition>, weeksValue Integer):
  ( ( Status."Active Condition" ( gaCondition ) ) Dx
      return {
        prevalencePeriod: FHIRBase."Prevalence Period" ( Dx ),
        value: if weeksValue is null then null 
          else System.Quantity { value: weeksValue, unit: 'weeks' },
        Dx: Dx
      }
  )