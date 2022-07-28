library APME_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Claims version '1.1.0' called Claims

valueset "Cholesterol Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1743'
valueset "Glucose Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1752'
valueset "HbA1c Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1756'
valueset "LDL-C Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1770'
valueset "Cholesterol Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1742'
valueset "Glucose Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1751'
valueset "HbA1c Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1755'
valueset "LDL-C Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1769'
valueset "Antipsychotic Combination Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1738'
valueset "Antipsychotic Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1737'
valueset "Prochlorperazine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2195'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)


context Patient

define "Age":
 AgeInYearsAt(date from end of "Measurement Period")


define "Initial Population 1":
  ( AgeInYearsAt(date from 
      end of "Measurement Period"
    )in Interval[1, 17]
  )
    and "Antipsychotics on Different Days"
    and "Enrolled During Participation Period"

define "Antipsychotics on Different Days":
  exists ( ( flatten ( ( Claims."Pharmacy Claim With Medication" ( "Member Claims", "Antipsychotic Medication" ) ) Claim1
        return Claim1.ServicePeriod
    ) ) MedicationClaim1
      where exists ( flatten ( ( Claims."Pharmacy Claim With Medication" ( "Member Claims", "Antipsychotic Medication" ) ) Claim2
          return Claim2.ServicePeriod
      ) ) MedicationClaim2
        where date from
        end of MedicationClaim1 !~ date from
        end of MedicationClaim2
  )


define "Antipsychotic Medication":
  FHIRBase."VS Cast Function" ( "Prochlorperazine Medications" )
    union FHIRBase."VS Cast Function" ( "Antipsychotic Combination Medications" )
    union FHIRBase."VS Cast Function" ( "Antipsychotic Medications" )

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
  end of "Measurement Period"], 45 )
    and Enrollment."Pharmacy Benefit Enrollment Criteria" ( "Member Coverage", date from 
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
    end of "Measurement Period"], 45 )


define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Denominator 1":
    ( AgeInYearsAt(date from
      end of "Measurement Period"
    )in Interval[1, 17]
  )
    and "Antipsychotics on Different Days"

define "Denominator 2":
  "Denominator 1"

define "Denominator 3":
  "Denominator 1"

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Numerator 1":
  ( "Glucose Testing During Measurement Period"
      or "HbA1c Testing During Measurement Period"
  )

define "Glucose Testing During Measurement Period":
  exists ( [Observation: "Glucose Lab Test"] GlucoseLab
      where FHIRBase."Normalize Interval" ( GlucoseLab.effective ) during "Measurement Period"
  )
    or exists ( [Observation: "Glucose Test Result or Finding"] GlucoseFinding
        where FHIRBase."Normalize Interval" ( GlucoseFinding.effective ) starts during "Measurement Period"
    )

define "HbA1c Testing During Measurement Period":
  exists ( [Observation: "HbA1c Lab Test"] HbA1cLab
      where FHIRBase."Normalize Interval" ( HbA1cLab.effective ) during "Measurement Period"
  )
    or exists ( [Observation: "HbA1c Test Result or Finding"] HbA1cFinding
        where FHIRBase."Normalize Interval" ( HbA1cFinding.effective ) starts during "Measurement Period"
    )

define "Numerator 2":
  "LDLC Testing During Measurement Period"
    or "Cholesterol Testing During Measurement Period"

define "LDLC Testing During Measurement Period":
  exists ( [Observation: "LDL-C Lab Test"] LDLCLab
      where FHIRBase."Normalize Interval" ( LDLCLab.effective ) during "Measurement Period"
  )
    or exists ( [Observation: "LDL-C Test Result or Finding"] LDLCFinding
        where FHIRBase."Normalize Interval" ( LDLCFinding.effective ) starts during "Measurement Period"
    )

define "Cholesterol Testing During Measurement Period":
  exists ( [Observation: "Cholesterol Lab Test"] CholesterolLab
      where FHIRBase."Normalize Interval" ( CholesterolLab.effective ) during "Measurement Period"
  )
    or exists ( [Observation: "Cholesterol Test Result or Finding"] CholesterolFinding
        where FHIRBase."Normalize Interval" ( CholesterolFinding.effective ) starts during "Measurement Period"
    )

define "Numerator 3":
  ( "Glucose Testing During Measurement Period"
      or "HbA1c Testing During Measurement Period"
  )
    and ( "LDLC Testing During Measurement Period"
        or "Cholesterol Testing During Measurement Period"
    )

define "Member Claims":
  [Claim] C
    where exists C.item CItem
      where date from 
      end of FHIRBase."Normalize Interval" ( CItem.serviced ) during Interval[date from start of "Measurement Period", date from 
      end of "Measurement Period"]

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"