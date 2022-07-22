library COLE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_AdvancedIllnessandFrailty version '1.1.0' called AdvancedIllnessFrailty
include NCQA_PalliativeCare version '1.1.0' called PalliativeCare
include NCQA_Status version '1.1.0' called Status
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Claims version '1.1.0' called Claims

valueset "Colonoscopy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1064'
valueset "Colorectal Cancer": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1065'
valueset "CT Colonography": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1421'
valueset "Flexible Sigmoidoscopy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1102'
valueset "FOBT Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1959'
valueset "FOBT Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1960'
valueset "History of Colonoscopy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1910'
valueset "History of Flexible Sigmoidoscopy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1911'
valueset "History of Total Colectomy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1912'
valueset "sDNA FIT Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1749'
valueset "sDNA FIT Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1750'
valueset "Total Colectomy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1250'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Acute Inpatient Discharge with Advanced Illness":
  exists ( ( Claims."Medical Claims With Diagnosis" ( Claims."Medical Claims With Nonacute or Acute Inpatient Discharge" ( [Claim] ).AcuteInpatientDischarge, AdvancedIllnessFrailty."Advanced Illness ValueSet" ).ServicePeriod ) InpatientDischarge
      where date from
      end of InpatientDischarge during Interval[date from start of "Measurement Period" - 1 year, date from
      end of "Measurement Period"]
  )

define "claimType func":
   Claims."Professional or Institutional Claims"([Claim])


define "test func":
    Claims."Medical Claims With Nonacute or Acute Inpatient Discharge" ( [Claim] )

define "Initial Population":
  AgeInYearsAt(date from 
    end of "Measurement Period"
  )in Interval[46, 75]
    and "Enrolled During Participation Period"

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period" - 1 year, date from 
  end of "Measurement Period" - 1 year], 45 )
    and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
    end of "Measurement Period"], 45 )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Participation Period"

define "Participation Period":
  Interval[start of "Measurement Period" - 1 year, 
  end of "Measurement Period"]

define "Denominator":
  "Initial Population"

define "Exclusions":
  Hospice."Hospice Intervention or Encounter"
    or exists "Colorectal Cancer Exclusion"
    or exists "Total Colectomy Exclusion"
    or AdvancedIllnessFrailty."Advanced Illness and Frailty Exclusion Not Including Over Age 80"
    or PalliativeCare."Palliative Care Overlapping Period" ( "Measurement Period" )

define "Colorectal Cancer Exclusion":
  ( Status."Active Condition" ( [Condition: "Colorectal Cancer"] ) ) ColorectalCancer
    where FHIRBase."Prevalence Period" ( ColorectalCancer ) starts on or before 
    end of "Measurement Period"

define "Total Colectomy Exclusion":
  ( ( Status."Completed Procedure" ( [Procedure: "Total Colectomy"] ) ) ColectomyProcedure
      where FHIRBase."Normalize Interval" ( ColectomyProcedure.performed ) ends on or before 
      end of "Measurement Period"
  )
    union ( ( Status."Active Condition" ( [Condition: "History of Total Colectomy"] ) ) ColectomyDiagnosis
        where FHIRBase."Prevalence Period" ( ColectomyDiagnosis ) starts on or before 
        end of "Measurement Period"
    )

define "Numerator":
  exists "Fecal Occult Blood Test Performed"
    or exists "Flexible Sigmoidoscopy Performed"
    or exists "Colonoscopy Performed"
    or exists "CT Colonography Performed"
    or exists "Fecal Immunochemical Test DNA Performed"

define "Fecal Occult Blood Test Performed":
  ( [Observation: "FOBT Lab Test"]
    union [Observation: "FOBT Test Result or Finding"] ) FecalOccultResult
    where FHIRBase."Normalize Interval" ( FecalOccultResult.effective ) during "Measurement Period"

define "Flexible Sigmoidoscopy Performed":
  ( ( Status."Completed Procedure" ( [Procedure: "Flexible Sigmoidoscopy"] ) ) FlexibleSigmoidoscopyProcedure
      where FHIRBase."Normalize Interval" ( FlexibleSigmoidoscopyProcedure.performed ) ends during Interval[start of "Measurement Period" - 4 years, 
      end of "Measurement Period"]
  )
    union ( ( Status."Active Condition" ( [Condition: "History of Flexible Sigmoidoscopy"] ) ) FlexibleSigmoidoscopyDiagnosis
        where FHIRBase."Prevalence Period" ( FlexibleSigmoidoscopyDiagnosis ) starts during Interval[start of "Measurement Period" - 4 years, 
        end of "Measurement Period"]
    )

define "Colonoscopy Performed":
  ( ( Status."Completed Procedure" ( [Procedure: "Colonoscopy"] ) ) ColonoscopyProcedure
      where FHIRBase."Normalize Interval" ( ColonoscopyProcedure.performed ) ends during Interval[start of "Measurement Period" - 9 years, 
      end of "Measurement Period"]
  )
    union ( ( Status."Active Condition" ( [Condition: "History of Colonoscopy"] ) ) ColonoscopyDiagnosis
        where FHIRBase."Prevalence Period" ( ColonoscopyDiagnosis ) starts during Interval[start of "Measurement Period" - 9 years, 
        end of "Measurement Period"]
    )

define "CT Colonography Performed":
    (  [Observation: "CT Colonography"] Colonography
        where FHIRBase."Normalize Interval" ( Colonography.effective ) ends during Interval[start of "Measurement Period" - 4 years,
        end of "Measurement Period"])

    union

    ([Procedure: "CT Colonography"] ColonographyProcedure
          where FHIRBase."Normalize Interval"  ( ColonographyProcedure.performed ) ends during Interval[start of "Measurement Period" - 4 years,
        end of "Measurement Period"]
    )


define "Fecal Immunochemical Test DNA Performed":
  ( [Observation: "sDNA FIT Lab Test"]
    union [Observation: "sDNA FIT Test Result or Finding"] ) FitDNALab
    where FHIRBase."Normalize Interval" ( FitDNALab.effective ) during Interval[start of "Measurement Period" - 2 years, 
    end of "Measurement Period"]