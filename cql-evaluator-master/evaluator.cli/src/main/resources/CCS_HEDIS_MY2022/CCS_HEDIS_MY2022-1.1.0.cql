library CCS_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_PalliativeCare version '1.1.0' called PalliativeCare
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_Terminology version '1.1.0' called Terminology
include NCQA_CQLBase version '1.1.0' called CQLBase

valueset "Absence of Cervix Diagnosis": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1522'
valueset "Cervical Cytology Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1524'
valueset "High Risk HPV Test Result or Finding": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1526'
valueset "Cervical Cytology Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1525'
valueset "High Risk HPV Lab Test": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1527'
valueset "Hysterectomy With No Residual Cervix": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1523'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Initial Population":
  Patient.gender.value = 'female'
    and AgeInYearsAt(date from
      end of "Measurement Period"
    )in Interval[24, 64]
    and "Enrolled During Participation Period"

define "Enrolled During Participation Period":
  if ( exists "Product Line as of December 31 of Measurement Period" ProductType
      where FHIRHelpers.ToCode ( ProductType ) ~ Terminology."managed care policy"
  ) then (  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
    end of "Measurement Period", Interval[date from start of "Measurement Period" - 2 years, date from
    end of "Measurement Period" - 2 years], 45 )
      and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
      end of "Measurement Period", Interval[date from start of "Measurement Period" - 1 year, date from
      end of "Measurement Period" - 1 year], 45 )
      and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
      end of "Measurement Period", Interval[date from start of "Measurement Period", date from
      end of "Measurement Period"], 45 )
   )
    else if ( exists "Product Line as of December 31 of Measurement Period" ProductType
      where FHIRHelpers.ToCode ( ProductType ) ~ Terminology."subsidized health program"
  ) then ( Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from
    end of "Measurement Period"], 45 )
  )
    else false

define "Enrolled During Participation Period For CE":
  if ( true
  ) then (  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
    end of "Measurement Period", Interval[date from start of "Measurement Period" - 2 years, date from
    end of "Measurement Period" - 2 years], 45 )
      and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
      end of "Measurement Period", Interval[date from start of "Measurement Period" - 1 year, date from
      end of "Measurement Period" - 1 year], 45 )
      and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
      end of "Measurement Period", Interval[date from start of "Measurement Period", date from
      end of "Measurement Period"], 45 )
   )
    else if (true
  ) then ( Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from
    end of "Measurement Period"], 45 )
  )
    else false



define "Product Line as of December 31 of Measurement Period":
  flatten ( "Member Coverage" C
    where FHIRBase."Normalize Interval" ( C.period ) includes
    end of Last(CQLBase."Sort Date Intervals"(Enrollment."All Coverage Info"("Member Coverage", Interval[date from start of "Measurement Period" - 2 years, date from
      end of "Measurement Period"]).CollapsedFinal)
    )) Records
    return Records.type.coding

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps Interval[start of "Measurement Period" - 2 years,
end of "Measurement Period"]

define "Denominator":
  "Initial Population"

define "Exclusions":
  Hospice."Hospice Intervention or Encounter"
    or PalliativeCare."Palliative Care Overlapping Period" ( "Measurement Period" )

define "Numerator":
  exists "Cervical Cytology Within 3 Years"
    or exists "hrHPV Testing Within 5 Years"

define "Cervical Cytology Within 3 Years":
  ( [Observation: "Cervical Cytology Lab Test"]
    union [Observation: "Cervical Cytology Result or Finding"] ) CervicalCytology
    where FHIRBase."Normalize Interval" ( CervicalCytology.effective ) during Interval[start of "Measurement Period" - 2 years,
end of "Measurement Period"]

define "hrHPV Testing Within 5 Years":
  ( [Observation: "High Risk HPV Lab Test"]
    union [Observation: "High Risk HPV Test Result or Finding"] ) HPVTest
    where AgeInYearsAt(date from start of FHIRBase."Normalize Interval"(HPVTest.effective))>= 30
      and FHIRBase."Normalize Interval" ( HPVTest.effective ) during Interval[start of "Measurement Period" - 4 years,
end of "Measurement Period"]

define "Denominator Exceptions":
  exists "Absence of Cervix"

define "Absence of Cervix":
  ( ( Status."Completed Procedure" ( [Procedure: "Hysterectomy With No Residual Cervix"] ) ) NoCervixHysterectomy
      where FHIRBase."Normalize Interval" ( NoCervixHysterectomy.performed ) ends on or before
      end of "Measurement Period"
  )
    union ( ( Status."Active Condition" ( [Condition: "Absence of Cervix Diagnosis"] ) ) NoCervix
        where FHIRBase."Prevalence Period" ( NoCervix ) starts on or before
        end of "Measurement Period"
    )