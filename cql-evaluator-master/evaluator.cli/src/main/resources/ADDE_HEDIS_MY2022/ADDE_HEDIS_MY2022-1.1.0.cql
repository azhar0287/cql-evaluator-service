library ADDE_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_CQLBase version '1.1.0' called CQLBase
include NCQA_Encounter version '1.1.0' called Encounters
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Status version '1.1.0' called Status
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Terminology version '1.1.0' called Terminology
include NCQA_Claims version '1.1.0' called Claims

valueset "Acute Inpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1810'
valueset "ADHD Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1736'
valueset "BH Outpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1481'
valueset "Health and Behavior Assessment or Intervention": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1487'
valueset "Mental, Behavioral and Neurodevelopmental Disorders": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1961'
valueset "Narcolepsy": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1182'
valueset "Observation": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1191'
valueset "Online Assessments": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1446'
valueset "Partial Hospitalization or Intensive Outpatient": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1492'
valueset "Telephone Visits": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1246'
valueset "Visit Setting Unspecified": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1493'

parameter "Measurement Period" Interval<DateTime>

context Patient

define "Initial Population 1":
  "Member is Appropriate Age and Has IPSD with Negative Medication History"
    and not ( exists "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders During Initiation Phase"
        or exists "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders During Initiation Phase"
    )
    and "Enrolled During Participation Period 1"

define "Member is Appropriate Age and Has IPSD with Negative Medication History":
  ( AgeInYearsAt(date from start of "Intake Period")>= 6
      and AgeInYearsAt(date from 
        end of "Intake Period"
      )<= 12
  )
    and "Index Prescription Start Date" is not null

define "Index Prescription Start Date":
  First(((flatten("ADHD Medication Dispensed" MedicationClaim
        return MedicationClaim.ServicePeriod
    ))MedicationClaimDate
      where date from start of MedicationClaimDate in "Intake Period"
      return date from start of MedicationClaimDate)InitialDispensingEventDate
      where not(exists "ADHD Medication Dispensed" ADHDMedHistoryClaimInfo
          where exists ADHDMedHistoryClaimInfo.ServicePeriod MedHistoryClaimDate
            where date from start of MedHistoryClaimDate during Interval[InitialDispensingEventDate - 120 days, InitialDispensingEventDate)
      )
      sort asc
  )

define "ADHD Medication Dispensed":
  ( Claims."Pharmacy Claim With Medication" ( "Member Claims", ( FHIRBase."VS Cast Function" ( "ADHD Medications" ) ) ) ) ADHDMedicationClaims
    where exists ADHDMedicationClaims.ServicePeriod ADHDMedicationClaimDate
      where date from start of ADHDMedicationClaimDate during Interval[date from start of "Intake Period" - 120 days, date from 
      end of "Measurement Period"]

define "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders During Initiation Phase":
  "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders" AcuteEncounter
    where date from 
    end of AcuteEncounter during Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 30 days]

define "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders":
  ( flatten ( ( Claims."Medical Claims With Procedure in Header or on Line Item" ( "Claims with Principal Diagnosis of Mental Behavioral and Neurodevelopmental Disorders", FHIRBase."VS Cast Function" ( "Acute Inpatient" ) ) ) AcuteEncounter
      return AcuteEncounter.ServicePeriod
  ) ) AcuteEncounterClaim
    where date from 
    end of AcuteEncounterClaim during Interval[date from start of "Intake Period", date from 
    end of "Measurement Period"]

define "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders During Initiation Phase":
  "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders" AcuteInpatientDischarge
    where exists AcuteInpatientDischarge.item AcuteLineItem
      where date from 
      end of FHIRBase."Normalize Interval" ( AcuteLineItem.serviced ) during Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 30 days]

define "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders":
  ( ( Claims."Medical Claims With Nonacute or Acute Inpatient Discharge" ( "Claims with Principal Diagnosis of Mental Behavioral and Neurodevelopmental Disorders" ) ) AcuteDischargeClaim
    return AcuteDischargeClaim."AcuteInpatientDischarge" ) AcuteInpatientDischarge
    where exists AcuteInpatientDischarge.item AcuteLineItem
      where date from 
      end of FHIRBase."Normalize Interval" ( AcuteLineItem.serviced ) during Interval[date from start of "Intake Period", date from 
      end of "Measurement Period"]

define "Claims with Principal Diagnosis of Mental Behavioral and Neurodevelopmental Disorders":
  flatten ( ( Claims."Medical Claims With Principal Diagnosis" ( "Member Claims", FHIRBase."VS Cast Function" ( "Mental, Behavioral and Neurodevelopmental Disorders" ) ) ) ClaimsWithPrincipalDx
      return ClaimsWithPrincipalDx.Claim
  )

define "Enrolled During Participation Period 1":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval["Index Prescription Start Date" - 120 days, "Index Prescription Start Date" + 30 days], 0 )
    and Enrollment."Pharmacy Benefit Enrollment Criteria" ( "Member Coverage", null, Interval["Index Prescription Start Date" - 120 days, "Index Prescription Start Date" + 30 days], 0 )

define "Initial Population 2":
  "Member is Appropriate Age and Has IPSD with Negative Medication History"
    and "Has 210 Medication Treatment Days in 301 Day Period Starting on IPSD and Continuing through End of Continuation and Maintenance Phase"
    and not ( exists "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders Before End of Continuation and Maintenance Phase"
        or exists "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders Before End of Continuation and Maintenance Phase"
    )
    and "Enrolled During Participation Period 2"

define "Has 210 Medication Treatment Days in 301 Day Period Starting on IPSD and Continuing through End of Continuation and Maintenance Phase":
  ( CQLBase."Date Interval Covering Relative to Base Interval Stats" ( Interval["Index Prescription Start Date", "Index Prescription Start Date" + 300 days], "ADHD Medication Coverage Intervals" ) )."Total Days In Intervals" >= 210

define "ADHD Medication Coverage Intervals":
  flatten ( "ADHD Medication Dispensed" ADHDMedicationClaim
      return ADHDMedicationClaim.CoveredDays MedicationDateTimeInterval
        return Interval[date from start of MedicationDateTimeInterval, date from 
        end of MedicationDateTimeInterval]
  )

define "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders Before End of Continuation and Maintenance Phase":
  "Acute Inpatient Encounter for Mental Behavioral or Neurodevelopmental Disorders" AcuteInpatientEncounters
    where date from 
    end of AcuteInpatientEncounters during Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 300 days]

define "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders Before End of Continuation and Maintenance Phase":
  "Acute Inpatient Discharge for Mental Behavioral or Neurodevelopmental Disorders" AcuteInpatientDischarge
    where exists AcuteInpatientDischarge.item AcuteLineItem
      where date from 
      end of FHIRBase."Normalize Interval" ( AcuteLineItem.serviced ) during Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 300 days]

define "Enrolled During Participation Period 2":
  "Enrolled During Participation Period 1"
    and Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", null, Interval["Index Prescription Start Date" + 31 days, "Index Prescription Start Date" + 300 days], 45 )
    and Enrollment."Pharmacy Benefit Enrollment Criteria" ( "Member Coverage", null, Interval["Index Prescription Start Date" + 31 days, "Index Prescription Start Date" + 300 days], 45 )

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"
    or exists "Diagnosis of Narcolepsy"

define "Diagnosis of Narcolepsy":
  ( Status."Active Condition" ( [Condition: "Narcolepsy"] ) ) NarcolepsyInMP
    where date from start of FHIRBase."Prevalence Period" ( NarcolepsyInMP ) on or before date from 
    end of "Measurement Period"

define "Exclusions 2":
  "Exclusions 1"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 2"

define "Numerator 1":
  exists "Follow Up Encounters or Assessments During Initiation Phase"

define "Follow Up Encounters or Assessments During Initiation Phase":
  ( [Observation: "Health and Behavior Assessment or Intervention"] HealthAssessment
      where date from start of FHIRBase."Normalize Interval" ( HealthAssessment.effective ) in Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 30 days]
  )
    union ( "Follow Up Encounters with Telehealth or Telephone Visits" FollowUpEncounters
        where date from start of FHIRBase."Normalize Interval" ( FollowUpEncounters.period ) in Interval["Index Prescription Start Date" + 1 day, "Index Prescription Start Date" + 30 days]
    )

define "Follow Up Encounters with Telehealth or Telephone Visits":
  ( Encounters."Finished Encounter with Telehealth POS" ( ["Encounter": "Visit Setting Unspecified"] )
    union Encounters."Finished Encounter with Outpatient POS" ( ["Encounter": "Visit Setting Unspecified"] )
    union Status."Finished Encounter" ( [Encounter: "BH Outpatient"] )
    union Status."Finished Encounter" ( [Encounter: "Observation"] )
    union Status."Finished Encounter" ( [Encounter: "Partial Hospitalization or Intensive Outpatient"] )
    union Status."Finished Encounter" ( [Encounter: "Telephone Visits"] ) ) TelehealthFollowUpEncounters
    where date from start of FHIRBase."Normalize Interval" ( TelehealthFollowUpEncounters.period ) during Interval[date from start of "Intake Period", date from 
    end of "Measurement Period"]

define "Numerator 2":
  ( "Two Follow Up Encounters or Assessments During Continuation and Maintenance Phase"
      and exists "Follow Up Encounters or Assessments During Initiation Phase"
  )
    or ( "Two Follow Up Encounters with One eVisit or Virtual Check in During Continuation and Maintenance Phase"
        and exists "Follow Up Encounters or Assessments During Initiation Phase"
    )

define "Two Follow Up Encounters or Assessments During Continuation and Maintenance Phase":
  exists ( "Follow Up Encounters or Assessments During Continuation and Maintenance Phase" FollowUpVisitOne
      with "Follow Up Encounters or Assessments During Continuation and Maintenance Phase" FollowUpVisitTwo
        such that date from start of FHIRBase."Normalize Interval" ( FollowUpVisitTwo.period ) 1 day or more after date from start of FHIRBase."Normalize Interval" ( FollowUpVisitOne.period )
          or date from start of FHIRBase."Normalize Interval" ( FollowUpVisitOne.period ) 1 day or more after date from start of FHIRBase."Normalize Interval" ( FollowUpVisitTwo.period )
  )

define "Two Follow Up Encounters with One eVisit or Virtual Check in During Continuation and Maintenance Phase":
  exists ( "Follow Up Encounters or Assessments During Continuation and Maintenance Phase" FollowUpVisitOne
      with "Follow Up Encounter with eVisit or Virtual Check in During Continuation and Maintenance Phase" FollowUpVisitTwo
        such that date from start of FHIRBase."Normalize Interval" ( FollowUpVisitTwo.period ) 1 day or more after date from start of FHIRBase."Normalize Interval" ( FollowUpVisitOne.period )
          or date from start of FHIRBase."Normalize Interval" ( FollowUpVisitOne.period ) 1 day or more after date from start of FHIRBase."Normalize Interval" ( FollowUpVisitTwo.period )
  )

define "Follow Up Encounter with eVisit or Virtual Check in During Continuation and Maintenance Phase":
  ( Status."Finished Encounter" ( [Encounter: "Online Assessments"] ) ) Encounters
    with "Index Prescription Start Date" IPSD
      such that date from start of FHIRBase."Normalize Interval" ( Encounters.period ) during Interval[IPSD + 31 days, IPSD + 300 days]

define "Follow Up Encounters or Assessments During Continuation and Maintenance Phase":
  ( [Observation: "Health and Behavior Assessment or Intervention"] HealthAssessment
      where date from start of FHIRBase."Normalize Interval" ( HealthAssessment.effective ) in Interval["Index Prescription Start Date" + 31 days, "Index Prescription Start Date" + 300 days]
  )
    union ( "Follow Up Encounters with Telehealth or Telephone Visits" FollowUpEncounters
        where date from start of FHIRBase."Normalize Interval" ( FollowUpEncounters.period ) in Interval["Index Prescription Start Date" + 31 days, "Index Prescription Start Date" + 300 days]
    )

define "Member Claims":
  [Claim] C
    where exists C.item CItem
      where date from start of FHIRBase."Normalize Interval" ( CItem.serviced ) during Interval[date from start of "Intake Period" - 120 days, date from 
      end of "Measurement Period"]

define "Member Coverage":
  [Coverage] C
    where Interval[date from start of FHIRBase."Normalize Interval" ( C.period ), date from 
    end of FHIRBase."Normalize Interval" ( C.period )]overlaps Interval[date from start of "Intake Period" - 120 days, date from 
    end of "Measurement Period"]

define "Intake Period":
  Interval["March 1 of Year Prior to Measurement Period", "Last Calendar Day of February"]

define "Last Calendar Day of February":
  ( DateTime(year from start of "Measurement Period", 3, 1, 23, 59, 59, 0, 0)) - 1 day

define "March 1 of Year Prior to Measurement Period":
  DateTime((year from start of "Measurement Period" - 1), 3, 1, 0, 0, 0, 0, 0)