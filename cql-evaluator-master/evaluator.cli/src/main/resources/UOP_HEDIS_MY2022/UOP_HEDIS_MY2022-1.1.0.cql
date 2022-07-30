library UOP_HEDIS_MY2022 version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Status version '1.1.0' called Status
include NCQA_Hospice version '1.1.0' called Hospice
include NCQA_Medication version '1.1.0' called Medications
include NCQA_CQLBase version '1.1.0' called CQLBase
include NCQA_HealthPlanEnrollment version '1.1.0' called Enrollment
include NCQA_Claims version '1.1.0' called Claims
include NCQA_Terminology version '1.1.0' called Terminology

valueset "Acetaminophen Benzhydrocodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.2217'
valueset "Acetaminophen Butalbital Caffeine Codeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1548'
valueset "Acetaminophen Caffeine Dihydrocodeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1561'
valueset "Acetaminophen Codeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1547'
valueset "Acetaminophen Hydrocodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1550'
valueset "Acetaminophen Oxycodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1552'
valueset "Acetaminophen Tramadol Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1562'
valueset "Aspirin Butalbital Caffeine Codeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1549'
valueset "Aspirin Caffeine Dihydrocodeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1551'
valueset "Aspirin Carisoprodol Codeine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1556'
valueset "Aspirin Oxycodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1553'
valueset "Belladonna Opium Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1555'
valueset "Buprenorphine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1545'
valueset "Butorphanol Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1544'
valueset "Codeine Sulfate Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1534'
valueset "Fentanyl Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1537'
valueset "Hydrocodone Ibuprofen Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1560'
valueset "Hydrocodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1546'
valueset "Hydromorphone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1538'
valueset "Ibuprofen Oxycodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1564'
valueset "Levorphanol Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1542'
valueset "Meperidine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1535'
valueset "Meperidine Promethazine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1554'
valueset "Methadone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1536'
valueset "Morphine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1539'
valueset "Morphine Naltrexone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1566'
valueset "Naloxone Pentazocine Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1557'
valueset "Opium Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1541'
valueset "Oxycodone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1540'
valueset "Oxymorphone Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1543'
valueset "Tapentadol Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1565'
valueset "Tramadol Medications": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1559'

parameter "Measurement Period" default Interval [@2022-01-01T00:00:00.0, @2022-12-31T23:59:59.0)

context Patient

define "Age":
  AgeInYearsAt(date from start of "Measurement Period")

define "Initial Population 1":
  AgeInYearsAt(date from start of "Measurement Period")>= 18
    and "Two Opioid Medications Dispensed on Different Dates of Service"
    and "Opioid Medication Coverage Days" >= 15
    and "Enrolled During Participation Period"

define "Two Opioid Medications Dispensed on Different Dates of Service":
  IsTrue(Count("Pharmacy Claim With Opioid Medication".ServicePeriod MedDispensedOne
        with "Pharmacy Claim With Opioid Medication".ServicePeriod MedDispensedTwo
          such that date from start of MedDispensedOne !~ date from start of MedDispensedTwo
    )>= 2
  )

define "Pharmacy Claim With Opioid Medication":
  ( Claims."Get Paid Claims for Pharmacy Services" ( "Member Claim Responses", "Member Claims", "Opioid Medication Valuesets" ) )

define "Member Claim Responses":
  [ClaimResponse] R
    where exists R.addItem RItem
      where FHIRBase."Normalize Interval" ( RItem.serviced ) during "Measurement Period"

define "Member Claims":
  [Claim] C
    where exists C.item CItem
      where FHIRBase."Normalize Interval" ( CItem.serviced ) during "Measurement Period"

define "Opioid Medication Valuesets":
  ( ( FHIRBase."VS Cast Function" ( "Acetaminophen Benzhydrocodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Butalbital Caffeine Codeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Caffeine Dihydrocodeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Codeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Hydrocodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Oxycodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Acetaminophen Tramadol Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Aspirin Butalbital Caffeine Codeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Aspirin Caffeine Dihydrocodeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Aspirin Carisoprodol Codeine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Aspirin Oxycodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Belladonna Opium Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Buprenorphine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Butorphanol Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Codeine Sulfate Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Fentanyl Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Hydrocodone Ibuprofen Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Hydrocodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Hydromorphone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Ibuprofen Oxycodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Levorphanol Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Meperidine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Meperidine Promethazine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Methadone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Morphine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Morphine Naltrexone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Naloxone Pentazocine Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Opium Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Oxycodone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Oxymorphone Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Tapentadol Medications" ) )
      union ( FHIRBase."VS Cast Function" ( "Tramadol Medications" ) )
  )

define "Opioid Medication Coverage Days":
  Medications."Coverage in Days" ( "Opioid Medication Coverage Intervals" )

define "Opioid Medication Coverage Intervals":
  Medications."Opioid Pharmacy Claim Service Periods During Period" ( "Pharmacy Claim With Opioid Medication".originalClaim.Claim, "Measurement Period" )

define "Enrolled During Participation Period":
  Enrollment."Health Plan Enrollment Criteria" ( "Member Coverage", date from 
  end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
  end of "Measurement Period"], 45 )
    and Enrollment."Pharmacy Benefit Enrollment Criteria" ( "Member Coverage", date from 
    end of "Measurement Period", Interval[date from start of "Measurement Period", date from 
    end of "Measurement Period"], 45 )

define "Member Coverage":
  [Coverage] C
    where FHIRBase."Normalize Interval" ( C.period ) overlaps "Measurement Period"

define "Initial Population 2":
  "Initial Population 1"

define "Initial Population 3":
  "Initial Population 1"

define "Denominator 1":
  "Initial Population 1"

define "Denominator 2":
  "Initial Population 1"

define "Denominator 3":
  "Initial Population 1"

define "Exclusions 1":
  Hospice."Hospice Intervention or Encounter"

define "Exclusions 2":
  "Exclusions 1"

define "Exclusions 3":
  "Exclusions 1"

define "Numerator 1":
  "Medication Dispensed from 4 or More Different Prescribers during Measurement Period"

define "Medication Dispensed from 4 or More Different Prescribers during Measurement Period":
  ( Claims."Get Prescriber NPI from Claims" ( "Pharmacy Claim With Opioid Medication".originalClaim.Claim ) ).IdentifierCount >= 4

define "Numerator 2":
  "Medication Dispensed from 4 or More Different Pharmacies during Measurement Period"

define "Medication Dispensed from 4 or More Different Pharmacies during Measurement Period":
  ( Claims."Get Pharmacy NPI from Claims" ( "Pharmacy Claim With Opioid Medication".originalClaim.Claim ) ).IdentifierCount >= 4

define "Numerator 3":
  "Medication Dispensed from 4 or More Different Prescribers during Measurement Period"
    and "Medication Dispensed from 4 or More Different Pharmacies during Measurement Period"