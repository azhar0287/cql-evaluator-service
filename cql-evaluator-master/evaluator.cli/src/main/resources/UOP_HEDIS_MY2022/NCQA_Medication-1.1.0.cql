library NCQA_Medication version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_CQLBase version '1.1.0' called CQLBase
include NCQA_FHIRBase version '1.1.0' called FHIRBase
include NCQA_Claims version '1.1.0' called Claims

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

define function "Opioid Pharmacy Claim Service Periods During Period"(claim List<FHIR.Claim>, period Interval<DateTime>):
  ( flatten { "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Benzhydrocodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Butalbital Caffeine Codeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Caffeine Dihydrocodeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Codeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Hydrocodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Oxycodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Acetaminophen Tramadol Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Aspirin Butalbital Caffeine Codeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Aspirin Caffeine Dihydrocodeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Aspirin Carisoprodol Codeine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Aspirin Oxycodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Belladonna Opium Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Buprenorphine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Butorphanol Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Codeine Sulfate Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Fentanyl Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Hydrocodone Ibuprofen Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Hydrocodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Hydromorphone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Ibuprofen Oxycodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Levorphanol Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Meperidine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Meperidine Promethazine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Methadone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Morphine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Morphine Naltrexone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Naloxone Pentazocine Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Opium Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Oxycodone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Oxymorphone Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Tapentadol Medications")))X
      return all X.CoveredDays
  ), "RolloutIntervals"(flatten(Claims."Pharmacy Claim With Medication"(claim, FHIRBase."VS Cast Function"("Tramadol Medications")))X
      return all X.CoveredDays
  )} ) OpioidIntervals
    return all ( OpioidIntervals A
        return all ( Interval[date from start of A, date from 
          end of A]
            intersect Interval[date from start of period, date from 
            end of period]
        )
    )

define function "RolloutIntervals"(Intervals List<Interval<System.DateTime>>):
  RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(RolloutIntervalsOnce(Intervals))))))))))

define function "RolloutIntervalsOnce"(Intervals List<Interval<DateTime>>):
  flatten ( ( CQLBase."Collapse DateTime Interval Workaround" ( Intervals ) ) CollapsedInterval
    let IinC: ( Intervals X
        where X during CollapsedInterval
        return all X
    ),
    IinCC: CQLBase."Collapse DateTime Interval Workaround" ( IinC ),
    ICount: Count(IinC),
    DDuration: Sum(IntervalDays(IinCC)),
    DSum: Sum(IntervalDays(IinC)),
    DDiff: DSum - DDuration,
    NewInterval: IinCC X
      return ( Tuple {
        sdate: start of X,
        edate: 
        end of X,
        ndate: 
        end of X + System.Quantity { value: DDiff, unit: 'day' }
      } ) X
        return Tuple {
          CInterval: Interval[X.sdate, X.edate],
          RInterval: Interval[X.sdate, X.ndate]
        }
    return ( Tuple {
      IntervalsinCollapse: IinC,
      CollapseIntervalsinCollapse: IinCC,
      IntervalsinCollapseCount: ICount,
      DurationOfCollapseIntervalsinCollapse: DDuration,
      DurationofIntervalsinCollapse: DSum,
      NewInterval: NewInterval,
      DaysDiff: DDiff
    } ) X
      return all ( if X.IntervalsinCollapseCount > 1 then X.NewInterval.RInterval 
          else X.NewInterval.CInterval
      ) ) A
    return all A

define function "IntervalDays"(Intervals List<Interval<DateTime>>):
  Intervals I
    return all ( duration in days of I + 1 )

define function "Coverage in Days"(Intervals List<Interval<System.DateTime>>):
  if Count(Intervals)> 0 then ( Sum((CQLBase."Collapse DateTime Interval Workaround"(Intervals))CoverageInterval
        return all(duration in days of CoverageInterval + 1)
    )
  ) 
    else 0

define function "Opioid Medication Dispensing Events During Period"(MedDispense List<FHIR.MedicationDispense>, period Interval<DateTime>):
  ( flatten { "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Benzhydrocodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Butalbital Caffeine Codeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Caffeine Dihydrocodeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Codeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Hydrocodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Oxycodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Acetaminophen Tramadol Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Aspirin Butalbital Caffeine Codeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Aspirin Caffeine Dihydrocodeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Aspirin Carisoprodol Codeine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Aspirin Oxycodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Belladonna Opium Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Buprenorphine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Butorphanol Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Codeine Sulfate Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Fentanyl Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Hydrocodone Ibuprofen Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Hydrocodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Hydromorphone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Ibuprofen Oxycodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Levorphanol Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Meperidine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Meperidine Promethazine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Methadone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Morphine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Morphine Naltrexone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Naloxone Pentazocine Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Opium Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Oxycodone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Oxymorphone Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Tapentadol Medications"))))X
      return all X.DispenseSupplyInterval
  ), "RolloutIntervals"((("Medication Information"(MedDispense, period, FHIRBase."VS Cast Function"("Tramadol Medications"))))X
      return all X.DispenseSupplyInterval
  )} ) OpioidIntervals
    return all ( OpioidIntervals A
        return all ( Interval[date from start of A, date from 
          end of A]
            intersect Interval[date from start of period, date from 
            end of period]
        )
    )

define function "Medication Information"(MedDispense List<MedicationDispense>, period Interval<DateTime>, ValueSetCodes List<System.Code>):
  ( ( ( MedDispense MD
    where MD.whenHandedOver during period
      and MD.daysSupply is not null ) MDD
    return Tuple {
      MDD: MDD,
      MDDid: MDD.id,
      DispenseDate: MDD.whenHandedOver,
      daysSupply: MDD.daysSupply,
      MDDInterval: Interval[MDD.whenHandedOver, MDD.whenHandedOver + ( MDD.daysSupply - 1 day )]
        intersect period,
      Medcode: ( if ( MDD.medication.reference ) is not null then FHIRHelpers.ToConcept ( ( singleton from ( [Medication] X
            where X.id = FHIRBase."GetId" ( MDD.medication.reference )
        )
      ).code ).codes 
        else FHIRHelpers.ToConcept ( MDD.medication as CodeableConcept ).codes ) c
        return c.code,
      ValueSetCodes: ( ValueSetCodes ) c
        return c.code
    } ) MedicationInformation
    return all ( if exists ( MedicationInformation.Medcode X
          where X in MedicationInformation.ValueSetCodes
      ) then Tuple {
        DispenseID: MedicationInformation.MDDid,
        DispenseDate: MedicationInformation.DispenseDate,
        DispenseQuantity: MedicationInformation.daysSupply,
        DispenseSupplyInterval: MedicationInformation.MDDInterval
      } 
        else null
    ) ) FinalList
    where FinalList is not null