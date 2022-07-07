library NCQA_Immunization version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers
include NCQA_FHIRBase version '1.1.0' called FHIRBase

context Patient

define function "Diagnosis on or Before Latest Possible Vaccination Date"(Diagnosis List<Condition>, LastVaccineDate System.Date):
  Diagnosis ExclusionDiagnosis
    where ( date from start of FHIRBase."Prevalence Period" ( ExclusionDiagnosis ) ) on or before date from LastVaccineDate

define function "Administered Datetime"(Vaccination Choice<Immunization, Procedure>):
  case
    when Vaccination is Immunization then ToDate(Vaccination.occurrence as dateTime)
    else date from
  end of ( FHIRBase."Normalize Interval" ( Vaccination.performed ) )
  end

// Use if the vaccinations follow the 14 day rule.

define function "Meets Required Vaccination Threshold With 14 Day Rule"(VaccinationList List<Choice<Immunization, Procedure>>, VaccinationPeriod Interval<Date>, MinimumThreshold Integer):
  exists ( VaccinationList Vaccine1
      let VaccineDate1: "Administered Datetime"(Vaccine1)
      where ( MinimumThreshold = 1
          and ( VaccineDate1 ) during VaccinationPeriod
      )
        or ( MinimumThreshold = 2
            and exists ( VaccinationList Vaccine2
                let VaccineDate2: "Administered Datetime"(Vaccine2)
                where ( VaccineDate1 ) during VaccinationPeriod
                  and VaccineDate2 during VaccinationPeriod
                  and VaccineDate1 14 days or more before VaccineDate2
            )
        )
        or ( MinimumThreshold = 3
            and exists ( from
                VaccinationList Vaccine2,
                VaccinationList Vaccine3
                let VaccineDate2: "Administered Datetime"(Vaccine2),
                VaccineDate3: "Administered Datetime"(Vaccine3)
                where VaccineDate1 during VaccinationPeriod
                  and VaccineDate2 during VaccinationPeriod
                  and VaccineDate3 during VaccinationPeriod
                  and VaccineDate1 14 days or more before VaccineDate2
                  and VaccineDate2 14 days or more before VaccineDate3
                return Vaccine1
            )
        )
        or ( MinimumThreshold = 4
            and exists ( from
                VaccinationList Vaccine2,
                VaccinationList Vaccine3,
                VaccinationList Vaccine4
                let VaccineDate2: "Administered Datetime"(Vaccine2),
                VaccineDate3: "Administered Datetime"(Vaccine3),
                VaccineDate4: "Administered Datetime"(Vaccine4)
                where VaccineDate1 during VaccinationPeriod
                  and VaccineDate2 during VaccinationPeriod
                  and VaccineDate3 during VaccinationPeriod
                  and VaccineDate4 during VaccinationPeriod
                  and VaccineDate1 14 days or more before VaccineDate2
                  and VaccineDate2 14 days or more before VaccineDate3
                  and VaccineDate3 14 days or more before VaccineDate4
                return Vaccine1
            )
        )
  )

// Use if the 14 day rule is not needed

define function "Meets Required Vaccination Threshold"(VaccinationList List<Choice<Immunization, Procedure>>, VaccinationPeriod Interval<Date>, MinimumThreshold Integer):
  Count(Distinct(VaccinationList Vaccine
        let vaccinationDate: "Administered Datetime"(Vaccine)
        where vaccinationDate during VaccinationPeriod
        return {
          i: Vaccine.id,
          code: Vaccine.code,
          vaccinationDate: vaccinationDate
        }
    ).vaccinationDate
  )>= MinimumThreshold

// Use if there is something other than 14 days needed between vaccinations

define function "Meets Required Threshold With Variable Days Apart"(VaccinationList List<Choice<Immunization, Procedure>>, VaccinationPeriod Interval<Date>, MinimumThreshold Integer, DaysBetween Integer):
  exists ( VaccinationList Vaccine1
      let VaccineDate1: "Administered Datetime"(Vaccine1)
      where ( MinimumThreshold = 2
          and exists ( VaccinationList Vaccine2
              let VaccineDate2: "Administered Datetime"(Vaccine2)
              where VaccineDate1 during VaccinationPeriod
                and VaccineDate2 during VaccinationPeriod
                and days between VaccineDate1 and VaccineDate2 >= DaysBetween
          )
      )
  )