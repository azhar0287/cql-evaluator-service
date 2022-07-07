library NCQA_Terminology version '1.1.0'

using FHIR version '4.0.1'

include FHIRHelpers version '4.0.1' called FHIRHelpers

codesystem "ConditionClinicalStatusCodes": 'http://terminology.hl7.org/CodeSystem/condition-clinical'
codesystem "ClaimTypeCodes": 'http://terminology.hl7.org/CodeSystem/claim-type'
codesystem "ActCode": 'http://terminology.hl7.org/CodeSystem/v3-ActCode'
codesystem "IdentifierType": 'http://terminology.hl7.org/CodeSystem/v2-0203'

// Condition Clinical Status Codes

code "active": 'active' from "ConditionClinicalStatusCodes"
code "recurrence": 'recurrence' from "ConditionClinicalStatusCodes"
code "relapse": 'relapse' from "ConditionClinicalStatusCodes"
code "inactive": 'inactive' from "ConditionClinicalStatusCodes"
code "remission": 'remission' from "ConditionClinicalStatusCodes"
code "resolved": 'resolved' from "ConditionClinicalStatusCodes"

// Claim Type Codes
code "Pharmacy": 'pharmacy' from "ClaimTypeCodes"
code "Institutional": 'institutional' from "ClaimTypeCodes"
code "Professional": 'professional' from "ClaimTypeCodes"
code "Oral": 'oral' from "ClaimTypeCodes"
code "Vision": 'vision' from "ClaimTypeCodes"

// ActEncounterCodes
code "virtual": 'VR' from "ActCode"
code "ambulatory": 'AMB' from "ActCode"
code "home health": 'HH' from "ActCode"
code "inpatient non-acute": 'NONAC' from "ActCode"
code "emergency": 'EMER' from "ActCode"
code "inpatient acute": 'ACUTE' from "ActCode"

// Coverage-type
code "drug policy": 'DRUGPOL' from "ActCode"
code "mental health policy": 'MENTPOL' from "ActCode"
code "managed care policy": 'MCPOL' from "ActCode"
code "subsidized health program": 'SUBSIDIZ' from "ActCode"
code "retiree health program": 'RETIRE' from "ActCode"
code "substance use policy": 'SUBPOL' from "ActCode"

// Identifier Type Codes
code "Provider number": 'PRN' from "IdentifierType"