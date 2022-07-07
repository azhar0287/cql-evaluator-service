package org.opencds.cqf.cql.evaluator.cli.util;

public class Constant {
   public static final String CODE_TYPE_COMMERCIAL = "HEDIS.Commercial.Custom.Codes.22";
   public static final  String CODE_TYPE_MEDICAID = "HEDIS.Medicaid.Custom.Codes.22";
   public static final String CODE_TYPE_MEDICARE = "HEDIS.Medicare.Custom.Codes.22";
   public static final String FOLDER_NAME= "/PDSE_HEDIS_MY2022";
// public static final String testResourceRelativePath = "resources";  //for jar
   public static final String testResourceRelativePath = "evaluator.cli/src/main/resources"; //for local processing

   /*Variables of Engine*/
   public static final String FHIR_VERSION = "R4";
   public static final String MODEL = "FHIR";
   public static final String MODEL_URL = testResourceRelativePath + FOLDER_NAME;
   public static final String TERMINOLOGY = testResourceRelativePath + FOLDER_NAME + "/vocabulary/ValueSet";
   public static final String LIBRARY_URL = testResourceRelativePath + FOLDER_NAME;
   public static final String CONTEXT = "Patient";
   public static final String LIBRARY_NAME = "PDSE_HEDIS_MY2022";

   public static final String EP_DICTIONARY = "dictionary_ep_2022_code";


   public static final String MAIN_FHIR_COLLECTION_NAME = "ep_encounter_fhir_PDSE_Sample_Deck";
   public static final String FHIR_UNPROCESSED_COLLECTION_NAME = "PDSE_Sample_Deck_Fhir_Unprocessed_Patients";

}
