package org.opencds.cqf.cql.evaluator.cli.util;

public class Constant {
   public static final String CODE_TYPE_COMMERCIAL = "HEDIS.Commercial.Custom.Codes.22";
   public static final  String CODE_TYPE_MEDICAID = "HEDIS.Medicaid.Custom.Codes.22";
   public static final String CODE_TYPE_MEDICARE = "HEDIS.Medicare.Custom.Codes.22";
   public static final String FOLDER_NAME="/CCS_HEDIS_MY2022";
 private static final String testResourceRelativePath = "resources";  //for jar
//   private static final String testResourceRelativePath = "evaluator.cli/src/main/resources"; //for local processing

   /*Variables of Engine*/
   public static final String FHIR_VERSION = "R4";
   public static final String MODEL = "FHIR";
   public static final String MODEL_URL = testResourceRelativePath + FOLDER_NAME;
   public static final String TERMINOLOGY = testResourceRelativePath + FOLDER_NAME + "/vocabulary/ValueSet";
   public static final String LIBRARY_URL = testResourceRelativePath + FOLDER_NAME;
   public static final String CONTEXT = "Patient";
   public static final String LIBRARY_NAME = "CCS_HEDIS_MY2022";

}
