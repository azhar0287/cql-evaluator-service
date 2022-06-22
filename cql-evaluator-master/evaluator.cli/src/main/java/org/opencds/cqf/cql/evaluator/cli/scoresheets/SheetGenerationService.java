package org.opencds.cqf.cql.evaluator.cli.scoresheets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SheetGenerationService {
    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();


    public void generateSheetForCCS(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) throws IOException {
        try {
            String globalPatientId;
            List<String> sheetObj;
            List<String> payerCodes;
            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                int age=Integer.parseInt(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
                if((age>23 && age<65) && utilityFunction.getGenderSymbol(document.getString("gender") ).equalsIgnoreCase("F")) {
                    Object object = document.get("payerCodes");
                    payerCodes = new ObjectMapper().convertValue(object, new TypeReference<List<String>>() {
                    });
                    utilityFunction.updatePayerCodes(payerCodes, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if (payerCodes.size() != 0) {


                        for (int i = 0; i < payerCodes.size(); i++) {
                            sheetObj = new ArrayList<>();
                            sheetObj.add(document.getString("id"));
                            sheetObj.add("CCS"); //Measure
                            String payerCode = payerCodes.get(i);
                            sheetObj.add(String.valueOf(payerCode));
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period For CE")));
                            sheetObj.add("0"); //event
                            if (document.getBoolean("Exclusions") || codeCheckList.stream().anyMatch(str -> str.trim().equals(payerCode))) {
                                sheetObj.add("0"); //Epop
                            } else {
                                sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator"))); //Epop
                            }
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator Exceptions"))); //exc
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator")));
                            sheetObj.add("0"); //Rexl
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions"))); //RexclId
                            sheetObj.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
                            sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
                            csvPrinter.printRecord(sheetObj);
                        }
                    }
                    else {
                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }
                else{
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void generateSheetForDMSE(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) throws IOException {
        try {
            String globalPatientId;
            List<String> sheetObj;
            List<String> payerCodes;
            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                int age=Integer.parseInt(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
                if((age>23 && age<65) && utilityFunction.getGenderSymbol(document.getString("gender") ).equalsIgnoreCase("F")) {
                    Object object = document.get("payerCodes");
                    payerCodes = new ObjectMapper().convertValue(object, new TypeReference<List<String>>() {});
                    utilityFunction.updatePayerCodes(payerCodes, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if (payerCodes.size() != 0) {
                        for (int i = 0; i < payerCodes.size(); i++) {
                            sheetObj = new ArrayList<>();
                            sheetObj.add(document.getString("id"));
                            sheetObj.add("CCS"); //Measure
                            String payerCode = payerCodes.get(i);
                            sheetObj.add(String.valueOf(payerCode));
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period For CE")));
                            sheetObj.add("0"); //event
                            if (document.getBoolean("Exclusions") || codeCheckList.stream().anyMatch(str -> str.trim().equals(payerCode))) {
                                sheetObj.add("0"); //Epop
                            } else {
                                sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator"))); //Epop
                            }
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator Exceptions"))); //exc
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator")));
                            sheetObj.add("0"); //Rexl
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions"))); //RexclId
                            sheetObj.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
                            sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
                            csvPrinter.printRecord(sheetObj);
                        }
                    }
                    else {
                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }
                else{
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
