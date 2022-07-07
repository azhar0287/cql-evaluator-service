package org.opencds.cqf.cql.evaluator.cli.scoresheets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.mappers.PayerInfo;
import org.opencds.cqf.cql.evaluator.cli.util.Constant;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;

import java.io.IOException;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

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
                    utilityFunction.updatePayerCodesCCS(payerCodes, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

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

    public String getFieldCount(String fieldName, Document document) {
        int eventSum = 0;
        if(document.getBoolean(fieldName + " 1")) {
            eventSum+=1;
        }
        if(document.getBoolean(fieldName + " 2")) {
            eventSum+=1;
        }
        if(document.getBoolean(fieldName + " 3")) {
            eventSum+=1;
        }
        return String.valueOf(eventSum);

    }

    void addObjectInSheet(List<String> sheetObj, Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter) throws IOException {
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));


        sheetObj.add(getFieldCount("Event", document));   //event

        sheetObj.add(getFieldCount("Denominator", document)); //epop

        sheetObj.add("0"); //excl


        sheetObj.add(getFieldCount("Numerator", document)); //Num

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //rexcl

        sheetObj.add("0"); //RexlD
//        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions"))); //RexclId
        sheetObj.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);
    }

    String getPayerCodeType(String payerCode){
        return dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,DBConnection.getConnection()).get(0).getString("oid");
    }


    public void updatePayerCodesDMS(List<PayerInfo> payerCodes, DbFunctions dbFunctions, DBConnection db) {
        int flag1 = 0;
        int flag2 = 0;
        int flag3  = 0;
        Map<String,String> updateCodeMap;
        if(payerCodes.size() == 2 ) {
            updateCodeMap = assignCodeToTypeDMS(payerCodes, dbFunctions, db);
            for (PayerInfo payerInfo : payerCodes) {
                String codeType;
                if(updateCodeMap != null) {
                    codeType = updateCodeMap.get(payerInfo.getPayerCode());
                    if (codeType.equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        flag1 += 1;
                    }
                    if (codeType.equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        flag1 += 1;
                    }
                    if (codeType.equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        flag2 += 1;
                    }
                    if (codeType.equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        flag2 += 1;
                    }

                    if (codeType.equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        flag3 += 1;
                    }
                    if (codeType.equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        flag3 += 1;
                    }
                }
            }

            if(flag1 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : updateCodeMap.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        //payerCodes.add(entry.getKey());
                    }
                }
            }

            if(flag2 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : updateCodeMap.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        //PayerInfo payerInfo =
                        //payerCodes.add(updateCodeMap);
                    }
                }
            }

            if(flag3 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : updateCodeMap.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        //payerCodes.add(entry.getKey());
                    }
                }
            }
        }
    }

    public Map<String,String> assignCodeToTypeDMS(List<PayerInfo> payerCodes, DbFunctions db, DBConnection connection) {
        //DBConnection db = new DBConnection();
        Map<String,String> updateCodeMap = new HashMap<>();
        String oid;
        for (PayerInfo payerInfo : payerCodes) {
            //Document document = db.getOidInfo(pCode, "dictionary_ep_2022_code");
            List<Document> documents = db.getOidInfo(payerInfo.getPayerCode(), "dictionary_ep_2022_code", connection);
            String pCode = payerInfo.getPayerCode();
            Document document;
            if(documents.size() > 0) {
                document = documents.get(0);
                if(document != null) {
                    oid = (String) document.get("oid");
                    if(oid.equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        updateCodeMap.put(pCode, CODE_TYPE_COMMERCIAL);
                        updateCodeMap.put("coverageStartDateString",payerInfo.getCoverageStartDateString());
                        updateCodeMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
                    }
                    if(oid.equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        updateCodeMap.put(pCode, CODE_TYPE_MEDICAID);
                        updateCodeMap.put("coverageStartDateString",payerInfo.getCoverageStartDateString());
                        updateCodeMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
                    }
                    if(oid.equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        updateCodeMap.put(pCode, CODE_TYPE_MEDICARE);
                        updateCodeMap.put("coverageStartDateString",payerInfo.getCoverageStartDateString());
                        updateCodeMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
                    }
                }
            }

            if(pCode.equalsIgnoreCase("MCD")) {
                updateCodeMap.put(pCode, CODE_TYPE_MEDICAID);
                updateCodeMap.put("coverageStartDateString",payerInfo.getCoverageStartDateString());
                updateCodeMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
            }
        }
        return updateCodeMap;
    }

}
