package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.mappers.PayerInfo;

import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class AddEScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    public boolean payerAnchorDateFlag(List<PayerInfo> payerInfoList) {
        Date anchorDate = utilityFunction.getConvertedDate("2022-12-31");
        Date endDate = utilityFunction.getConvertedDate("2022-01-01");
        boolean rexcleFlag = false;
        if (payerInfoList.size() > 0) {
            for (PayerInfo payerInfo : payerInfoList) {
                boolean dateOverLapFlag = anchorDate.compareTo(payerInfo.getCoverageStartDate()) >= 0
                        && payerInfo.getCoverageEndDate().compareTo(endDate) >= 0;
                rexcleFlag = dateOverLapFlag;
            }
        }
        return rexcleFlag;
    }

    public String getAnchorDate(Date date,int days){
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_YEAR, + days);
        Date myDate = cal.getTime();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(myDate);
    }


    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) {
        try {
            Date indexPrescriptionStartDate;
            List<String> sheetObj;
            List<PayerInfo> payerInfoList;
            int age;
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                if(document.getString("Index Prescription Start Date")!= null) {
                    indexPrescriptionStartDate = utilityFunction.getConvertedDate(document.getString("Index Prescription Start Date"));

                    String anchorDate =  getAnchorDate(indexPrescriptionStartDate, 30);
                    age = Integer.parseInt(utilityFunction.getAge(document.getDate("birthDate"), utilityFunction.getConvertedDate("2022-02-28" )));

                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList = utilityFunction.mapPayersCodeAddE(payerInfoList, anchorDate);
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if(age>5 && age<13 && payersList.size() != 0) {
                        for (int i = 0; i < payersList.size(); i++) {
                            sheetObj = new ArrayList<>();
                            sheetObj.add(document.getString("id"));
                            sheetObj.add("ADD1"); //Measure
                            String payerCode = payersList.get(i);
                            sheetObj.add(String.valueOf(payerCode)); //payer
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period 1")));
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 1"))); //event
                            if (document.getBoolean("Enrolled During Participation Period 1")
                                    && document.getBoolean("Denominator 1") && !document.getBoolean("Exclusions 1")) {
                                sheetObj.add("1"); //epop
                            } else {
                                sheetObj.add("0"); //epop
                            }

                            sheetObj.add("0"); //exc

                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 1"))); //Numenator

                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //Rexl

                            sheetObj.add("0"); //RexclId

                            sheetObj.add(String.valueOf(age));

                            sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));

                            csvPrinter.printRecord(sheetObj);
                        }
                    }

                    anchorDate =  getAnchorDate(indexPrescriptionStartDate, 300);

                    age = Integer.parseInt(utilityFunction.getAge(document.getDate("birthDate"), utilityFunction.getConvertedDate("2022-02-28" )));
                    payersList = utilityFunction.mapPayersCodeAddE(payerInfoList, anchorDate);
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if(age > 5 && age < 13 &&  payersList.size() != 0) {
                        for (int i = 0; i < payersList.size(); i++) {
                            sheetObj = new ArrayList<>();
                            sheetObj.add(document.getString("id"));
                            sheetObj.add("ADD2"); //Measure
                            String payerCode = payersList.get(i);
                            sheetObj.add(String.valueOf(payerCode));
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period 2")));
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 2"))); //event
                            if(document.getBoolean("Enrolled During Participation Period 2")
                                    && document.getBoolean("Denominator 2") && !document.getBoolean("Exclusions 2")) {
                                sheetObj.add("1"); //epop
                            }
                            else {
                                sheetObj.add("0"); //epop
                            }

                            sheetObj.add("0"); //exc

                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 2"))); //Numenator

                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 2"))); //Rexl

                            sheetObj.add("0"); //RexclId

                            sheetObj.add(String.valueOf(age));

                            sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));

                            csvPrinter.printRecord(sheetObj);
                        }
                    }
                    csvPrinter.flush();
                }
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


    public void updatePayerCodes(List<String> payerCodes, DbFunctions dbFunctions, DBConnection db) {
        int flag1 = 0;
        int flag2 = 0;
        int flag3  = 0;
        List<String> updatedPayersList=new LinkedList<>();
        Map<String,String> codeTypes;
        if(payerCodes.size() == 2) {
            codeTypes = utilityFunction.assignCodeToType(payerCodes,dbFunctions, db);
            for (String code : payerCodes) {
                String codeType;
                if(codeTypes != null) {
                    codeType = codeTypes.get(code);
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
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());
                    }
                }
            }

            if(flag2 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());

                    }
                }
            }

            if(flag3 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        mapSpecialPayerCodes(updatedPayersList, entry.getKey());
                }   }
            }
            payerCodes.clear();
            payerCodes.addAll(updatedPayersList);
        }
        else {
            for(String payerCode:payerCodes) {
                mapSpecialPayerCodes(updatedPayersList,payerCode);
            }
            payerCodes.clear();
            payerCodes.addAll(updatedPayersList);
        }
    }
    void mapSpecialPayerCodes(List<String> payersList,String payerCode){
        if(payerCode.equals("MD") || payerCode.equals("MDE") || payerCode.equals("MLI")|| payerCode.equals("MRB")){
            payersList.add("MCD");
        }
        else if(payerCode.equals("SN1") || payerCode.equals("SN2") || payerCode.equals("SN3")){
            payersList.add("MCR");
        }
        else if(payerCode.equals("MMP")){
            payersList.add("MCD");
//            payersList.add("MCR");
        }
        else{
            payersList.add(payerCode);
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
