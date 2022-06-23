package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

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
import java.text.SimpleDateFormat;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;
import static org.opencds.cqf.cql.evaluator.cli.util.Constant.CODE_TYPE_MEDICAID;

public class DmseScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();



    public String getFieldCount(String fieldName, Document document) {
        int eventSum = 0;
        String feildname=fieldName.concat(" 1");
        if(document.getBoolean(fieldName.concat(" 1") )) {
            eventSum+=1;
        }
        if(document.getBoolean(fieldName.concat(" 2") )) {
            eventSum+=1;
        }
        if(document.getBoolean(fieldName.concat(" 3") )) {
            eventSum+=1;
        }
        return String.valueOf(eventSum);

    }

    void addObjectInSheet(List<String> sheetObj, Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter) throws IOException {
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");

        sheetObj.add(getFieldCount("Event", document));   //event

        if(document.getBoolean("Exclusions 1") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode))){
            sheetObj.add("0"); //epop
        }
        else {
            sheetObj.add(getFieldCount("Denominator", document)); //epop

        }

        sheetObj.add("0"); //excl


        sheetObj.add(getFieldCount("Numerator", document)); //Num

        
        if(document.getBoolean("Exclusions 1")){
            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObj.add("1");
        }
        else{
            sheetObj.add("0");
        }


        sheetObj.add("0"); //RexlD
        sheetObj.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);
    }

    String getPayerCodeType(String payerCode ,DBConnection dbConnection){
        if(dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,dbConnection).size()>0){
           return  dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,new DBConnection()).get(0).getString("oid");
        }
        return "";
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



    void mapSpecialPayerCodes(List<String> payersList,String payerCode){
        if(payerCode.equals("MD") || payerCode.equals("MDE") || payerCode.equals("MLI")|| payerCode.equals("MRB")){
            payersList.add("MCD");
        }
        else if(payerCode.equals("SN1") || payerCode.equals("SN2") || payerCode.equals("SN3")){
            payersList.add("MCR");
        }
        else if(payerCode.equals("MMP")){
            payersList.add("MCD");
            payersList.add("MCR");
        }
        else{
            payersList.add(payerCode);
        }
    }

    public List<String> mapPayersCodeInList(List<PayerInfo> payerInfoList){
        List<String> payersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {
            Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat("2022-12-31", "yyyy-MM-dd");
            Date insuranceEndDate = null,insuranceStartDate=null;
            for (int i = 0; i < payerInfoList.size(); i++) {
                insuranceEndDate = payerInfoList.get(i).getCoverageEndDate();
                insuranceStartDate=payerInfoList.get(i).getCoverageStartDate();
                if (null!=insuranceEndDate && insuranceEndDate.compareTo(measurementPeriodEndingDate) >= 0 && !payerInfoList.get(i).getCoverageStartDateString().equals("20240101") && !(insuranceStartDate.compareTo(measurementPeriodEndingDate) > 0)) {
//                    payersList.add(insuranceList.get(i).getPayerCode());
                    mapSpecialPayerCodes(payersList,payerInfoList.get(i).getPayerCode());
                }
            }

            //If no payer matches the above condition than the recent payer code in appended in payerlist
            //Commenting as Faizan bhai said.
            if (payersList.isEmpty() || payersList.size() == 0) {

                for(int i=payerInfoList.size()-1;i>-1;i--) {

                    String lastCoverageObjectStartDate = payerInfoList.get(i).getCoverageStartDateString();
                    String lastCoverageObjectEndDate = payerInfoList.get(i).getCoverageEndDateString();
                    if ((null != lastCoverageObjectStartDate) && (null != lastCoverageObjectEndDate)) {

                        if (!lastCoverageObjectStartDate.equals("20240101") && (lastCoverageObjectEndDate.substring(0, 4).equals("2022"))) {
                            mapSpecialPayerCodes(payersList, payerInfoList.get(i).getPayerCode());
                            break;
                        }

                    }
                }

            }
        }
        return payersList;
    }

    public void updatePayerCodes(List<String> payerCodes, DbFunctions dbFunctions, DBConnection db) {
        int flag1 = 0;
        int flag2 = 0;
        int flag3  = 0;
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
                        payerCodes.add(entry.getKey());
                    }
                }
            }

            if(flag2 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        payerCodes.add(entry.getKey());
                    }
                }
            }

            if(flag3 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        payerCodes.add(entry.getKey());
                    }
                }
            }
        }
    }
    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) throws IOException {
        try {
            String globalPatientId;
            List<String> sheetObj;
            List<PayerInfo> payerInfoList;
            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                if(document.getString("id").equals("95083")){
                    int a=0;
                }
                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                if(patientAge>11 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList=mapPayersCodeInList(payerInfoList);
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            String payerCodeType = getPayerCodeType(payerCode,db);
                            if (((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) && patientAge>11)
                                    || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) && patientAge > 17)){
                                sheetObj = new ArrayList<>();
                                sheetObj.add(document.getString("id"));
                                sheetObj.add("DMS"); //Measure
                                addObjectInSheet(sheetObj,document,payerCode,measureDate,csvPrinter);
                            }
                        }
                    }
                    else {
//                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }
                else{
                    Main.failedPatients.add(document.getString("id"));
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
