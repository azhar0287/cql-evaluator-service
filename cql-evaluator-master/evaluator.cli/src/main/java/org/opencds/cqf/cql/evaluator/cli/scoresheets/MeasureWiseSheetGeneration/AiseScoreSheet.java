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
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class AiseScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();





    void mapZosterImmunization(Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter) throws IOException {
        List<String> sheetObj  = new ArrayList<>();
        sheetObj.add(document.getString("id"));
        sheetObj.add("AISZOS"); //Measure
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));


        sheetObj.add("0");   //event

//        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//            sheetObj.add("0"); //epop
//        }
//        else {
//            sheetObj.add(utilityFunction.getFieldCount("Denominator", document)); //epop
//
//        }

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 3"))); //epop

        sheetObj.add("0"); //excl


        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 3"))); //Num

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 3"))); //rexcl

        sheetObj.add("0"); //RexlD
        sheetObj.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);
    }

    void mapPneumococcalImmunization(Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter) throws IOException {
        List<String> sheetObj  = new ArrayList<>();
        sheetObj.add(document.getString("id"));
        sheetObj.add("AISPNEU"); //Measure
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));


        sheetObj.add("0");   //event

//        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//            sheetObj.add("0"); //epop
//        }
//        else {
//            sheetObj.add(utilityFunction.getFieldCount("Denominator", document)); //epop
//
//        }

        sheetObj.add((utilityFunction.getIntegerString(document.getBoolean("Denominator 4")))); //epop

        sheetObj.add("0"); //excl


        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 4"))); //Num

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 4"))); //rexcl

        sheetObj.add("0"); //RexlD
        sheetObj.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);
    }
    void addObjectInSheet(String payerCodeType, int patientAge,Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter) throws IOException {

        List<String> sheetObj  = new ArrayList<>();
        sheetObj.add(document.getString("id"));
        sheetObj.add("AISINFL"); //Measure
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");

        sheetObj.add("0");   //event

//        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//            sheetObj.add("0"); //epop
//        }
//        else {
//            sheetObj.add(utilityFunction.getFieldCount("Denominator", document)); //epop
//
//        }

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 1"))); //epop

        sheetObj.add("0"); //excl


        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 1"))); //Num

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //rexcl

        sheetObj.add("0"); //RexlD
        sheetObj.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);

        ////////////////////////////AISTD
        List<String> sheetObj2  = new ArrayList<>();
        sheetObj2.add(document.getString("id"));
        sheetObj2.add("AISTD"); //Measure
        sheetObj2.add(payerCode);
        sheetObj2.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));


        sheetObj2.add("0");   //event

//        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//            sheetObj.add("0"); //epop
//        }
//        else {
//            sheetObj.add(utilityFunction.getFieldCount("Denominator", document)); //epop
//
//        }

        sheetObj2.add((utilityFunction.getIntegerString(document.getBoolean("Denominator 2")))); //epop

        sheetObj2.add("0"); //excl


        sheetObj2.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 2"))); //Num

        sheetObj2.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 2"))); //rexcl

        sheetObj2.add("0"); //RexlD
        sheetObj2.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj2.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj2);

        if(patientAge>=50 && patientAge<=65 && !payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            mapZosterImmunization(document,payerCode,measureDate,csvPrinter);
        }
        else if(patientAge>=66){
            mapZosterImmunization(document,payerCode,measureDate,csvPrinter);
        }

        if(patientAge>=66 && payerCodeType.equals(CODE_TYPE_MEDICARE)){
            mapPneumococcalImmunization(document,payerCode,measureDate,csvPrinter);
        }

    }

    String getPayerCodeType(String payerCode ,DBConnection dbConnection){
        if(dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,dbConnection).size()>0){
            return  dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,DBConnection.getConnection()).get(0).getString("oid");
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
//                    mapSpecialPayerCodes(payersList,payerInfoList.get(i).getPayerCode());
                    payersList.add(payerInfoList.get(i).getPayerCode());
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
//                            mapSpecialPayerCodes(payersList, payerInfoList.get(i).getPayerCode());
                            payersList.add(payerInfoList.get(i).getPayerCode());
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
//                        payerCodes.add(entry.getKey());
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());
                    }
                }
            }

            if(flag2 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
//                        payerCodes.add(entry.getKey());
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());

                    }
                }
            }

            if(flag3 == 2) {
                payerCodes.clear();
                for (Map.Entry<String, String> entry : codeTypes.entrySet()) {

//                        payerCodes.add(entry.getKey());
                    mapSpecialPayerCodes(updatedPayersList,entry.getKey());


                }
            }
            payerCodes.clear();
            payerCodes.addAll(updatedPayersList);
        }
        else{
            for(String payerCode:payerCodes){
                mapSpecialPayerCodes(updatedPayersList,payerCode);
            }
            payerCodes.clear();
            payerCodes.addAll(updatedPayersList);
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
                if(document.getString("id").equals("95005")){
                    int a = 0;
                }
                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                if(patientAge>11 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList=mapPayersCodeInList(payerInfoList);

                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    if (payersList.size() != 0) {
                        Boolean flag=false;
                        for (String payerCode:payersList) {
                            flag=false;
                            String payerCodeType = getPayerCodeType(payerCode,db);
                            if (((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) && patientAge>18 && patientAge<66)
                                    || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) && patientAge > 65)){

                                addObjectInSheet(payerCodeType,patientAge,document,payerCode,measureDate,csvPrinter);
                                flag=true;
                            }
                        }
                        if(flag==false){
                            Main.failedPatients.add(document.getString("id"));
                        }
                    }
                    else {
//                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }
                else{
//                    Main.failedPatients.add(document.getString("id"));
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
