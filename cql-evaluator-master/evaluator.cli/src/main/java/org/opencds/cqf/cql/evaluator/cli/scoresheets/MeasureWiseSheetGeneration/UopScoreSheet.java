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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class UopScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    void addUopObjectInSheet(Document document, String payerCode, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("UOPA");
        sheetObjA.add(payerCode);
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 1")));// Added Event Here
        if(!document.getBoolean("Enrolled During Participation Period") ||!document.getBoolean("Denominator 1") || document.getBoolean("Exclusions 1")
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Initial Population 1"))); //epop
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 1"))); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getBoolean("Exclusions 1")){
            sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(document.get("Age").toString());
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet B Mapping ////////////////////////////////////////////////
        List<String> sheetObjB = new LinkedList<>();
        sheetObjB.add(document.getString("id"));
        sheetObjB.add("UOPB");
        sheetObjB.add(payerCode);
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 2")));
        //denominator 2 false
        if(!document.getBoolean("Enrolled During Participation Period") ||!document.getBoolean("Denominator 2") || document.getBoolean("Exclusions 2")
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjB.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Initial Population 2"))); //epop
        }
        //sheetObjB.add("Initial Population 2");
        sheetObjB.add("0"); //excl
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 2"))); //Numerator
        sheetObjB.add("0"); //rexcl
        if(document.getBoolean("Exclusions 2")){
            sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 2"))); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjB.add("1");
        }
        else{
            sheetObjB.add("0");
        }
        sheetObjB.add(document.get("Age").toString());
        sheetObjB.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjB);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet C Mapping ////////////////////////////////////////////////
        List<String> sheetObjC = new LinkedList<>();
        sheetObjC.add(document.getString("id"));
        sheetObjC.add("UOPC");
        sheetObjC.add(payerCode);
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 3")));
        if(!document.getBoolean("Enrolled During Participation Period") ||!document.getBoolean("Denominator 3") || document.getBoolean("Exclusions 3")
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjC.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Initial Population 3"))); //epop
        }
        sheetObjC.add("0"); //excl
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 3"))); //Numerator
        sheetObjC.add("0"); //rexcl
        if(document.getBoolean("Exclusions 3")){
            sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 3"))); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjC.add("1");
        }
        else{
            sheetObjC.add("0");
        }
        sheetObjC.add(document.get("Age").toString());
        sheetObjC.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjC);
    }

    String getPayerCodeType(String payerCode , DBConnection dbConnection, Map<String,String> dictionaryStringMap){
        if(!dictionaryStringMap.isEmpty()){
            String payerCodeOid=dictionaryStringMap.get(payerCode);
            if(payerCodeOid !=null && payerCodeOid!=""){
                return payerCodeOid;
            }
        }
        List<Document> documentList=dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,dbConnection);
        if(documentList.size()>0){
            dictionaryStringMap.put(payerCode,documentList.get(0).getString("oid"));
            return documentList.get(0).getString("oid");
        }
        return "";
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
    public void generateSheet(List<Document> documents, CSVPrinter csvPrinter, DBConnection db,Map<String,String> dictionaryStringMap) throws IOException {
        try {

            List<PayerInfo> payerInfoList;
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
//                if(document.getString("id").equals("95446")){
//                    int a=0;
//                }
                int patientAge = document.getInteger("Age");
                if(patientAge>18 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList=mapPayersCodeInList(payerInfoList);
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                                addUopObjectInSheet(document,payerCode,csvPrinter);
                        }
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
