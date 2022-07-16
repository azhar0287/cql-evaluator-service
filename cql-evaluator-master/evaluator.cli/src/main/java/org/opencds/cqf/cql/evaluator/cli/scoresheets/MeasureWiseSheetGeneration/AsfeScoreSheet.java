package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
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

public class AsfeScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    void addAsfObjectInSheet(Document document, String payerCode, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("ASFA");
        sheetObjA.add(payerCode);
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjA.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Initial Population 1"))); //epop
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 1"))); //Numerator
        if(document.getBoolean("Exclusions 1")){
            sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add("0"); //RexlD
        sheetObjA.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet B Mapping ////////////////////////////////////////////////
        List<String> sheetObjB = new LinkedList<>();
        sheetObjB.add(document.getString("id"));
        sheetObjB.add("ASFB");
        sheetObjB.add(payerCode);
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 2")));
        //denominator 2 false
        if(!document.getBoolean("Denominator 2") || document.getBoolean("Exclusions 2") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjB.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Initial Population 2"))); //epop
        }
        //sheetObjB.add("Initial Population 2");
        sheetObjB.add("0"); //excl
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 2"))); //Numerator
        if(document.getBoolean("Exclusions 2")){
            sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 2"))); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjB.add("1");
        }
        else{
            sheetObjB.add("0");
        }
        sheetObjB.add("0"); //RexlD
        sheetObjB.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObjB.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjB);
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
    public void generateSheet(List<Document> documents, CSVPrinter csvPrinter, DBConnection db) {
        try {
            List<PayerInfo> payerInfoList;
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
//                if(document.getString("id").equals("95446")){
//                    int a=0;
//                }
                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                if(patientAge > 17 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList=mapPayersCodeInList(payerInfoList);
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            addAsfObjectInSheet(document,payerCode,csvPrinter);
                        }
                    }
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
