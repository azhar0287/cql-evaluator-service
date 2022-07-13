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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class CisScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    String getPayerCodeType(String payerCode ,DBConnection dbConnection,Map<String,String> dictionaryStringMap){
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

    void addCISObjectInSheet(Document document, String payerCode,Date measureDate, CSVPrinter csvPrinter,Map<String,String> dictionaryStringMap,DBConnection db) throws IOException {
        String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet CISDTP Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("CISDTP");
        sheetObjA.add(payerCode);
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjA.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 1")||payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 1"))); //epop
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 1"))); //Numerator
        sheetObjA.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 1"))); //rexcl
        sheetObjA.add("0"); //RexlD
        sheetObjA.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISOPV Mapping ////////////////////////////////////////////////
        List<String> sheetObjB = new LinkedList<>();
        sheetObjB.add(document.getString("id"));
        sheetObjB.add("CISOPV");
        sheetObjB.add(payerCode);
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjB.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 2") ||payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjB.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 2"))); //epop
        }
        sheetObjB.add("0"); //excl
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 2"))); //Numerator
        sheetObjB.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 2"))); //rexcl
        sheetObjB.add("0"); //RexlD
        sheetObjB.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjB.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjB);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISMMR Mapping ////////////////////////////////////////////////
        List<String> sheetObjC = new LinkedList<>();
        sheetObjC.add(document.getString("id"));
        sheetObjC.add("CISMMR");
        sheetObjC.add(payerCode);
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjC.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 3")|| payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjC.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 3"))); //epop
        }
        sheetObjC.add("0"); //excl
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 3"))); //Numerator
        sheetObjC.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 3"))); //rexcl
        sheetObjC.add("0"); //RexlD
        sheetObjC.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjC.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjC);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISHIB Mapping ////////////////////////////////////////////////
        List<String> sheetObjD = new LinkedList<>();
        sheetObjD.add(document.getString("id"));
        sheetObjD.add("CISHIB");
        sheetObjD.add(payerCode);
        sheetObjD.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjD.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 4") || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjD.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjD.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 4"))); //epop
        }
        sheetObjD.add("0"); //excl
        sheetObjD.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 4"))); //Numerator
        sheetObjD.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 4"))); //rexcl
        sheetObjD.add("0"); //RexlD
        sheetObjD.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjD.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjD);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISHEPB Mapping ////////////////////////////////////////////////
        List<String> sheetObjE = new LinkedList<>();
        sheetObjE.add(document.getString("id"));
        sheetObjE.add("CISHEPB");
        sheetObjE.add(payerCode);
        sheetObjE.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjE.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 5") || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjE.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjE.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 5"))); //epop
        }
        sheetObjE.add("0"); //excl
        sheetObjE.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 5"))); //Numerator
        sheetObjE.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 5"))); //rexcl
        sheetObjE.add("0"); //RexlD
        sheetObjE.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjE.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjE);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISVZV Mapping ////////////////////////////////////////////////
        List<String> sheetObjF = new LinkedList<>();
        sheetObjF.add(document.getString("id"));
        sheetObjF.add("CISVZV");
        sheetObjF.add(payerCode);
        sheetObjF.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjF.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 6") || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjF.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjF.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 6"))); //epop
        }
        sheetObjF.add("0"); //excl
        sheetObjF.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 6"))); //Numerator
        sheetObjF.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 6"))); //rexcl
        sheetObjF.add("0"); //RexlD
        sheetObjF.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjF.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjF);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISPNEU Mapping ////////////////////////////////////////////////
        List<String> sheetObjG = new LinkedList<>();
        sheetObjG.add(document.getString("id"));
        sheetObjG.add("CISPNEU");
        sheetObjG.add(payerCode);
        sheetObjG.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjG.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 7") || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjG.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjG.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 7"))); //epop
        }
        sheetObjG.add("0"); //excl
        sheetObjG.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 7"))); //Numerator
        sheetObjG.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 7"))); //rexcl
        sheetObjG.add("0"); //RexlD
        sheetObjG.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjG.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjG);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISHEPA Mapping ////////////////////////////////////////////////
        List<String> sheetObjH = new LinkedList<>();
        sheetObjH.add(document.getString("id"));
        sheetObjH.add("CISHEPA");
        sheetObjH.add(payerCode);
        sheetObjH.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjH.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 8")|| payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjH.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjH.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 8"))); //epop
        }
        sheetObjH.add("0"); //excl
        sheetObjH.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 8"))); //Numerator
        sheetObjH.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 8"))); //rexcl
        sheetObjH.add("0"); //RexlD
        sheetObjH.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjH.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjH);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISROTA Mapping ////////////////////////////////////////////////
        List<String> sheetObjI = new LinkedList<>();
        sheetObjI.add(document.getString("id"));
        sheetObjI.add("CISROTA");
        sheetObjI.add(payerCode);
        sheetObjI.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjI.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 9")  || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjI.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjI.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 9"))); //epop
        }
        sheetObjI.add("0"); //excl
        sheetObjI.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 9"))); //Numerator
        sheetObjI.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 9"))); //rexcl
        sheetObjI.add("0"); //RexlD
        sheetObjI.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjI.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjI);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISINFL Mapping ////////////////////////////////////////////////
        List<String> sheetObjJ = new LinkedList<>();
        sheetObjJ.add(document.getString("id"));
        sheetObjJ.add("CISINFL");
        sheetObjJ.add(payerCode);
        sheetObjJ.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjJ.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 10")  || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjJ.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjJ.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 10"))); //epop
        }
        sheetObjJ.add("0"); //excl
        sheetObjJ.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 10"))); //Numerator
        sheetObjJ.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 10"))); //rexcl
        sheetObjJ.add("0"); //RexlD
        sheetObjJ.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjJ.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjJ);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISCMB3 Mapping ////////////////////////////////////////////////
        if(!codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode))){
            List<String> sheetObjK = new LinkedList<>();
            sheetObjK.add(document.getString("id"));
            sheetObjK.add("CISCMB3");
            sheetObjK.add(payerCode);
            sheetObjK.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
            sheetObjK.add("0"); // Added Event Here
            if(document.getBoolean("Exclusions 11") || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
                sheetObjK.add("0"); //epop (Also known as denominator)
            }
            else {
                sheetObjK.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 11"))); //epop
            }
            sheetObjK.add("0"); //excl
            sheetObjK.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 11"))); //Numerator
            sheetObjK.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 11"))); //rexcl
            sheetObjK.add("0"); //RexlD
            sheetObjK.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
            sheetObjK.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjK);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISCMB7 Mapping ////////////////////////////////////////////////
        if(!codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode))){
            List<String> sheetObjL = new LinkedList<>();
            sheetObjL.add(document.getString("id"));
            sheetObjL.add("CISCMB7");
            sheetObjL.add(payerCode);
            sheetObjL.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
            sheetObjL.add("0"); // Added Event Here
            if(document.getBoolean("Exclusions 12")  || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
                sheetObjL.add("0"); //epop (Also known as denominator)
            }
            else {
                sheetObjL.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 12"))); //epop
            }
            sheetObjL.add("0"); //excl
            sheetObjL.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 12"))); //Numerator
            sheetObjL.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 12"))); //rexcl
            sheetObjL.add("0"); //RexlD
            sheetObjL.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
            sheetObjL.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjL);
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  Sheet CISCMB10 Mapping ////////////////////////////////////////////////
        List<String> sheetObjM = new LinkedList<>();
        sheetObjM.add(document.getString("id"));
        sheetObjM.add("CISCMB10");
        sheetObjM.add(payerCode);
        sheetObjM.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        sheetObjM.add("0"); // Added Event Here
        if(document.getBoolean("Exclusions 13")  || payerCodeType.equals(Constant.CODE_TYPE_MEDICARE)){
            sheetObjM.add("0"); //epop (Also known as denominator)
        }
        else {
            sheetObjM.add(utilityFunction.getIntegerString(document.getBoolean("Denominator 13"))); //epop
        }
        sheetObjM.add("0"); //excl
        sheetObjM.add(utilityFunction.getIntegerString(document.getBoolean("Numerator 13"))); //Numerator
        sheetObjM.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions 13"))); //rexcl
        sheetObjM.add("0"); //RexlD
        sheetObjM.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
        sheetObjM.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjM);
        ///////////////////////////////////////////////////////////////////////////////////////////////
    }

    public String getAnchorDate(Date date,int year){
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, year);
        Date myDate = cal.getTime();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(myDate);
    }

    public List<String> mapPayersCodeInList(List<PayerInfo> payerInfoList,Date birthDate){
        /// Add the 2 year in the birthdate that will be my anchor date
        String anchorDate=getAnchorDate(birthDate,2);
        List<String> payersList=new LinkedList<>();
        List<PayerInfo> tempPayersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {
            Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat(anchorDate, "yyyy-MM-dd");
            for (PayerInfo payerInfo : payerInfoList) {
                Date insuranceEndDate = payerInfo.getCoverageEndDate();
                Date insuranceStartDate = payerInfo.getCoverageStartDate();
                if (insuranceEndDate != null  && insuranceEndDate.compareTo(measurementPeriodEndingDate) >= 0 &&
                        !(insuranceStartDate.compareTo(measurementPeriodEndingDate) > 0)) {
                    payersList.add(payerInfo.getPayerCode());
                }
            }

            //If no payer matches the above condition than get previous date from and check the range within the anchordate and send the latest POS back
            if (payersList.isEmpty()) {
                String oneYearAnchorDate=getAnchorDate(birthDate,1);
                Date measurementPeriodStartingDate = UtilityFunction.getParsedDateInRequiredFormat(oneYearAnchorDate, "yyyy-MM-dd");
                for (PayerInfo payerInfo : payerInfoList) {
                    Date insuranceEndDate = payerInfo.getCoverageEndDate();
                    Date insuranceStartDate = payerInfo.getCoverageStartDate();

                    //It returns the value 0 if the argument Date is equal to this Date.
                    //It returns a value less than 0 if this Date is before the Date argument.
                    //It returns a value greater than 0 if this Date is after the Date argument.
                    int oneYearBefore= insuranceStartDate.compareTo(measurementPeriodStartingDate);
                    int twoYearBefore= insuranceStartDate.compareTo(measurementPeriodEndingDate);

                    int oneYearEndBefore= insuranceEndDate.compareTo(measurementPeriodStartingDate);
                    int twoYearEndBefore= insuranceEndDate.compareTo(measurementPeriodEndingDate);
                    if( (oneYearBefore>=0 && twoYearBefore<0) || (oneYearEndBefore>=0 && twoYearEndBefore<0)){
                        tempPayersList.add(payerInfo);
                    }
                }
                if(tempPayersList.size()>0){
                    int size=tempPayersList.size()-1;
                    payersList.add(tempPayersList.get(size).getPayerCode());
                }
            }
        }
        return payersList;
    }

    void mapSpecialPayerCodes(List<String> payersList,String payerCode){
        if(payerCode.equals("MD") || payerCode.equals("MDE") || payerCode.equals("MLI")|| payerCode.equals("MRB") || payerCode.equals("MMP")){
            payersList.add("MCD");
        }
        else if(payerCode.equals("SN1") || payerCode.equals("SN2") || payerCode.equals("SN3")){
            payersList.add("MCR");
        }
        else{
            payersList.add(payerCode);
        }
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
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());
                    }
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

    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db,Map<String,String> dictionaryStringMap) throws IOException {
        try {
            List<PayerInfo> payerInfoList;
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                int patientAge = Integer.parseInt(utilityFunction.getAge(document.getDate("birthDate"),measureDate));
                if(patientAge == 2) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    List<String> payersList=mapPayersCodeInList(payerInfoList,document.getDate("birthDate"));
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            addCISObjectInSheet(document,payerCode,measureDate,csvPrinter,dictionaryStringMap,db);
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
