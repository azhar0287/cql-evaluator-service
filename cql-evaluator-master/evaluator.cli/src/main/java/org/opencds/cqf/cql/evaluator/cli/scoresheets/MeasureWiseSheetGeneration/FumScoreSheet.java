package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.mappers.EligibleVisitDate;
import org.opencds.cqf.cql.evaluator.cli.mappers.PayerInfo;
import org.opencds.cqf.cql.evaluator.cli.util.Constant;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class FumScoreSheet {
    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    ///////////////////////////////////////////     FUM 30 ////////////////////////////////////////////////////////////////////////
    void addNumerator30ATrueFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM30A");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("1"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator30BTrueFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM30B");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("1"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator30AFalseFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM30A");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("0"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator30BFalseFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM30B");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("0"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////     FUM 7 ////////////////////////////////////////////////////////////////////////
    void addNumerator7ATrueFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM7A");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("1"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator7BTrueFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM7B");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("1"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator7AFalseFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM7A");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("0"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }

    void addNumerator7BFalseFumObjectInSheet(Document document, String payerCode,String age, CSVPrinter csvPrinter) throws IOException {
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  Sheet A Mapping ////////////////////////////////////////////////
        List<String> sheetObjA = new LinkedList<>();
        sheetObjA.add(document.getString("id"));
        sheetObjA.add("FUM7B");
        sheetObjA.add(payerCode);
        //CE
        if(document.getInteger("Eligible ED Visits") == 0){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }
        //Event
        if(document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm") ==null){
            sheetObjA.add("0");
        }
        else{
            sheetObjA.add("1");
        }


        if(document.getInteger("Eligible ED Visits") == 0 ||document.getInteger("Denominator 1") == 0|| document.getInteger("Exclusions 1") >0
                || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObjA.add("0"); //epop (Also known as denominator)
        }
        else {
            if(document.getInteger("Initial Population 1") == 0){
                sheetObjA.add("0");
            }
            else{
                sheetObjA.add("1");
            }
        }
        sheetObjA.add("0"); //excl
        sheetObjA.add("0"); //Numerator
        sheetObjA.add("0"); //rexcl
        if(document.getInteger("Exclusions 1") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getInteger("ED Exclusions 2") >0){
            sheetObjA.add("1"); //RexlD
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjA.add("1");
        }
        else{
            sheetObjA.add("0");
        }
        sheetObjA.add(age);
        sheetObjA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjA);
        ///////////////////////////////////////////////////////////////////////////////////////////////


    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




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

    public String getEncounterDateString(String stringDate,int days){
        String year=stringDate.substring(0,4);
        String month=stringDate.substring(5,7);
        String day=stringDate.substring(8,10);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
        calendar.set(Calendar.DATE, Integer.parseInt(day));
        calendar.set(Calendar.YEAR, Integer.parseInt(year));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.AM_PM,Calendar.AM);
        calendar.add(Calendar.HOUR_OF_DAY, 5);
        calendar.add(Calendar.DAY_OF_YEAR, days);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(calendar.getTime());

    }

    public List<String> mapPayersCodeInList(List<PayerInfo> payerInfoList,String encounterDateString){
        /// Add the 2 year in the birthdate that will be my anchor date
        String anchorDate=getEncounterDateString(encounterDateString,30);
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
                Date measurementPeriodStartingDate = UtilityFunction.getParsedDateInRequiredFormat(encounterDateString, "yyyy-MM-dd");
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
//                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        mapSpecialPayerCodes(updatedPayersList,entry.getKey());
                    //}
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

    public List<PayerInfo> removeDuplicatePayerOfSameDate(List<PayerInfo> payerInfoList){
//        List<PayerInfo> payerInfos=new LinkedList<>();
//        for(PayerInfo payerInfo:payerInfoList){
//            if(payerInfos.size()>0){
//                boolean flag=true;
//                for(PayerInfo temp:payerInfos){
//                    if(payerInfo.getCoverageEndDate().compareTo(temp.getCoverageEndDate()) == 0 &&
//                            payerInfo.getCoverageStartDate().compareTo(temp.getCoverageStartDate()) == 0){
//                        temp.setPayerCode(payerInfo.getPayerCode());
//                        flag=false;
//                        break;
//                    }
//                }
//                if(flag){
//                    payerInfos.add(payerInfo);
//                }
//            }
//            else{
//                payerInfos.add(payerInfo);
//            }
//        }
        return payerInfoList;
    }

    public void generateSheet(List<Document> documents, CSVPrinter csvPrinter, DBConnection db,Map<String,String> dictionaryStringMap) throws IOException {
       String patientId=null;
        try {
            List<PayerInfo> payerInfoList;
            for(Document document : documents) {
                System.out.println("Processing patient: "+document.getString("id"));
                patientId=document.getString("id");
                if(document.getString("id").equals("100019")){
                    int a=0;
                }
                if(!document.getBoolean("numerator1Exist") && !document.getBoolean("numerator2Exist")&& Integer.parseInt(document.getString("numerator1ExistAge1"))>=6 &&  Integer.parseInt(document.getString("numerator2ExistAge1"))>=6){    // both numerators not exists
                    List<String> payersList=new LinkedList<>();

                    Object object = document.get("payerCodes");
                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                    Object objectEdVisit = document.get("First Eligible ED Visits per 31 Day Period Dates");
                    List<EligibleVisitDate> dateStringList=new ObjectMapper().convertValue(objectEdVisit, new TypeReference<List<EligibleVisitDate>>() {});

                    if(dateStringList.size()>0){
                        payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                    }
                    else{
                        String getharmDate=document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm");
                        payersList=mapPayersCodeInList(payerInfoList,getharmDate);
                    }

                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                            {
                                if(document.getBoolean("eligibleVisitMoreThan1Num1")!=null && document.getBoolean("eligibleVisitMoreThan1Num2")!=null
                                        && document.getBoolean("eligibleVisitMoreThan1Num1") && document.getBoolean("eligibleVisitMoreThan1Num2")){
                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("numerator1ExistAge1"),csvPrinter);
                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("numerator2ExistAge1"),csvPrinter);
                                    addNumerator30BFalseFumObjectInSheet(document,payerCode,document.getString("numerator1ExistAge2"),csvPrinter);
                                    addNumerator7BFalseFumObjectInSheet(document,payerCode,document.getString("numerator2ExistAge2"),csvPrinter);
                                }
                                else{
                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("numerator1ExistAge1"),csvPrinter);
                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("numerator2ExistAge1"),csvPrinter);
                                }

                            }
                        }
                    }
                    else{
                        Main.failedPatients.add(document.getString("id"));
                    }
                    csvPrinter.flush();

                }
                else if(document.getBoolean("numerator1Exist") && document.getBoolean("numerator2Exist")){ // both numerators exists
                    Object objectEdVisitMain = document.get("First Eligible ED Visits per 31 Day Period Dates");
                    List<EligibleVisitDate> dateStringListMain=new ObjectMapper().convertValue(objectEdVisitMain, new TypeReference<List<EligibleVisitDate>>() {});
                    if(dateStringListMain.size()>0){
                        if(!document.getBoolean("eligibleVisitMoreThan1Num2") && !document.getBoolean("eligibleVisitMoreThan1Num1")){
                            Object objectEdVisit = document.get("First Eligible ED Visits per 31 Day Period Dates");
                            List<EligibleVisitDate> dateStringList=new ObjectMapper().convertValue(objectEdVisit, new TypeReference<List<EligibleVisitDate>>() {});
                            if(dateStringList.size()>0){
                                Object object = document.get("payerCodes");
                                List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                                updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                if (payersList.size() != 0) {
                                    for (String payerCode:payersList) {
                                        String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                        if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                        {
                                            //means only 1 ed visit available
                                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                                if(document.getBoolean("FUM30A Num1")){
                                                    addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                            }
                                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age1")) >=6){
                                                if(document.getBoolean("FUM7A Num2")){
                                                    addNumerator7ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                }
                                else{
                                    Main.failedPatients.add(document.getString("id"));
                                }
                                csvPrinter.flush();

                            }
                        }
                        else if(document.getBoolean("eligibleVisitMoreThan1Num2") && document.getBoolean("eligibleVisitMoreThan1Num1")){
                            Object objectEdVisit = document.get("First Eligible ED Visits per 31 Day Period Dates");
                            List<EligibleVisitDate> dateStringList=new ObjectMapper().convertValue(objectEdVisit, new TypeReference<List<EligibleVisitDate>>() {});
                            if(dateStringList.size()>0){
                                //means only 1 ed visit available
                                if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                    Object object = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM30A Num1")){
                                                    addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();



                                }

                                if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age2")) >=6){
                                    Object object = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(1).getEligibleEdDate());
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM30B Num1")){
                                                    addNumerator30BTrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }

                                if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age1")) >=6){

                                    Object object = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM7A Num2")){
                                                    addNumerator7ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }

                                if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age2")) >=6){
                                    Object object = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(1).getEligibleEdDate());
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM7B Num2")){
                                                    addNumerator7BTrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age2"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age2"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }
                            }
                        }
                    }
                    else{
                        String getharmDateMain=document.getString("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm");
                        if(getharmDateMain!=null){
                            if(!document.getBoolean("eligibleVisitMoreThan1Num2") && !document.getBoolean("eligibleVisitMoreThan1Num1")){
                                Object objectSecond = document.get("payerCodes");
                                List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(objectSecond, new TypeReference<List<PayerInfo>>() {});
                                payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                List<String> payersList=mapPayersCodeInList(payerInfoList,getharmDateMain);
                                updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                if (payersList.size() != 0) {
                                    for (String payerCode:payersList) {
                                        String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                        if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                        {
                                            //means only 1 ed visit available
                                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                                if(document.getBoolean("FUM30A Num1")){
                                                    addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                            }
                                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age1")) >=6){
                                                if(document.getBoolean("FUM7A Num2")){
                                                    addNumerator7ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                }
                                else{
                                    Main.failedPatients.add(document.getString("id"));
                                }
                                csvPrinter.flush();
                            }
                            else if(document.getBoolean("eligibleVisitMoreThan1Num2") && document.getBoolean("eligibleVisitMoreThan1Num1")){
                                //means only 1 ed visit available
                                if(document.getString("eligibleVisitMoreThan1Num1Age1")!=null && Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                    Object objectSecond = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(objectSecond, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,getharmDateMain);
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM30A Num1")){
                                                    addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();



                                }

                                if(document.getString("eligibleVisitMoreThan1Num1Age2")!=null && Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age2")) >=6){
                                    Object objectSecond = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(objectSecond, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,getharmDateMain);
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM30B Num1")){
                                                    addNumerator30BTrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator30BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }

                                if(document.getString("eligibleVisitMoreThan1Num2Age1")!=null && Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age1")) >=6){

                                    Object objectSecond = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(objectSecond, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,getharmDateMain);
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM7A Num2")){
                                                    addNumerator7ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age1"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }

                                if(document.getString("eligibleVisitMoreThan1Num2Age2")!=null && Integer.parseInt(document.getString("eligibleVisitMoreThan1Num2Age2")) >=6){
                                    Object objectSecond = document.get("payerCodes");
                                    List<PayerInfo> temppayerInfoList = new ObjectMapper().convertValue(objectSecond, new TypeReference<List<PayerInfo>>() {});
                                    payerInfoList=removeDuplicatePayerOfSameDate(temppayerInfoList);
                                    List<String> payersList=mapPayersCodeInList(payerInfoList,getharmDateMain);
                                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                    if (payersList.size() != 0) {
                                        for (String payerCode:payersList) {
                                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                            if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                            {
                                                if(document.getBoolean("FUM7B Num2")){
                                                    addNumerator7BTrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age2"),csvPrinter);
                                                }
                                                else{
                                                    addNumerator7BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num2Age2"),csvPrinter);
                                                }
                                            }
                                        }
                                    }
                                    else{
                                        Main.failedPatients.add(document.getString("id"));
                                    }
                                    csvPrinter.flush();

                                }
                            }
                            else{
                                Main.failedPatients.add(document.getString("id"));
                            }
                            csvPrinter.flush();

                        }

                    }
                }
                else if(document.getBoolean("numerator1Exist") && !document.getBoolean("numerator2Exist")){ //numerator 1 exist and numerator 2 not exist

                    Object objectEdVisit = document.get("First Eligible ED Visits per 31 Day Period Dates");
                    List<EligibleVisitDate> dateStringList=new ObjectMapper().convertValue(objectEdVisit, new TypeReference<List<EligibleVisitDate>>() {});
                    if(dateStringList.size()>0){
                        if(!document.getBoolean("eligibleVisitMoreThan1Num1")){
                            Object object = document.get("payerCodes");
                            payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                            List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                            updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                            if (payersList.size() != 0) {
                                for (String payerCode:payersList) {
                                    String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                    if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                    {
                                        //means only 1 ed visit available
                                        if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                            if(document.getBoolean("FUM30A Num1")){
                                                addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                            }
                                            else{
                                                addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                            }
                                        }
                                    }
                                }
                            }
                            else{
                                Main.failedPatients.add(document.getString("id"));
                            }
                            csvPrinter.flush();

                        }
                        else if(document.getBoolean("eligibleVisitMoreThan1Num1")){
                            //means only more than 2 ed visit available
                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age1")) >=6){
                                Object object = document.get("payerCodes");
                                payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(0).getEligibleEdDate());
                                updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                if (payersList.size() != 0) {
                                    for (String payerCode:payersList) {
                                        String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                        if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                        {
                                            if(document.getBoolean("FUM30A Num1")){
                                                addNumerator30ATrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);

                                            }
                                            else{
                                                addNumerator30AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                                addNumerator7AFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age1"),csvPrinter);
                                            }
                                        }
                                    }
                                }
                                else{
                                    Main.failedPatients.add(document.getString("id"));
                                }
                                csvPrinter.flush();

                            }
                            if(Integer.parseInt(document.getString("eligibleVisitMoreThan1Num1Age2")) >=6){
                                Object object = document.get("payerCodes");
                                payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                                List<String> payersList=mapPayersCodeInList(payerInfoList,dateStringList.get(1).getEligibleEdDate());
                                updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                                if (payersList.size() != 0) {
                                    for (String payerCode:payersList) {
                                        String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                                        if ((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)) || (payerCodeType.equals(Constant.CODE_TYPE_MEDICARE) ))
                                        {
                                            if(document.getBoolean("FUM30B Num1")){
                                                addNumerator30BTrueFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                addNumerator7BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                            }
                                            else{
                                                addNumerator30BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                                addNumerator7BFalseFumObjectInSheet(document,payerCode,document.getString("eligibleVisitMoreThan1Num1Age2"),csvPrinter);
                                            }
                                        }
                                    }
                                }
                                else{
                                    Main.failedPatients.add(document.getString("id"));
                                }
                                csvPrinter.flush();

                            }
                        }
                    }

                }
            }
            documents.clear();
        } catch (Exception e) {
            System.out.println("Exception! while processing PatientId="+patientId);
            e.printStackTrace();
        }
    }

}
