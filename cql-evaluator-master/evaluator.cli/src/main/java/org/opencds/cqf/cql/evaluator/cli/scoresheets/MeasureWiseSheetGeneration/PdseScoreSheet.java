package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.hl7.elm.r1.In;
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.SheetGenerationTask;
import org.opencds.cqf.cql.evaluator.cli.util.Constant;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import org.opencds.cqf.cql.evaluator.engine.retrieve.DeliveryProcedureInfo;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class PdseScoreSheet {

    public static Logger LOGGER  = LogManager.getLogger(PdseScoreSheet.class);

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    public static List<?> convertObjectToList(Object obj) {
        List<?> list = new ArrayList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[])obj);
        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>)obj);
        }
        return list;
    }


    static boolean  isSizeGreaterThanZero(Object obj){
        if(convertObjectToList(obj).size()>0){
            return true;
        }
        return false;
    }
    static int  getSize(Object obj){
        return convertObjectToList(obj).size();
    }

    public static Document createDocumentForPdseResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", UtilityFunction.getPayerInfoMap(patientData.getPayerInfo()));
        document.put("deliveryProcedureInfos", UtilityFunction.getDeliveryProcedureInfoMap(patientData.getDeliveryProcedureInfos()));
        document.put("hospiceFlag",patientData.getHospiceFlag());

        document.put("Member Coverage",getSize(expressionResults.get("Member Coverage")));
        document.put("Delivery",getSize(expressionResults.get("Delivery")));
        document.put("Initial Population 1",getSize(expressionResults.get("Initial Population 1")));
        document.put("Initial Population 2",getSize(expressionResults.get("Initial Population 2")));
        document.put("Denominator 1",getSize(expressionResults.get("Denominator 1")));
        document.put("Denominator 2",getSize(expressionResults.get("Denominator 2")));
        document.put("Exclusions 1",getSize(expressionResults.get("Exclusions 1")));
        document.put("Exclusions 2",getSize(expressionResults.get("Exclusions 2")));
        document.put("Numerator 1",getSize(expressionResults.get("Numerator 1")));
        document.put("Numerator 2",getSize(expressionResults.get("Numerator 2")));


        document.put("Adolescent Full Length Depression Screen with Positive Result",getSize(expressionResults.get("Adolescent Full Length Depression Screen with Positive Result")));
        document.put("Adolescent Brief Screen with Positive Result",getSize(expressionResults.get("Adolescent Brief Screen with Positive Result")));
        document.put("Adolescent Depression Screening with Positive Result 7 to 84 Days after Delivery",getSize(expressionResults.get("Adolescent Depression Screening with Positive Result 7 to 84 Days after Delivery")));
        document.put("Adult Full Length Depression Screen with Positive Result",getSize(expressionResults.get("Adult Full Length Depression Screen with Positive Result")));
        document.put("Adult Brief Screen with Positive Result",getSize(expressionResults.get("Adult Brief Screen with Positive Result")));
        document.put("Adult Depression Screening with Positive Result 7 to 84 Days after Delivery",getSize(expressionResults.get("Adult Depression Screening with Positive Result 7 to 84 Days after Delivery")));
        document.put("Delivery with Hospice Intervention or Encounter",getSize(expressionResults.get("Delivery with Hospice Intervention or Encounter")));
        document.put("Adolescent Full Length Depression Screen with Documented Result",getSize(expressionResults.get("Adolescent Full Length Depression Screen with Documented Result")));
        document.put("Adolescent Brief Screen with Documented Result",getSize(expressionResults.get("Adolescent Brief Screen with Documented Result")));
        document.put("Adolescent Depression Screening with Documented Result 7 to 84 Days after Delivery",getSize(expressionResults.get("Adolescent Depression Screening with Documented Result 7 to 84 Days after Delivery")));
        document.put("Adult Full Length Depression Screen with Documented Result",getSize(expressionResults.get("Adult Full Length Depression Screen with Documented Result")));
        document.put("Adult Brief Screen with Documented Result",getSize(expressionResults.get("Adult Brief Screen with Documented Result")));
        document.put("Adult Depression Screening with Documented Result 7 to 84 Days after Delivery",getSize(expressionResults.get("Adult Depression Screening with Documented Result 7 to 84 Days after Delivery")));
        document.put("First Positive Adolescent Depression Screen after Delivery",getSize(expressionResults.get("First Positive Adolescent Depression Screen after Delivery")));
        document.put("First Positive Adult Depression Screen after Delivery",getSize(expressionResults.get("First Positive Adult Depression Screen after Delivery")));
        document.put("Follow Up Care on or 30 days after First Positive Screen after Delivery",getSize(expressionResults.get("Follow Up Care on or 30 days after First Positive Screen after Delivery")));
        document.put("Delivery with Follow Up Care on or 30 days after First Positive Screen",getSize(expressionResults.get("Delivery with Follow Up Care on or 30 days after First Positive Screen")));
        document.put("First Positive Adult Screen after Delivery is Brief Screen",getSize(expressionResults.get("First Positive Adult Screen after Delivery is Brief Screen")));
        document.put("First Positive Adolescent Screen after Delivery is Brief Screen",getSize(expressionResults.get("First Positive Adolescent Screen after Delivery is Brief Screen")));
        document.put("Delivery with Positive Brief Screen Same Day as Negative Full Length Screen",getSize(expressionResults.get("Delivery with Positive Brief Screen Same Day as Negative Full Length Screen")));


        return document;
    }
    public String getProcedureBasedAge(String birthdate,String procedurePerformedDate){
        birthdate=utilityFunction.getFormattedDate(birthdate);
        procedurePerformedDate=utilityFunction.getFormattedDate(procedurePerformedDate);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = null;
        Date endDate=null;
        try {
            startDate = sdf.parse(birthdate);
            endDate = sdf.parse(procedurePerformedDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }


        OffsetDateTime startOdt = startDate.toInstant().atOffset(ZoneOffset.UTC);
        OffsetDateTime endOdt = endDate.toInstant().atOffset(ZoneOffset.UTC);

        int years = Period.between(startOdt.toLocalDate(), endOdt.toLocalDate()).getYears();
        return String.valueOf(years);

    }
//    public String getProcedureBasedAge(String birthdate,String procedurePerformedDate) {
//        Calendar measurementDate = new GregorianCalendar(Integer.parseInt(procedurePerformedDate.substring(0,4)), Integer.parseInt(procedurePerformedDate.substring(4,6)), Integer.parseInt(procedurePerformedDate.substring(6,8)));
//        Calendar dob = new GregorianCalendar(Integer.parseInt(birthdate.substring(0,4)), Integer.parseInt(birthdate.substring(4,6)), Integer.parseInt(birthdate.substring(6,8)));
////
////determines the year of DOB and current date
//        int age = measurementDate.get(Calendar.YEAR) - dob.get(Calendar.YEAR);
//        if ((dob.get(Calendar.MONTH) > measurementDate.get(Calendar.MONTH)) || (dob.get(Calendar.MONTH) == measurementDate.get(Calendar.MONTH) && dob.get(Calendar.DAY_OF_MONTH) > measurementDate.get(Calendar.DAY_OF_MONTH)))
//        {
////decrements age by 1
//            age--;
//        }
////        else if((Integer.parseInt(birthdate.substring(4,6) )> Integer.parseInt(procedurePerformedDate.substring(4,6))) ||
////        ((Integer.parseInt(birthdate.substring(4,6) ) == Integer.parseInt(procedurePerformedDate.substring(4,6)) &&
////                (Integer.parseInt(birthdate.substring(6,8)) > Integer.parseInt(procedurePerformedDate.substring(6,8))))  ){
////            age--;
////        }
////prints the age
//        return String.valueOf(age);
//    }


    void addObjectInSheet(Document document, String payerCode, CSVPrinter csvPrinter,String payerCodeType) throws IOException {
        Object deliveryProcedureInfoObject = document.get("deliveryProcedureInfos");
        List<DeliveryProcedureInfo> deliveryProcedureInfos = new ObjectMapper().convertValue(deliveryProcedureInfoObject, new TypeReference<List<DeliveryProcedureInfo>>() {});

        String numerator1A="";
        String numerator1B="";



        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");
        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
        /////////////////////////////  PDS1A Mapping ////////////////////////////////////////////////
        List<String> sheetObjPds1A = new LinkedList<>();
        sheetObjPds1A.add(document.getString("id"));
        sheetObjPds1A.add("PDS1A");
        sheetObjPds1A.add(payerCode);
        if(document.getInteger("Initial Population 1")>0){//CE
            sheetObjPds1A.add("1");
        }
        else{
            sheetObjPds1A.add("0");
        }

        sheetObjPds1A.add("1");//Event
//        if(document.getInteger("Denominator 1")>0){//Event
//            sheetObjPds1A.add("1");
//        }
//        else{
//            sheetObjPds1A.add("0");
//        }

        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
            if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
                if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                    sheetObjPds1A.add("0"); //epop (Also known as denominator)
                }
                else {
                    sheetObjPds1A.add("1"); //epop
                }
            }else{
                sheetObjPds1A.add("0");
            }
        }
        else{
            sheetObjPds1A.add("0");
        }


        sheetObjPds1A.add("0"); //excl

        if((document.getInteger("Denominator 1")==2) && document.getInteger("Numerator 1") > 0  && document.getInteger("Denominator 1") > 0 ){
            sheetObjPds1A.add("0");
            numerator1A="0";

        }
        else if(document.getInteger("Numerator 1") > 0  && document.getInteger("Denominator 1") > 0 ){
            sheetObjPds1A.add("1");
            numerator1A="1";
        }
        else{
            sheetObjPds1A.add("0");
            numerator1A="0";
        }

        if(document.getInteger("Exclusions 1")>0){
            sheetObjPds1A.add("1"); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjPds1A.add("1");
        }
        else{
            sheetObjPds1A.add("0");
        }
        sheetObjPds1A.add("0"); //RexlD

        if(deliveryProcedureInfos.size()>1){
            sheetObjPds1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));

        }
        else if(deliveryProcedureInfos.size()>0){
            sheetObjPds1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));

        }else{
            sheetObjPds1A.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

        }
        sheetObjPds1A.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjPds1A);
        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PDS1B Mapping ////////////////////////////////////////////////
        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
            List<String> sheetObjPds1B = new LinkedList<>();
            sheetObjPds1B.add(document.getString("id"));
            sheetObjPds1B.add("PDS1B");
            sheetObjPds1B.add(payerCode);
            if(document.getInteger("Initial Population 1")>0){//CE
                sheetObjPds1B.add("1");
            }
            else{
                sheetObjPds1B.add("0");
            }

            sheetObjPds1B.add("1");//Event
//            if(document.getInteger("Denominator 1")==2){//Event
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
                if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
                    if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                        sheetObjPds1B.add("0"); //epop (Also known as denominator)
                    }
                    else {
                        sheetObjPds1B.add("1"); //epop
                    }
                }
                else{
                    sheetObjPds1B.add("0");
                }
            }
            else{
                sheetObjPds1B.add("0");
            }

            sheetObjPds1B.add("0"); //excl

            if(document.getInteger("Numerator 1") > 0 && document.getInteger("Denominator 1") > 0 ){
                sheetObjPds1B.add("1");
                numerator1B="1";
            }
            else{
                sheetObjPds1B.add("0");
                numerator1B="0";
            }

            if(document.getInteger("Exclusions 1")>0){
                sheetObjPds1B.add("1"); //rexcl
            }
            else if(document.getString("hospiceFlag").equals("Y")) {
                sheetObjPds1B.add("1");
            }
            else{
                sheetObjPds1B.add("0");
            }

            sheetObjPds1B.add("0"); //RexlD
            if(document.getInteger("Denominator 1") > 0){
                if(deliveryProcedureInfos.size()==0){
                    sheetObjPds1B.add(String.valueOf(Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))))+1));

                }
                else{
                    sheetObjPds1B.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));

                }

            }else{
                sheetObjPds1B.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

            }
            sheetObjPds1B.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjPds1B);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PDS2A Mapping ////////////////////////////////////////////////

        List<String> sheetObjPds2A = new LinkedList<>();
        sheetObjPds2A.add(document.getString("id"));
        sheetObjPds2A.add("PDS2A");
        sheetObjPds2A.add(payerCode);
        if(document.getInteger("Initial Population 2")>0){//CE
            sheetObjPds2A.add("1");
        }
        else{
            sheetObjPds2A.add("0");
        }

        if(document.getInteger("Denominator 2")>0 && numerator1A.equals("1")){//Event
            sheetObjPds2A.add("1");
        }
        else{
            sheetObjPds2A.add("0");
        }


//        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
//            if(document.getInteger("Initial Population 2")>0 && document.getInteger("Denominator 2")>0){
//                if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//                    sheetObjPds2A.add("0"); //epop (Also known as denominator)
//                }
//                else {
//                    sheetObjPds2A.add("1"); //epop
//                }
//            }
//            else{
//                sheetObjPds2A.add("0");
//            }
//        }
//        else{
//            sheetObjPds2A.add("0");
//
//        }
        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
            if(document.getInteger("Initial Population 2") > 0 && document.getInteger("Denominator 2")>0 && numerator1A.equals("1")){
                if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                    sheetObjPds2A.add("0"); //epop (Also known as denominator)
                }
                else {
                    sheetObjPds2A.add("1"); //epop
                }
            }
            else{
                sheetObjPds2A.add("0");
            }
        }
        else{
            sheetObjPds2A.add("0");

        }

        sheetObjPds2A.add("0"); //excl

        if(document.getInteger("Numerator 2") ==1 && document.getInteger("Denominator 1")== 1){
            sheetObjPds2A.add("1");
        }
        else{
            sheetObjPds2A.add("0");

        }
        if(document.getInteger("Exclusions 2")>0){
            sheetObjPds2A.add("1"); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjPds2A.add("1");
        }
        else{
            sheetObjPds2A.add("0");
        }
        sheetObjPds2A.add("0"); //RexlD

        if(deliveryProcedureInfos.size()>1){
            sheetObjPds2A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
        }
        else if(deliveryProcedureInfos.size()>0){
            sheetObjPds2A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));
        }else{
            sheetObjPds2A.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        }
        sheetObjPds2A.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjPds2A);


        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PDS2B Mapping ////////////////////////////////////////////////
        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
            List<String> sheetObjPds2B = new LinkedList<>();
            sheetObjPds2B.add(document.getString("id"));
            sheetObjPds2B.add("PDS2B");
            sheetObjPds2B.add(payerCode);
            if(document.getInteger("Initial Population 2") > 0 ){//CE
                sheetObjPds2B.add("1");
            }
            else{
                sheetObjPds2B.add("0");
            }

            if(numerator1B.equals("1")){//Event
                sheetObjPds2B.add("1");
            }
            else{
                sheetObjPds2B.add("0");
            }
//            if(document.getInteger("Denominator 2") == 2){//Event
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//            }

            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
                if(document.getInteger("Initial Population 2")>0 && document.getInteger("Denominator 2")>0){
                    if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                        sheetObjPds2B.add("0"); //epop (Also known as denominator)
                    }
                    else {
                        sheetObjPds2B.add("1"); //epop
                    }
                }
                else{
                    sheetObjPds2B.add("0");
                }
            }
            else{
                sheetObjPds2B.add("0");

            }

            sheetObjPds2B.add("0"); //excl
            sheetObjPds2B.add("0");//Num
//            if(document.getInteger("Numerator 2") == 2 && document.getInteger("Denominator 1")==2){
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//
//            }
            if(document.getInteger("Exclusions 2")>0){
                sheetObjPds2B.add("1"); //rexcl
            }
            else if(document.getString("hospiceFlag").equals("Y")) {
                sheetObjPds2B.add("1");
            }
            else{
                sheetObjPds2B.add("0");
            }
            sheetObjPds2B.add("0"); //RexlD
            if(document.getInteger("Denominator 1") > 0){
                if(deliveryProcedureInfos.size()==0){
                    sheetObjPds2B.add(String.valueOf(Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))))+1));

                }
                else{
                    sheetObjPds2B.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));

                }
            }else{
                sheetObjPds2B.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

            }
            sheetObjPds2B.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjPds2B);
        }

    }

//    void addObjectInSheet(Document document, String payerCode, CSVPrinter csvPrinter,String payerCodeType) throws IOException {
//        Object deliveryProcedureInfoObject = document.get("deliveryProcedureInfos");
//        List<DeliveryProcedureInfo> deliveryProcedureInfos = new ObjectMapper().convertValue(deliveryProcedureInfoObject, new TypeReference<List<DeliveryProcedureInfo>>() {});
//
//
//
//        List<String> codeList = new LinkedList<>();
//        codeList.add ("MEP");
//        codeList.add ("MMO");
//        codeList.add ("MPO");
//        codeList.add ("MOS");
//        ////////////////////////////// Mapping Started ////////////////////////////////////////////////
//        /////////////////////////////  PDS1A Mapping ////////////////////////////////////////////////
//        List<String> sheetObjPds1A = new LinkedList<>();
//        sheetObjPds1A.add(document.getString("id"));
//        sheetObjPds1A.add("PDS1A");
//        sheetObjPds1A.add(payerCode);
//        if(document.getInteger("Initial Population 1")>0){//CE
//            sheetObjPds1A.add("1");
//        }
//        else{
//            sheetObjPds1A.add("0");
//        }
//
//        if(document.getInteger("Denominator 1")>0){//Event
//            sheetObjPds1A.add("1");
//        }
//        else{
//            sheetObjPds1A.add("0");
//        }
//
//        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
//            if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
//                if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//                    sheetObjPds1A.add("0"); //epop (Also known as denominator)
//                }
//                else {
//                    sheetObjPds1A.add("1"); //epop
//                }
//            }else{
//                sheetObjPds1A.add("0");
//            }
//        }
//        else{
//            sheetObjPds1A.add("0");
//        }
//
//
//
//        sheetObjPds1A.add("0"); //excl
//
//        if(document.getInteger("Numerator 1") > 0  && document.getInteger("Denominator 1") > 0 ){
//            sheetObjPds1A.add("1");
//        }
//        else{
//            sheetObjPds1A.add("0");
//
//        }
//        if(document.getInteger("Exclusions 1")>0){
//            sheetObjPds1A.add("1"); //rexcl
//        }
//        else if(document.getString("hospiceFlag").equals("Y")) {
//            sheetObjPds1A.add("1");
//        }
//        else{
//            sheetObjPds1A.add("0");
//        }
//        sheetObjPds1A.add("0"); //RexlD
//
//        if(deliveryProcedureInfos.size()>1){
//            sheetObjPds1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
//
//        }
//       else if(deliveryProcedureInfos.size()>0){
//            sheetObjPds1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));
//
//        }else{
//            sheetObjPds1A.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
//
//        }
//        sheetObjPds1A.add(utilityFunction.getGenderSymbol(document.getString("gender")));
//        csvPrinter.printRecord(sheetObjPds1A);
//        ///////////////////////////////////////////////////////////////////////////////////////////////
//        /////////////////////////////  PDS1B Mapping ////////////////////////////////////////////////
////        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
//            List<String> sheetObjPds1B = new LinkedList<>();
//            sheetObjPds1B.add(document.getString("id"));
//            sheetObjPds1B.add("PDS1B");
//            sheetObjPds1B.add(payerCode);
//            if(document.getInteger("Initial Population 1")>0){//CE
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
//
//            if(document.getInteger("Denominator 1")==2){//Event
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
//            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
//                if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
//                    if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//                        sheetObjPds1B.add("0"); //epop (Also known as denominator)
//                    }
//                    else {
//                        sheetObjPds1B.add("1"); //epop
//                    }
//                }
//                else{
//                    sheetObjPds1B.add("0");
//                }
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
//
//            sheetObjPds1B.add("0"); //excl
//
//            if(document.getInteger("Numerator 2") > 0 && document.getInteger("Denominator 1") > 0 ){
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
//            if(document.getInteger("Exclusions 1")>0){
//                sheetObjPds1B.add("1"); //rexcl
//            }
//            else if(document.getString("hospiceFlag").equals("Y")) {
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
//            sheetObjPds1B.add("0"); //RexlD
//            if(deliveryProcedureInfos.size()>0){
//                sheetObjPds1B.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));
//
//            }else{
//                sheetObjPds1B.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
//
//            }
//            sheetObjPds1B.add(utilityFunction.getGenderSymbol(document.getString("gender")));
//            csvPrinter.printRecord(sheetObjPds1B);
//        }
//
//        ///////////////////////////////////////////////////////////////////////////////////////////////
//        /////////////////////////////  PDS2A Mapping ////////////////////////////////////////////////
//
//        List<String> sheetObjPds2A = new LinkedList<>();
//        sheetObjPds2A.add(document.getString("id"));
//        sheetObjPds2A.add("PDS2A");
//        sheetObjPds2A.add(payerCode);
//        if(document.getInteger("Initial Population 2")>0){//CE
//            sheetObjPds2A.add("1");
//        }
//        else{
//            sheetObjPds2A.add("0");
//        }
//
////        if(document.getInteger("Denominator 2")==1){//Event
////            sheetObjPds2A.add("1");
////        }
////        else{
////            sheetObjPds2A.add("0");
////        }
//        if(document.getInteger("Numerator 1")>0  && document.getInteger("Denominator 1")==1){//Event
//            sheetObjPds2A.add("1");
//        }
//        else{
//            sheetObjPds2A.add("0");
//        }
//
//        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
//            if(document.getInteger("Initial Population 2")>0 && document.getInteger("Denominator 2")>0){
//                if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//                    sheetObjPds2A.add("0"); //epop (Also known as denominator)
//                }
//                else {
//                    sheetObjPds2A.add("1"); //epop
//                }
//            }
//            else{
//                sheetObjPds2A.add("0");
//            }
//        }
//        else{
//            sheetObjPds2A.add("0");
//
//        }
//
//
//
//        sheetObjPds2A.add("0"); //excl
//
//        if(document.getInteger("Numerator 2") ==1 && document.getInteger("Denominator 1")== 1){
//            sheetObjPds2A.add("1");
//        }
//        else{
//            sheetObjPds2A.add("0");
//
//        }
//        if(document.getInteger("Exclusions 2")>0){
//            sheetObjPds2A.add("1"); //rexcl
//        }
//        else if(document.getString("hospiceFlag").equals("Y")) {
//            sheetObjPds2A.add("1");
//        }
//        else{
//            sheetObjPds2A.add("0");
//        }
//        sheetObjPds2A.add("0"); //RexlD
//
//        if(deliveryProcedureInfos.size()>1){
//            sheetObjPds2A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
//        }
//        else if(deliveryProcedureInfos.size()>0){
//            sheetObjPds2A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));
//        }else{
//            sheetObjPds2A.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
//        }
//        sheetObjPds2A.add(utilityFunction.getGenderSymbol(document.getString("gender")));
//        csvPrinter.printRecord(sheetObjPds2A);
//
//
//        ///////////////////////////////////////////////////////////////////////////////////////////////
//        /////////////////////////////  PDS2B Mapping ////////////////////////////////////////////////
////        if(document.getInteger("Denominator 2")>1){
//        if(deliveryProcedureInfos.size()==2){
//            List<String> sheetObjPds2B = new LinkedList<>();
//            sheetObjPds2B.add(document.getString("id"));
//            sheetObjPds2B.add("PDS2B");
//            sheetObjPds2B.add(payerCode);
//            if(document.getInteger("Initial Population 2") > 0 ){//CE
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//            }
//
//            if(document.getInteger("Denominator 2") == 2){//Event
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//            }
//
//            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
//                if(document.getInteger("Initial Population 2")>0 && document.getInteger("Denominator 2")>0){
//                    if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
//                        sheetObjPds2B.add("0"); //epop (Also known as denominator)
//                    }
//                    else {
//                        sheetObjPds2B.add("1"); //epop
//                    }
//                }
//                else{
//                    sheetObjPds2B.add("0");
//                }
//            }
//            else{
//                sheetObjPds2B.add("0");
//
//            }
//
//            sheetObjPds2B.add("0"); //excl
//
//            if(document.getInteger("Numerator 2") == 2 && document.getInteger("Denominator 1")==2){
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//
//            }
//            if(document.getInteger("Exclusions 2")>0){
//                sheetObjPds2B.add("1"); //rexcl
//            }
//            else if(document.getString("hospiceFlag").equals("Y")) {
//                sheetObjPds2B.add("1");
//            }
//            else{
//                sheetObjPds2B.add("0");
//            }
//            sheetObjPds2B.add("0"); //RexlD
//            if(deliveryProcedureInfos.size()>0){
//                sheetObjPds2B.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));
//
//            }else{
//                sheetObjPds2B.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
//
//            }
//            sheetObjPds2B.add(utilityFunction.getGenderSymbol(document.getString("gender")));
//            csvPrinter.printRecord(sheetObjPds2B);
//        }
//
//    }

    void mapAllowedDeliveryProcedureInList(List<DeliveryProcedureInfo> deliveryProcedureInfoList){
        Date intervalStartDate = UtilityFunction.getParsedDateInRequiredFormat("2021-09-08", "yyyy-MM-dd");
        Date intervalEndDate = UtilityFunction.getParsedDateInRequiredFormat("2022-09-07", "yyyy-MM-dd");

        List<DeliveryProcedureInfo> deliveryProcedureInfoTempList=new LinkedList<>();
        if(deliveryProcedureInfoList.size()>0){
            for(DeliveryProcedureInfo deliveryProcedureInfo:deliveryProcedureInfoList){
                //if there is any delivery procedure that has performed date in between above mentioned interval than that patient will have entery in the sheet.
                if(deliveryProcedureInfo.getPerformedDate().compareTo(intervalStartDate)>=0 && intervalEndDate.compareTo(deliveryProcedureInfo.getPerformedDate())>=0){
                    deliveryProcedureInfoTempList.add(deliveryProcedureInfo);
                }

            }
        }
        deliveryProcedureInfoList.clear();
        deliveryProcedureInfoList.addAll(deliveryProcedureInfoTempList);
    }

    String getPayerCodeType(String payerCode ,DBConnection dbConnection,Map<String,String> dictionaryStringMap){
        if(!dictionaryStringMap.isEmpty()){
            String payerCodeOid=dictionaryStringMap.get(payerCode);
            if(payerCodeOid !=null && !payerCodeOid.equals("")){
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
//            payersList.add("MCR");
        }
        else{
            payersList.add(payerCode);
        }
    }

    public String getAnchorDate(Date date,int days){
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, days);
        Date myDate = cal.getTime();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(myDate);
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

    public List<String> mapDeliveryProcedurePayersCodeInList(List<DeliveryProcedureInfo> deliveryProcedureInfos,List<PayerInfo> payerInfoList){
        /// Add the 2 year in the birthdate that will be my anchor date
        Set<String> payerSet = new HashSet<String>();
        Set<String> payersList=new HashSet<>();
        List<PayerInfo> tempPayersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {

            for (DeliveryProcedureInfo deliveryProcedureInfo : deliveryProcedureInfos) {
                String sixtyDaysAnchorDate=getAnchorDate(deliveryProcedureInfo.getPerformedDate(),60);

                Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat(sixtyDaysAnchorDate, "yyyy-MM-dd");
                for (PayerInfo payerInfo : payerInfoList) {
                    Date insuranceEndDate = payerInfo.getCoverageEndDate();
                    Date insuranceStartDate = payerInfo.getCoverageStartDate();
                    if (insuranceEndDate != null && insuranceEndDate.compareTo(measurementPeriodEndingDate) >= 0 &&
                            !(insuranceStartDate.compareTo(measurementPeriodEndingDate) > 0)) {
                        payersList.add(payerInfo.getPayerCode());
                    }
                }
            }

            //If no payer matches the above condition than get previous date from and check the range within the anchordate and send the latest POS back
            if (payersList.isEmpty()) {
                boolean flag=true;
                int j=0;
                while ( j<deliveryProcedureInfos.size() && flag) {
                    DeliveryProcedureInfo deliveryProcedureInfo=deliveryProcedureInfos.get(j);
                    String anchorDateString = getAnchorDate(deliveryProcedureInfo.getPerformedDate(), 0);
                    String sixtyDaysAnchorDateString = getAnchorDate(deliveryProcedureInfo.getPerformedDate(), 60);
                    Date sixtyDaysAnchorDate = UtilityFunction.getParsedDateInRequiredFormat(sixtyDaysAnchorDateString, "yyyy-MM-dd");
                    Date anchorDate = UtilityFunction.getParsedDateInRequiredFormat(anchorDateString, "yyyy-MM-dd");
                    for (int z=payerInfoList.size()-1;z>=0;z--) {
                        PayerInfo payerInfo = payerInfoList.get(z);
                        Date insuranceEndDate = payerInfo.getCoverageEndDate();
                        Date insuranceStartDate = payerInfo.getCoverageStartDate();

                        //It returns the value 0 if the argument Date is equal to this Date.
                        //It returns a value less than 0 if this Date is before the Date argument.
                        //It returns a value greater than 0 if this Date is after the Date argument.

                        if (((insuranceStartDate.compareTo(anchorDate)>=0) && sixtyDaysAnchorDate.compareTo(insuranceStartDate)>=0) ||
                                (((insuranceStartDate.compareTo(anchorDate)<=0)) && (insuranceEndDate.compareTo(anchorDate)>=0)))
                        {
                            tempPayersList.add(payerInfo);
                            flag=false;
                        }
                    }

                    j++;

                }
            }
            if (tempPayersList.size() > 0) {
                int size = tempPayersList.size() - 1;
                payersList.add(tempPayersList.get(size).getPayerCode());
            }

        }
        return new LinkedList<>(payersList);
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
                if(codeTypes != null && codeTypes.size()!=0 ) {
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
//                    mapSpecialPayerCodes(updatedPayersList,entry.getKey());

                    if (entry.getValue().equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
//                        payerCodes.add(entry.getKey());
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
    //    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db,Map<String,String> dictionaryStringMap) throws IOException {
//        String patientId="";
//        try {
//            List<String> sheetObj;
//            List<PayerInfo> payerInfoList;
//            List<DeliveryProcedureInfo> deliveryProcedureInfos;
//
//            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
//            for(Document document : documents) {
//                patientId=document.getString("id");
//                System.out.println("Processing patient: "+patientId);
//                Object deliveryProcedureInfoObject = document.get("deliveryProcedureInfos");
//                deliveryProcedureInfos = new ObjectMapper().convertValue(deliveryProcedureInfoObject, new TypeReference<List<DeliveryProcedureInfo>>() {});
//                mapAllowedDeliveryProcedureInList(deliveryProcedureInfos);
//                if(patientId.equals("96053")){
//                    int a=0;
//                }
//                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
//                if(patientAge>10 ) {
//                    Object object = document.get("payerCodes");
//                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
//
//                    List<String> payersList=mapDeliveryProcedurePayersCodeInList(deliveryProcedureInfos,payerInfoList);
//                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
//                    if (payersList.size() != 0) {
//                        for (String payerCode:payersList) {
//                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
//                            if (((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)
//                                    || payerCodeType.equals(CODE_TYPE_MEDICARE) || payerCodeType.equals("Exchange Codes")  ) && patientAge>10 && deliveryProcedureInfos.size()>0))
//                            {
//                                addObjectInSheet(document,payerCode,csvPrinter,payerCodeType);
//                            }
//                            else{
//                                Main.failedPatients.add(document.getString("id"));
//                            }
//                        }
//                    }
//                    else {
//                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
//                    }
//                }
//                else{
//                    Main.failedPatients.add(document.getString("id"));
//                }
//                csvPrinter.flush();
//            }
//            documents.clear();
//        } catch (Exception e) {
//            LOGGER.error("Exception for patient-Id :- " + patientId );;
//            LOGGER.error( e.toString());;
//        }
//    }
    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db,Map<String,String> dictionaryStringMap) throws IOException {
        String patientId="";
        try {
            List<String> sheetObj;
            List<PayerInfo> payerInfoList;
            List<DeliveryProcedureInfo> deliveryProcedureInfos;

            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
                patientId=document.getString("id");
                System.out.println("Processing patient: "+patientId);
                Object deliveryProcedureInfoObject = document.get("deliveryProcedureInfos");
                deliveryProcedureInfos = new ObjectMapper().convertValue(deliveryProcedureInfoObject, new TypeReference<List<DeliveryProcedureInfo>>() {});
                mapAllowedDeliveryProcedureInList(deliveryProcedureInfos);
                if(patientId.equals("100548")){
                    int a=0;
                }
                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                List<String> payersList=null;
                if(patientAge>10 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});

                    if(deliveryProcedureInfos.size()>0){
                        payersList=mapDeliveryProcedurePayersCodeInList(deliveryProcedureInfos,payerInfoList);
                    }
                    else{
                        payersList=mapPayersCodeInList(payerInfoList);

                    }
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                            if (((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)
                                    || payerCodeType.equals(CODE_TYPE_MEDICARE) || payerCodeType.equals("Exchange Codes")  ) && patientAge>10 && document.getInteger("Denominator 1")>0))
                            {
                                addObjectInSheet(document,payerCode,csvPrinter,payerCodeType);
                            }
                            else{
                                Main.failedPatients.add(document.getString("id"));
                            }
                        }
                    }
                    else {
                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }
                else{
                    Main.failedPatients.add(document.getString("id"));
                }
                csvPrinter.flush();
            }
            documents.clear();
        } catch (Exception e) {
            LOGGER.error("Exception for patient-Id :- " + patientId );;
            LOGGER.error( e.toString());;
        }
    }

}
