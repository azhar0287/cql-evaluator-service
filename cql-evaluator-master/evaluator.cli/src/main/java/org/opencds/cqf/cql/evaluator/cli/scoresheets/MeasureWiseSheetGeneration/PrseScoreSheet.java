package org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
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

public class PrseScoreSheet {

    public static Logger LOGGER  = LogManager.getLogger(PrseScoreSheet.class);

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

    public static Document createDocumentForPrseResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", UtilityFunction.getPayerInfoMap(patientData.getPayerInfo()));
        document.put("deliveryProcedureInfos", UtilityFunction.getDeliveryProcedureInfoMap(patientData.getDeliveryProcedureInfos()));
        document.put("hospiceFlag",patientData.getHospiceFlag());

        document.put("Gestational Age Diagnosis",getSize(expressionResults.get("Gestational Age Diagnosis")));
        document.put("Delivery",getSize(expressionResults.get("Delivery")));
        document.put("Member Coverage",getSize(expressionResults.get("Member Coverage")));
        document.put("Initial Population 1",getSize(expressionResults.get("Initial Population 1")));
        document.put("Initial Population 2",getSize(expressionResults.get("Initial Population 2")));
        document.put("Initial Population 3",getSize(expressionResults.get("Initial Population 3")));
        document.put("Denominator 1",getSize(expressionResults.get("Denominator 1")));
        document.put("Denominator 2",getSize(expressionResults.get("Denominator 2")));
        document.put("Denominator 3",getSize(expressionResults.get("Denominator 3")));
        document.put("Delivery in Measurement Period with Hospice Intervention or Encounter",getSize(expressionResults.get("Delivery in Measurement Period with Hospice Intervention or Encounter")));
        document.put("Exclusions 1",getSize(expressionResults.get("Exclusions 1")));
        document.put("Exclusions 2",getSize(expressionResults.get("Exclusions 2")));
        document.put("Exclusions 3",getSize(expressionResults.get("Exclusions 3")));
        document.put("Influenza Vaccine",getSize(expressionResults.get("Influenza Vaccine")));
        document.put("Delivery with Influenza Vaccine Between July 1 of Year Prior to Measurement Period and Delivery Date",getSize(expressionResults.get("Delivery with Influenza Vaccine Between July 1 of Year Prior to Measurement Period and Delivery Date")));
        document.put("Numerator 1",getSize(expressionResults.get("Numerator 1")));
        document.put("Tdap Vaccine",getSize(expressionResults.get("Tdap Vaccine")));
        document.put("Delivery with Tdap Vaccine during Pregnancy",getSize(expressionResults.get("Delivery with Tdap Vaccine during Pregnancy")));
        document.put("Td or Tdap Vaccine Contraindications",getSize(expressionResults.get("Td or Tdap Vaccine Contraindications")));
        document.put("Delivery with Td or Tdap Vaccine Contraindications on or before Delivery Date",getSize(expressionResults.get("Delivery with Td or Tdap Vaccine Contraindications on or before Delivery Date")));
        document.put("Numerator 2",getSize(expressionResults.get("Numerator 2")));
        document.put("Delivery with Influenza Criteria and Tdap Criteria",getSize(expressionResults.get("Delivery with Influenza Criteria and Tdap Criteria")));
        document.put("Numerator 3",getSize(expressionResults.get("Numerator 3")));

        document.put("checkIfCodesPresentInCondition",expressionResults.get("checkIfCodesPresentInCondition"));


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
        /////////////////////////////  PRSINFLA Mapping ////////////////////////////////////////////////
        List<String> sheetObjPrsInfl1A = new LinkedList<>();
        sheetObjPrsInfl1A.add(document.getString("id"));
        sheetObjPrsInfl1A.add("PRSINFLA");
        sheetObjPrsInfl1A.add(payerCode);
        if(document.getInteger("Initial Population 1") > 0){//CE
            sheetObjPrsInfl1A.add("1");
        }
        else{
            sheetObjPrsInfl1A.add("0");
        }

        sheetObjPrsInfl1A.add("1");//Event


        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
            if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
                if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                    sheetObjPrsInfl1A.add("0"); //epop (Also known as denominator)
                }
                else {
                    sheetObjPrsInfl1A.add("1"); //epop
                }
            }else{
                sheetObjPrsInfl1A.add("0");
            }
        }
        else{
            sheetObjPrsInfl1A.add("0");
        }


        sheetObjPrsInfl1A.add("0"); //excl


        if((document.getInteger("Denominator 1")==2) && document.getInteger("Numerator 1") > 0  && document.getInteger("Denominator 1") > 0 ){
            sheetObjPrsInfl1A.add("0");
            numerator1A="0";
        }else if(document.getInteger("Numerator 1") > 0  && document.getInteger("Denominator 1") > 0 ){
            sheetObjPrsInfl1A.add("1");
            numerator1A="1";
        }else{
            sheetObjPrsInfl1A.add("0");
            numerator1A="0";
        }


        if(document.getInteger("Exclusions 1")>0){
            sheetObjPrsInfl1A.add("1"); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjPrsInfl1A.add("1");
        }
        else{
            sheetObjPrsInfl1A.add("0");
        }


        sheetObjPrsInfl1A.add("0"); //RexlD

        if(deliveryProcedureInfos.size()>1){
            sheetObjPrsInfl1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
        }
        else if(deliveryProcedureInfos.size()>0){
            sheetObjPrsInfl1A.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));

        }else{
            sheetObjPrsInfl1A.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

        }
        sheetObjPrsInfl1A.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjPrsInfl1A);


        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PRSINFLB Mapping ////////////////////////////////////////////////
        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
            List<String> sheetObjPrsInfl1B = new LinkedList<>();
            sheetObjPrsInfl1B.add(document.getString("id"));
            sheetObjPrsInfl1B.add("PRSINFLB");
            sheetObjPrsInfl1B.add(payerCode);

            sheetObjPrsInfl1B.add("1");//CE
//            if(document.getInteger("Initial Population 1") >0 ){//CE
//                sheetObjPrsInfl1B.add("1");
//            }
//            else{
//                sheetObjPrsInfl1B.add("0");
//            }

            sheetObjPrsInfl1B.add("1");//Event
//            if(document.getInteger("Denominator 1")==2){//Event
//                sheetObjPds1B.add("1");
//            }
//            else{
//                sheetObjPds1B.add("0");
//            }
            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)){
                if(document.getInteger("Initial Population 1")>0 && document.getInteger("Denominator 1")>0){
                    if(document.getInteger("Exclusions 1")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                        sheetObjPrsInfl1B.add("0"); //epop (Also known as denominator)
                    }
                    else {
                        sheetObjPrsInfl1B.add("1"); //epop
                    }
                }
                else{
                    sheetObjPrsInfl1B.add("0");
                }
            }
            else{
                sheetObjPrsInfl1B.add("0");
            }

            sheetObjPrsInfl1B.add("0"); //excl

            if(document.getInteger("Numerator 1") > 0 && document.getInteger("Denominator 1") > 0 ){
                sheetObjPrsInfl1B.add("1");
                numerator1B="1";
            }
            else{
                sheetObjPrsInfl1B.add("0");
                numerator1B="0";
            }

            sheetObjPrsInfl1B.add("0"); //rexcl
//            if(document.getInteger("Exclusions 1")>0){
//                sheetObjPrsInfl1B.add("1"); //rexcl
//            }
//            else if(document.getString("hospiceFlag").equals("Y")) {
//                sheetObjPrsInfl1B.add("1");
//            }
//            else{
//                sheetObjPrsInfl1B.add("0");
//            }

            sheetObjPrsInfl1B.add("0"); //RexlD

            if(document.getInteger("Denominator 1") > 0){
                if(deliveryProcedureInfos.size()==0){
                    sheetObjPrsInfl1B.add(String.valueOf(Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))))+1));

                }
                else{
                    sheetObjPrsInfl1B.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));

                }

            }else{
                sheetObjPrsInfl1B.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

            }
            sheetObjPrsInfl1B.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjPrsInfl1B);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PRSTDA Mapping ////////////////////////////////////////////////

        List<String> sheetObjPrsTdA = new LinkedList<>();
        sheetObjPrsTdA.add(document.getString("id"));
        sheetObjPrsTdA.add("PRSTDA");
        sheetObjPrsTdA.add(payerCode);
        if(document.getInteger("Initial Population 2")>0){//CE
            sheetObjPrsTdA.add("1");
        }
        else{
            sheetObjPrsTdA.add("0");
        }

        sheetObjPrsTdA.add("1");//Event
//        if(document.getInteger("Denominator 2")>0 && numerator1A.equals("1")){//Event
//            sheetObjPrsTdA.add("1");
//        }
//        else{
//            sheetObjPrsTdA.add("0");
//        }


        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
            if(document.getInteger("Initial Population 2") > 0 && document.getInteger("Denominator 2")>0 && numerator1A.equals("1")){
                if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                    sheetObjPrsTdA.add("0"); //epop (Also known as denominator)
                }
                else {
                    sheetObjPrsTdA.add("1"); //epop
                }
            }
            else{
                sheetObjPrsTdA.add("0"); //epop
            }
        }
        else{
            sheetObjPrsTdA.add("0"); //epop

        }

        sheetObjPrsTdA.add("0"); //excl

        if(document.getInteger("Numerator 2") ==1 && document.getInteger("Denominator 1")== 1){
            sheetObjPrsTdA.add("1");
        }
        else{
            sheetObjPrsTdA.add("0");

        }


        if(document.getInteger("Exclusions 2")>0){
            sheetObjPrsTdA.add("1"); //rexcl
        }else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjPrsTdA.add("1");//rexcl
        }else{
            sheetObjPrsTdA.add("0");//rexcl
        }


        sheetObjPrsTdA.add("0"); //RexlD

        if(deliveryProcedureInfos.size()>1){
            sheetObjPrsTdA.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
        }
        else if(deliveryProcedureInfos.size()>0){
            sheetObjPrsTdA.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));
        }else{
            sheetObjPrsTdA.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        }
        sheetObjPrsTdA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjPrsTdA);


        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PRSTDB Mapping ////////////////////////////////////////////////
        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
            List<String> sheetObjPrsTdB = new LinkedList<>();
            sheetObjPrsTdB.add(document.getString("id"));
            sheetObjPrsTdB.add("PRSTDB");
            sheetObjPrsTdB.add(payerCode);

            sheetObjPrsTdB.add("1");//CE
//            if(document.getInteger("Initial Population 2") > 0 ){//CE
//                sheetObjPrsTdB.add("1");
//            }
//            else{
//                sheetObjPrsTdB.add("0");
//            }

            sheetObjPrsTdB.add("1");//Event
//            if(numerator1B.equals("1")){//Event
//                sheetObjPrsTdB.add("1");
//            }
//            else{
//                sheetObjPrsTdB.add("0");
//            }

            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
                if(document.getInteger("Initial Population 2")>0 && document.getInteger("Denominator 2")>0){
                    if(document.getInteger("Exclusions 2")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                        sheetObjPrsTdB.add("0"); //epop (Also known as denominator)
                    }
                    else {
                        sheetObjPrsTdB.add("1"); //epop
                    }
                }
                else{
                    sheetObjPrsTdB.add("0");//epop
                }
            }
            else{
                sheetObjPrsTdB.add("0");//epop

            }

            sheetObjPrsTdB.add("0"); //excl


            if(document.getInteger("Numerator 2") ==1 && document.getInteger("Denominator 1")== 1){
                sheetObjPrsTdB.add("1");//num
            }
            else{
                sheetObjPrsTdB.add("0");//num
            }

//            if(document.getInteger("Exclusions 2")>0){
//                sheetObjPrsTdB.add("1"); //rexcl
//            }
//            else if(document.getString("hospiceFlag").equals("Y")) {
//                sheetObjPrsTdB.add("1");
//            }
//            else{
//                sheetObjPrsTdB.add("0");
//            }
            sheetObjPrsTdB.add("0");//rexcl

            sheetObjPrsTdB.add("0"); //RexlD

            if(document.getInteger("Denominator 1") > 0){
                if(deliveryProcedureInfos.size()==0){
                    sheetObjPrsTdB.add(String.valueOf(Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))))+1));

                }
                else{
                    sheetObjPrsTdB.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));

                }
            }else{
                sheetObjPrsTdB.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

            }
            sheetObjPrsTdB.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjPrsTdB);
        }



        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PRSCMBA Mapping ////////////////////////////////////////////////

        List<String> sheetObjPrsCmbA = new LinkedList<>();
        sheetObjPrsCmbA.add(document.getString("id"));
        sheetObjPrsCmbA.add("PRSCMBA");
        sheetObjPrsCmbA.add(payerCode);

        if(document.getInteger("Initial Population 3")>0){//CE
            sheetObjPrsCmbA.add("1");
        }
        else{
            sheetObjPrsCmbA.add("0");
        }

//        if(document.getInteger("Denominator 3")>0 && numerator1A.equals("1")){//Event
//            sheetObjPrsCmbA.add("1");
//        }
//        else{
//            sheetObjPrsCmbA.add("0");
//        }
        sheetObjPrsCmbA.add("1");//Event

        if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
            if(document.getInteger("Initial Population 3") > 0 && document.getInteger("Denominator 3")>0 && numerator1A.equals("1")){
                if(document.getInteger("Exclusions 3")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                    sheetObjPrsCmbA.add("0"); //epop (Also known as denominator)
                }
                else {
                    sheetObjPrsCmbA.add("1"); //epop
                }
            }
            else{
                sheetObjPrsCmbA.add("0"); //epop
            }
        }
        else{
            sheetObjPrsCmbA.add("0"); //epop
        }

        sheetObjPrsCmbA.add("0"); //excl

        if(document.getInteger("Numerator 3") ==1 && document.getInteger("Denominator 3")== 1){
            sheetObjPrsCmbA.add("1");
        }
        else{
            sheetObjPrsCmbA.add("0");

        }

        if(document.getInteger("Exclusions 3")>0){
            sheetObjPrsCmbA.add("1"); //rexcl
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObjPrsCmbA.add("1");//rexcl
        }
        else{
            sheetObjPrsCmbA.add("0");//rexcl
        }

        sheetObjPrsCmbA.add("0"); //RexlD

        if(deliveryProcedureInfos.size()>1){
            sheetObjPrsCmbA.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-2).getPerformedDateString()));
        }
        else if(deliveryProcedureInfos.size()>0){
            sheetObjPrsCmbA.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(deliveryProcedureInfos.size()-1).getPerformedDateString()));
        }else{
            sheetObjPrsCmbA.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        }
        sheetObjPrsCmbA.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObjPrsCmbA);


        ///////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////  PRSCMBB Mapping ////////////////////////////////////////////////
        if(document.getInteger("Denominator 1")>1){
//        if(deliveryProcedureInfos.size()==2){
            List<String> sheetObjPrsCmbB = new LinkedList<>();
            sheetObjPrsCmbB.add(document.getString("id"));
            sheetObjPrsCmbB.add("PRSCMBB");
            sheetObjPrsCmbB.add(payerCode);

//            if(document.getInteger("Initial Population 2") > 0 ){//CE
//                sheetObjPrsCmbB.add("1");
//            }
//            else{
//                sheetObjPrsCmbB.add("0");
//            }
            sheetObjPrsCmbB.add("1");//CE

//            if(numerator1B.equals("1")){//Event
//                sheetObjPrsCmbB.add("1");
//            }
//            else{
//                sheetObjPrsCmbB.add("0");
//            }
            sheetObjPrsCmbB.add("1");//Event

            if(payerCodeType.equals(CODE_TYPE_COMMERCIAL) || payerCodeType.equals(CODE_TYPE_MEDICAID)) {
                if(document.getInteger("Initial Population 3")>0 && document.getInteger("Denominator 3")>0){
                    if(document.getInteger("Exclusions 3")>0 || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
                        sheetObjPrsCmbB.add("0"); //epop (Also known as denominator)
                    }
                    else {
                        sheetObjPrsCmbB.add("1"); //epop
                    }
                }
                else{
                    sheetObjPrsCmbB.add("0");
                }
            }
            else{
                sheetObjPrsCmbB.add("0");

            }

            sheetObjPrsCmbB.add("0"); //excl

            if(document.getInteger("Numerator 3") ==1 && document.getInteger("Denominator 3")== 1){
                sheetObjPrsCmbB.add("1");//Num
            }
            else{
                sheetObjPrsCmbB.add("0");//Num

            }

//            if(document.getInteger("Exclusions 2")>0){
//                sheetObjPrsCmbB.add("1"); //rexcl
//            }
//            else if(document.getString("hospiceFlag").equals("Y")) {
//                sheetObjPrsCmbB.add("1");
//            }
//            else{
//                sheetObjPrsCmbB.add("0");
//            }
            sheetObjPrsCmbB.add("0"); //rexcl

            sheetObjPrsCmbB.add("0"); //RexlD
            if(document.getInteger("Denominator 1") > 0){
                if(deliveryProcedureInfos.size()==0){
                    sheetObjPrsCmbB.add(String.valueOf(Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))))+1));

                }
                else{
                    sheetObjPrsCmbB.add(getProcedureBasedAge(utilityFunction.getConvertedDateString(document.getDate("birthDate")),deliveryProcedureInfos.get(1).getPerformedDateString()));

                }
            }else{
                sheetObjPrsCmbB.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));

            }
            sheetObjPrsCmbB.add(utilityFunction.getGenderSymbol(document.getString("gender")));
            csvPrinter.printRecord(sheetObjPrsCmbB);
        }

    }


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
                    String anchorDateString = getAnchorDate(deliveryProcedureInfo.getPerformedDate(), -30);
                    String sixtyDaysAnchorDateString = getAnchorDate(deliveryProcedureInfo.getPerformedDate(), 0);
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
                if(patientId.equals("95059")){
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
                                    || payerCodeType.equals(CODE_TYPE_MEDICARE) || payerCodeType.equals("Exchange Codes")  ) && patientAge>10 && document.getInteger("Delivery")>0))
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
