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

public class CoueScoreSheet {

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

    public static Document createDocumentForCoueResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", UtilityFunction.getPayerInfoMap(patientData.getPayerInfo()));
        document.put("deliveryProcedureInfos", UtilityFunction.getDeliveryProcedureInfoMap(patientData.getDeliveryProcedureInfos()));
        document.put("hospiceFlag",patientData.getHospiceFlag());



        document.put("Member Claim Responses",getSize(expressionResults.get("Member Claim Responses")));
        document.put("Member Claims",getSize(expressionResults.get("Member Claims")));
        document.put("Member is Appropriate Age and Has IPSD with Negative Medication History",expressionResults.get("Member is Appropriate Age and Has IPSD with Negative Medication History"));
        document.put("Member Coverage",getSize(expressionResults.get("Member Coverage")));
        document.put("Enrolled During Participation Period",expressionResults.get("Enrolled During Participation Period"));
        document.put("Initial Population 1",expressionResults.get("Initial Population 1"));
        document.put("Initial Population 2",expressionResults.get("Initial Population 2"));
        document.put("Denominator 1",expressionResults.get("Denominator 1"));
        document.put("Denominator 2",expressionResults.get("Denominator 2"));
        document.put("Exclusions 1",expressionResults.get("Exclusions 1"));
        document.put("Exclusions 2",expressionResults.get("Exclusions 2"));
        document.put("Numerator 1",expressionResults.get("Numerator 1"));
        document.put("Numerator 2",expressionResults.get("Numerator 2"));
        document.put("Opioid Coverage Intervals in 30 Days on or after IPSD",getSize(expressionResults.get("Opioid Coverage Intervals in 30 Days on or after IPSD")));
        document.put("Opioid Coverage Intervals in 62 Days on or after IPSD",getSize(expressionResults.get("Opioid Coverage Intervals in 62 Days on or after IPSD")));

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

    boolean deliveryProceduresOnSameDateExsists(List<DeliveryProcedureInfo> deliveryProcedureInfos){

        boolean flag=false;
        if(deliveryProcedureInfos.size()>1){
            List<String> deliveryProceduresPerformedDates=new ArrayList<>();
            for(DeliveryProcedureInfo deliveryProcedureInfo: deliveryProcedureInfos){
                deliveryProceduresPerformedDates.add(deliveryProcedureInfo.getPerformedDateString());
            }
            for(String procedurePerformedDate:deliveryProceduresPerformedDates){
                int occurrences = Collections.frequency(deliveryProceduresPerformedDates, procedurePerformedDate);
                if(occurrences>1){
                    deliveryProceduresPerformedDates.removeAll(Collections.singleton(procedurePerformedDate));
                    if(deliveryProceduresPerformedDates.size()>0){
                        return false;
                    }
                    else{
                        return true;
                    }
                }
            }
        }
        return false;
    }

    DeliveryProcedureInfo getFirstOccurenceOfObject(List<DeliveryProcedureInfo> deliveryProcedureInfos,String deliveryProceduresPerformedDate){
        for(DeliveryProcedureInfo deliveryProcedureInfo:deliveryProcedureInfos){
            if(deliveryProcedureInfo.getPerformedDateString().equals(deliveryProceduresPerformedDate)){
                return deliveryProcedureInfo;
            }
        }
        return null;
    }

    List<DeliveryProcedureInfo> removeDuplicateDeliveryFromList(List<DeliveryProcedureInfo> deliveryProcedureInfos){
        Set<String> deliveryProceduresPerformedDatesSet=new HashSet<>();
        for(DeliveryProcedureInfo deliveryProcedureInfo: deliveryProcedureInfos){
            deliveryProceduresPerformedDatesSet.add(deliveryProcedureInfo.getPerformedDateString());
        }
        List<String> deliveryProceduresPerformedDatesList = new ArrayList<String>(deliveryProceduresPerformedDatesSet);
        Collections.sort(deliveryProceduresPerformedDatesList);

        List<DeliveryProcedureInfo> deliveryProcedureInfoListTemp=new ArrayList<>();

        for(String deliveryDate:deliveryProceduresPerformedDatesList){
            deliveryProcedureInfoListTemp.add(getFirstOccurenceOfObject(deliveryProcedureInfos,deliveryDate));

        }
        return deliveryProcedureInfoListTemp;

    }

    void addObjectInSheet( Document document, String payerCode, CSVPrinter csvPrinter,String payerCodeType) throws IOException {
        List<String> sheetObj = null;
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Event")));   //event

        if(document.getBoolean("Exclusions 1") || document.getString("hospiceFlag").equals("Y") || codeList.stream().anyMatch(str-> str.equalsIgnoreCase(payerCode)) ){
            sheetObj.add("0"); //epop
        }
        else {
            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator"))); //epop

        }

        sheetObj.add("0"); //excl

        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator"))); //Num

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
        sheetObj.add(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        csvPrinter.printRecord(sheetObj);
    }


    void mapAllowedDeliveryProcedureInList(List<DeliveryProcedureInfo> deliveryProcedureInfoList){
        Date intervalStartDate = UtilityFunction.getParsedDateInRequiredFormat("2022-01-01", "yyyy-MM-dd");
        Date intervalEndDate = UtilityFunction.getParsedDateInRequiredFormat("2022-12-31", "yyyy-MM-dd");

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

    public List<String> mapPayersCodeInList(List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> payerInfoList){
        List<String> payersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {
            Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat("2022-12-31", "yyyy-MM-dd");
            Date insuranceEndDate = null,insuranceStartDate=null;
            for (int i = 0; i < payerInfoList.size(); i++) {
                insuranceEndDate = utilityFunction.resetTimeZoneFrom( payerInfoList.get(i).getCoverageEndDate() );
                insuranceStartDate=utilityFunction.resetTimeZoneFrom( payerInfoList.get(i).getCoverageStartDate() );
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

    public List<String> mapPayersCodeInList(List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> payerInfoList, Date birthDate){
        /// Add the 2 year in the birthdate that will be my anchor date
        String anchorDate=getAnchorDate(birthDate,2);
        List<String> payersList=new LinkedList<>();
        List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> tempPayersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {
            Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat(anchorDate, "yyyy-MM-dd");
            for (org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo payerInfo : payerInfoList) {
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
                for (org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo payerInfo : payerInfoList) {
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

    public List<String> mapDeliveryProcedurePayersCodeInList(List<DeliveryProcedureInfo> deliveryProcedureInfos,List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> payerInfoList){
        /// Add the 2 year in the birthdate that will be my anchor date
        Set<String> payerSet = new HashSet<String>();
        Set<String> payersList=new HashSet<>();
        List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> tempPayersList=new LinkedList<>();
        if(payerInfoList != null && payerInfoList.size() != 0) {

            for (DeliveryProcedureInfo deliveryProcedureInfo : deliveryProcedureInfos) {
                String sixtyDaysAnchorDate=getAnchorDate(deliveryProcedureInfo.getPerformedDate(),0);

                Date measurementPeriodEndingDate = UtilityFunction.getParsedDateInRequiredFormat(sixtyDaysAnchorDate, "yyyy-MM-dd");
                for (org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo payerInfo : payerInfoList) {
                    Date insuranceEndDate =utilityFunction.resetTimeZoneFrom( payerInfo.getCoverageEndDate());
                    Date insuranceStartDate = utilityFunction.resetTimeZoneFrom(payerInfo.getCoverageStartDate());
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
                        org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo payerInfo = payerInfoList.get(z);
                        Date insuranceEndDate = utilityFunction.resetTimeZoneFrom(payerInfo.getCoverageEndDate());
                        Date insuranceStartDate = utilityFunction.resetTimeZoneFrom(payerInfo.getCoverageStartDate());

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
            List<org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo> payerInfoList;
            List<DeliveryProcedureInfo> deliveryProcedureInfos;

            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
                patientId=document.getString("id");
                System.out.println("Processing patient: "+patientId);
                Object deliveryProcedureInfoObject = document.get("deliveryProcedureInfos");
                deliveryProcedureInfos = new ObjectMapper().convertValue(deliveryProcedureInfoObject, new TypeReference<List<DeliveryProcedureInfo>>() {});
                mapAllowedDeliveryProcedureInList(deliveryProcedureInfos);
                if(patientId.equals("125437")){
                    int a=0;
                }
                int patientAge = Integer.parseInt(utilityFunction.getAgeV2(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                List<String> payersList=null;
                if(patientAge>18 ) {
                    Object object = document.get("payerCodes");
                    payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});

//                    if(deliveryProcedureInfos.size()>0){
//                        payersList=mapDeliveryProcedurePayersCodeInList(deliveryProcedureInfos,payerInfoList);
//                    }
//                    else{
                    payersList=mapPayersCodeInList(payerInfoList);
//                    }
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode:payersList) {
                            String payerCodeType = getPayerCodeType(payerCode,db,dictionaryStringMap);
                            if (((payerCodeType.equals(Constant.CODE_TYPE_COMMERCIAL) || payerCodeType.equals(Constant.CODE_TYPE_MEDICAID)
                                    || payerCodeType.equals(CODE_TYPE_MEDICARE) || payerCodeType.equals("Exchange Codes")  )

                                    && patientAge>18 ))
                            {
                                addObjectInSheet(document,payerCode,csvPrinter,payerCodeType);
                            }else{
                                Main.failedPatients.add(document.getString("id"));
                            }
                        }
                    }else {
                        Main.failedPatients.add(document.getString("id"));//patients missed due to payerlist size=0
                    }
                }else{
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
