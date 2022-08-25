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
import org.opencds.cqf.cql.evaluator.engine.retrieve.Premium;

import java.io.IOException;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class ColEScoreSheet {

    UtilityFunction utilityFunction = new UtilityFunction();
    DbFunctions dbFunctions = new DbFunctions();

    public String getFieldCount(String fieldName, Document document) {
        int eventSum = 0;
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



    String getPayerCodeType(String payerCode ,DBConnection dbConnection){
        if(dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,dbConnection).size()>0){
            return  dbFunctions.getOidInfo(payerCode, Constant.EP_DICTIONARY,new DBConnection()).get(0).getString("oid");
        }
        return "";
    }

    public Map<String,String> assignCodeToTypeDMS(List<PayerInfo> payerCodes, DbFunctions db, DBConnection connection) {
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
                if (null!=insuranceEndDate && insuranceEndDate.compareTo(measurementPeriodEndingDate) >= 0
                        && !payerInfoList.get(i).getCoverageStartDateString().equals("20240101") && !(insuranceStartDate.compareTo(measurementPeriodEndingDate) > 0)) {
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

                        if (!lastCoverageObjectStartDate.equals("20240101") &&
                                (lastCoverageObjectEndDate.substring(0, 4).equals("2022") || (lastCoverageObjectEndDate.substring(0, 4).equals("2021")))) {
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
        else {
            for(String payerCode:payerCodes) {
                mapSpecialPayerCodes(updatedPayersList,payerCode);
            }
            payerCodes.clear();
            payerCodes.addAll(updatedPayersList);
        }
    }

    void addObjectInSheet(List<String> sheetObj, Document document, String payerCode, Date measureDate, CSVPrinter csvPrinter, boolean ltiFlag, boolean rexcleFlag) throws IOException {
        sheetObj.add(payerCode);
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period")));
        List<String> codeList = new LinkedList<>();
        codeList.add ("MEP");
        codeList.add ("MMO");
        codeList.add ("MPO");
        codeList.add ("MOS");

        sheetObj.add("0");   //event

        if(document.getBoolean("Exclusions") || document.getString("hospiceFlag").equals("Y") || ltiFlag || rexcleFlag) {
            sheetObj.add("0"); //epop
        }
        else {
            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator"))); //epop
        }

        sheetObj.add("0"); //excl
        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator"))); //Num


        if(document.getBoolean("Exclusions")) {
            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions"))); //rexcl
        }

        else if(rexcleFlag) {
            sheetObj.add("1");
        }
        else if(document.getString("hospiceFlag").equals("Y")) {
            sheetObj.add("1");
        }

        else if(ltiFlag) {
            sheetObj.add("1");
        }

        else {
            sheetObj.add("0");
        }
        sheetObj.add("0"); //RexlD
        sheetObj.add(utilityFunction.getAgeFromMeasureEndDate(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
        sheetObj.add(document.getString("race"));
        sheetObj.add(document.getString("ethnicity"));
        sheetObj.add(document.getString("raceDS"));
        sheetObj.add(document.getString("ethnicityDS"));
        csvPrinter.printRecord(sheetObj);
    }

    public boolean getRexclFlagBySN2(List<PayerInfo> payerInfoList, int patientAge) {
        Date anchorDate = utilityFunction.getConvertedDate("2022-12-31");
        Date endDate = utilityFunction.getConvertedDate("2022-01-01");
        boolean rexcleFlag = false;
        if (payerInfoList.size() > 0) {
            for (PayerInfo payerInfo : payerInfoList) {
                if (payerInfo.getPayerCode().equalsIgnoreCase("SN2") && patientAge >= 66) {
                    boolean dateOverLapFlag = anchorDate.compareTo(payerInfo.getCoverageStartDate()) >= 0
                            && payerInfo.getCoverageEndDate().compareTo(endDate) >= 0;
                    rexcleFlag = dateOverLapFlag;
                }
            }
        }
        return rexcleFlag;
    }

    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) throws IOException {
        try {
            boolean rexcleFlag;
            boolean ltiFlag;
            List<Premium> premiums;
            List<String> sheetObj;
            List<PayerInfo> payerInfoList;
            for (Document document : documents) {
                System.out.println("Processing patient: " + document.getString("id"));
                int patientAge = Integer.parseInt(utilityFunction.getAgeFromMeasureEndDate(utilityFunction.getConvertedDateString(document.getDate("birthDate"))));
                premiums = getPremiumFromDoc(document);
                Object object = document.get("payerCodes");
                payerInfoList = new ObjectMapper().convertValue(object, new TypeReference<List<PayerInfo>>() {});
                List<String> payersList = mapPayersCodeInList(payerInfoList);

                if ((patientAge >= 46 && patientAge <= 75)) {
                    rexcleFlag = getRexclFlagBySN2(payerInfoList, patientAge); //SN2 Check
                    updatePayerCodes(payersList, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions
                    if (payersList.size() != 0) {
                        for (String payerCode : payersList) {
                            sheetObj = new ArrayList<>();
                            sheetObj.add(document.getString("id"));
                            String measure = getMeasurePartType(document, payerCode, db, dbFunctions);
                            sheetObj.add(measure); //Measure

                            if (this.getLtiFlagAndMedicareFlag(premiums, payerCode, db, dbFunctions, patientAge)) {
                                ltiFlag = true;
                            } else {
                                ltiFlag = false;
                            }
                            addObjectInSheet(sheetObj, document, payerCode, measureDate, csvPrinter, ltiFlag, rexcleFlag);
                            rexcleFlag = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean getListHistFlag(List<Premium> premiums) {
        boolean listHistFlag = false;
        int flag2 = 0;
        String listHist;
        int dateFlag2 = 0;
        for (Premium premium : premiums) {
            if (null != premium.getLisHist()) {
                listHist = premium.getLisHist();
                if (listHist != null) {
                    int dateFlag = premium.getStartDate().compareTo(utilityFunction.getConvertedDate("2022-12-31"));
                    if (premium.getEndDate() != null) {
                        dateFlag2 = premium.getEndDate().compareTo(utilityFunction.getConvertedDate("2020-12-31"));
                        if ((dateFlag < 0 && dateFlag2 > 0) && (listHist.equalsIgnoreCase("D"))) {
                            flag2++;
                        }
                    }
                    else {
                        if ((dateFlag < 0) && (listHist.equalsIgnoreCase("D"))) {
                            flag2++;
                        }
                    }
                }
            }
            if (flag2 > 0) {
                listHistFlag = true;
            }
        }
        return listHistFlag;
    }

    public String getMeasurePartType(Document document, String code, DBConnection connection, DbFunctions db) {
        boolean listHistFlag;
        String measureName = "";
        String orec;
        String year;
        boolean measurePickedFlag = false;
        List<Premium> premiums = getPremiumFromDoc(document);
        boolean medicareFlag = getOidDetails(connection, code, db).equalsIgnoreCase(CODE_TYPE_MEDICARE);
        Premium finalPremium = new Premium();
        if(premiums.size()>0 && medicareFlag) {

           listHistFlag = getListHistFlag(premiums);

            for(Premium premium: premiums) {
                orec = premium.getOrec();
                if (null != orec && premium.getHospiceDateString().substring(0, 4).equalsIgnoreCase("2022")) {
                    finalPremium = premium;
                }
            }

            orec = finalPremium.getOrec();  //Final not null orec within 2022

            if (null != orec) {
                if (orec.equalsIgnoreCase("2") || orec.equalsIgnoreCase("9") && !measurePickedFlag) {
                    measureName = "COLOT";
                    measurePickedFlag = true;
                }
            }

            if (null != orec) {
                if (orec.equalsIgnoreCase("0") && !listHistFlag  && !measurePickedFlag) {
                    measureName = "COLNON";
                    measurePickedFlag = true;
                }
            }

            if (null != orec) {
                if ((orec.equalsIgnoreCase("1") || orec.equalsIgnoreCase("3")) && listHistFlag && !measurePickedFlag) {
                    measureName = "COLCMB";
                    measurePickedFlag = true;
                }
            }

            if (null != orec) {
                if ((orec.equalsIgnoreCase("1") || orec.equalsIgnoreCase("3")) && !listHistFlag && !measurePickedFlag) {
                    measureName = "COLDIS";
                    measurePickedFlag = true;
                }

            }

            if (null != orec) {
                if (orec.equalsIgnoreCase("0") && listHistFlag && !measurePickedFlag) {
                    measureName = "COLLISDE";
                }
            }

            if(measureName.equalsIgnoreCase("") && medicareFlag && !listHistFlag) {
                measureName = "COLNON";
            }
        }
        else {
            measureName = "COL";
        }
        return measureName;
    }

    public List<Premium> getPremiumFromDoc(Document document) {
        List<Premium> premiums = new ArrayList<>();
        if(null != document.get("premium")) {
            Object prem = document.get("premium");
            premiums = new ObjectMapper().convertValue(prem, new TypeReference<List<Premium>>() {});
        }
        return premiums;
    }

    public String getOidDetails(DBConnection connection, String code, DbFunctions db) {
        List<Document> documents = db.getOidInfo(code, "dictionary_ep_2022_code", connection);
        Document document = documents.get(0);
        return  (String) document.get("oid");
    }

    public boolean getLtiFlagAndMedicareFlag(List<Premium> premiums, String code, DBConnection connection, DbFunctions db, int age) {
        int flag1 = 0;
        int flag2 = 0;
        if(getOidDetails(connection, code, db).equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
            flag1++;
        }

        if(premiums.size() > 0) {
            for(Premium premium: premiums) {
                if(null != premium.getLti() ) {
                    if(premium.getLti().equalsIgnoreCase("Y")) {
                        String year = premium.getHospiceDateString().substring(0,4);
                        if(year.equalsIgnoreCase("2022") && !year.equalsIgnoreCase("2024") && age >= 66) {
                            flag2++;
                        }
                    }
                }
            }
        }
        if(flag1 > 0 && flag2 > 0) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean getPremiumFlag(List<Premium> premiums) {
        int flag = 0;
        if(premiums.size()>0) {
            for(Premium premium: premiums) {
                if(null != premium.getOrec() ) {
                    if(!premium.getOrec().equalsIgnoreCase("")) {
                        String year = premium.getHospiceDateString().substring(0,4);
                        if(!year.equalsIgnoreCase("2022") && !year.equalsIgnoreCase("2024")) {
                            flag++;
                        }
                    }
                }

                if(null != premium.getLisHist() ) {
                    if(!premium.getLisHist().equalsIgnoreCase("")) {
                        String year = premium.getStartDateString().substring(0,4);
                        if(!year.equalsIgnoreCase("2022") && !year.equalsIgnoreCase("2024")) {
                            flag++;
                        }
                    }
                }
            }
        }
        if(flag > 0) {
            return true;
        }
        else {
            return false;
        }
    }
}
