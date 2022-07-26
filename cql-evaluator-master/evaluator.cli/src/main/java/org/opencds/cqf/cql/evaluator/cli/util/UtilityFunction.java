package org.opencds.cqf.cql.evaluator.cli.util;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.opencds.cqf.cql.engine.retrieve.RetrieveProvider;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.engine.retrieve.DeliveryProcedureInfo;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class UtilityFunction {

    public CSVPrinter setupSheetHeaders() throws IOException {
        String SAMPLE_CSV_FILE = "C:\\Projects\\cql-evaluator-service\\cql-evaluator-master\\evaluator.cli\\src\\main\\resources\\sample.csv";
        String[] header = { "MemID", "Meas", "Payer","CE","Event","Epop","Excl","Num","RExcl","RExclD","Age","Gender"};
        FileWriter writer = new FileWriter(SAMPLE_CSV_FILE, true);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header));
        return csvPrinter;
    }

    public CSVPrinter setupSheetHeadersAppend() throws IOException {
        String SAMPLE_CSV_FILE = "C:\\Projects\\cql-evaluator-service\\cql-evaluator-master\\evaluator.cli\\src\\main\\resources\\sample.csv";
        FileWriter writer = new FileWriter(SAMPLE_CSV_FILE, true);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
        return csvPrinter;
    }

    public List<RetrieveProvider> mapToRetrieveProviderForSingle(String patientId, int skip, int limit, String fhirVersion, List<LibraryOptions> libraries, DbFunctions dbFunctions, DBConnection connection,String CollectionName) {
        DBConnection db = DBConnection.getConnection();
        PatientData patientData;
        PayerInfo payerInfo = new PayerInfo();
        List<RetrieveProvider> retrieveProviders = new ArrayList<>();

        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();

        List<Document> documents = dbFunctions.getSinglePatient(patientId, CollectionName, skip, limit, connection);
        for (Document document : documents) {
            patientData = new PatientData();
            patientData.setId(document.get("id").toString());
            patientData.setBirthDate(getConvertedDate(document.get("birthDate").toString()));
            patientData.setGender(document.get("gender").toString());
            patientData.setHospiceFlag(document.getString("hospiceFlag"));
            Object o = document.get("payerInfo");

            List<PayerInfo> payerCodes = new ObjectMapper().convertValue(o, new TypeReference<List<PayerInfo>>() {
            });

            patientData.setPayerInfo(payerCodes);

            bundle = (IBaseBundle) selectedParser.parseResource(document.toJson());
            RetrieveProvider retrieveProvider;
            retrieveProvider = new BundleRetrieveProvider(fhirContext, bundle, patientData, payerInfo);
            retrieveProviders.add(retrieveProvider);
        }
        return retrieveProviders;
    }

    public List<RetrieveProvider> mapToRetrieveProvider(int skip, int limit, String fhirVersion, List<LibraryOptions> libraries, DbFunctions dbFunctions, DBConnection connection,String CollectionName) {
        DBConnection db = DBConnection.getConnection();
        PatientData patientData;
        PayerInfo payerInfo = new PayerInfo();
        List<RetrieveProvider> retrieveProviders = new ArrayList<>();

        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();

        List<Document> documents = dbFunctions.getRemainingData(libraries.get(0).context.contextValue, CollectionName, skip, limit, connection);
        for(int i=0; i<documents.size(); i++) {

            patientData = new PatientData();
            patientData.setId(documents.get(i).get("id").toString());
            patientData.setBirthDate(getConvertedDate(documents.get(i).get("birthDate").toString()));
            patientData.setGender(documents.get(i).get("gender").toString());
            patientData.setHospiceFlag(documents.get(i).getString("hospiceFlag"));
            Object o = documents.get(i).get("payerInfo");

            List<PayerInfo> payerCodes = new ObjectMapper().convertValue(o, new TypeReference<List<PayerInfo>>() {});
            patientData.setPayerInfo(payerCodes);

            o=documents.get(i).get("deliveryProcedureInfos");
            List<DeliveryProcedureInfo> deliveryProcedureInfos = new ObjectMapper().convertValue(o, new TypeReference<List<DeliveryProcedureInfo>>() {});
            patientData.setDeliveryProcedureInfos(deliveryProcedureInfos);

            bundle = (IBaseBundle) selectedParser.parseResource(documents.get(i).toJson());
            RetrieveProvider retrieveProvider;
            retrieveProvider = new BundleRetrieveProvider(fhirContext, bundle, patientData, payerInfo);
            retrieveProviders.add(retrieveProvider);
        }
        return retrieveProviders;
    }

    public Date getConvertedDate(String birthDate) {
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(birthDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public String getConvertedDateString(Date birthDate) {
        DateFormat dateFormat = null;
        dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.format(birthDate);
        return  dateFormat.format(birthDate);
    }

    public Map<String,String> assignCodeToType(List<String> payerCodes, DbFunctions db, DBConnection connection) {
        //DBConnection db = new DBConnection();
        Map<String,String> codeTypes = new HashMap<>();
        String oid;
        for (String pCode : payerCodes) {
            //Document document = db.getOidInfo(pCode, "dictionary_ep_2022_code");
            List<Document> documents = db.getOidInfo(pCode, "dictionary_ep_2022_code", connection);
            Document document;
            if(documents.size() > 0) {
                document = documents.get(0);
                if(document != null) {
                    oid = (String) document.get("oid");
                    if(oid.equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                        codeTypes.put(pCode, CODE_TYPE_COMMERCIAL);
                    }
                    else if(oid.equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                        codeTypes.put(pCode, CODE_TYPE_MEDICAID);
                    }
                    else if(oid.equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                        codeTypes.put(pCode, CODE_TYPE_MEDICARE);
                    }
                }
            }

            if(pCode.equalsIgnoreCase("MCD")) {
                codeTypes.put(pCode, CODE_TYPE_MEDICAID);
            }
        }
        return codeTypes;
    }

    public void updatePayerCodesCCS(List<String> payerCodes, DbFunctions dbFunctions, DBConnection db) {
        int flag1 = 0;
        int flag2 = 0;
        int flag3  = 0;
        Map<String,String> codeTypes;
        if(payerCodes.size() == 2) {
            codeTypes = assignCodeToType(payerCodes,dbFunctions, db);
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


    public List<String> checkCodeForCCS() {

        List<String> codeCheckList = new ArrayList<>();
        codeCheckList.add("MCS");
        codeCheckList.add("MCR");
        codeCheckList.add("MP");
        codeCheckList.add("MC");
        return codeCheckList;
    }

    public String getFormattedDate(String date){

        if(date!=null){
            return (date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8));
        }
        return "";
    }

    public void saveScoreFile(HashMap<String, Map<String, Object>> finalResult, HashMap<String, PatientData> infoMap, Date measureDate, CSVPrinter csvPrinter) {
        try {
            List<String> data;
            PatientData patientData;
            Map<String, Object> exp;
            List<String> payerCodes = null;
            List<String> codeCheckList = new ArrayList<>();
            codeCheckList.add("MCS");
            codeCheckList.add("MCR");
            codeCheckList.add("MP");
            codeCheckList.add("MC");

            for(Map.Entry<String, Map<String, Object>> map : finalResult.entrySet()) {

                patientData = infoMap.get(map.getKey());
                exp = map.getValue();
                //payerCodes = patientData.getPayerCodes();

                //this.updatePayerCodes(payerCodes);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                for(int i=0; i< payerCodes.size(); i++) {
                    data = new ArrayList<>();
                    data.add(map.getKey());
                    data.add("CCS");
                    String payerCode = payerCodes.get(i);
                    data.add(String.valueOf(payerCode));
                    data.add(getIntegerString(Boolean.parseBoolean(exp.get("Enrolled During Participation Period For CE").toString())));
                    data.add("0"); //event
                    Boolean bol = Boolean.parseBoolean(exp.get("Exclusions").toString());
                    if(Boolean.parseBoolean(exp.get("Exclusions").toString()) || codeCheckList.stream().anyMatch(str -> str.trim().equals(payerCode))) {
                        data.add("0"); //Epop
                    }
                    else {
                        data.add(getIntegerString(Boolean.parseBoolean(exp.get("Denominator").toString()))); //Epop
                    }
                    data.add(getIntegerString(Boolean.parseBoolean(exp.get("Denominator Exceptions").toString()))); //exc
                    data.add(getIntegerString(Boolean.parseBoolean(exp.get("Numerator").toString())));
                    data.add("0"); //Rexl
                    data.add(getIntegerString(Boolean.parseBoolean(exp.get("Exclusions").toString()))); //RexclId
                    data.add(getAge(patientData.getBirthDate(), measureDate));
                    data.add(getGenderSymbol(patientData.getGender()));
                    csvPrinter.printRecord(data);
                }
            }
            csvPrinter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getIntegerString(boolean value) {
        if(value) {
            return "1";
        } else {
            return "0";
        }
    }

    public String getGenderSymbol(String gender) {
        if(!gender.isEmpty()) {
            if(gender.equalsIgnoreCase("Female")) {
                return "F";
            }
            if(gender.equalsIgnoreCase("male")) {
                return "M";
            }
        }
        return "N";
    }

    public LocalDate convertToLocalDateViaInstant(Date dateToConvert) {
        return dateToConvert.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
    }

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

    public String getAge(Date birthday, Date date) {
        LocalDate dob = convertToLocalDateViaInstant(birthday);
        LocalDate curDate = convertToLocalDateViaInstant(date);
        Period period = Period.between(dob, curDate);
        return String.valueOf(period.getYears());
    }
    public String getAgeV2(String birthday) {
        Calendar measurementDate = new GregorianCalendar(2022, 01, 01);
        Calendar dob = new GregorianCalendar(Integer.parseInt(birthday.substring(0,4)), Integer.parseInt(birthday.substring(4,6)), Integer.parseInt(birthday.substring(6,8)));
//
//determines the year of DOB and current date
        int age = measurementDate.get(Calendar.YEAR) - dob.get(Calendar.YEAR);
        if ((dob.get(Calendar.MONTH) > measurementDate.get(Calendar.MONTH)) || (dob.get(Calendar.MONTH) == measurementDate.get(Calendar.MONTH) && dob.get(Calendar.DAY_OF_MONTH) > measurementDate.get(Calendar.DAY_OF_MONTH)))
        {
//decrements age by 1
            age--;
        }
//prints the age
        return String.valueOf(age);
    }

    public static List<Document> getDeliveryProcedureInfoMap(List<DeliveryProcedureInfo> deliveryProcedureInfos) {
        List<HashMap<String,String>> mapList = new ArrayList<>();
        List<Document> documents = new LinkedList<>();
//        for(PayerInfo payerInfo: list) {
//            HashMap<String, String> patientMap = new HashMap<>();
//            patientMap.put("payerCode", payerInfo.getPayerCode());
//            patientMap.put("coverageStartDate", payerInfo.getCoverageStartDate());
//            patientMap.put("coverageEndDate", payerInfo.getCoverageEndDate());
//            patientMap.put("coverageStartDateString", payerInfo.getCoverageStartDateString());
//            patientMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
//            mapList.add(patientMap);
//        }


        if(null!=deliveryProcedureInfos) {
            for (DeliveryProcedureInfo deliveryProcedure : deliveryProcedureInfos) {
                Document document = new Document();
                document.put("procedureCode", deliveryProcedure.getProcedureCode());
                document.put("performedDateString", deliveryProcedure.getPerformedDateString());
                document.put("performedTimeString", deliveryProcedure.getPerformedTimeString());
                document.put("performedDate", deliveryProcedure.getPerformedDate());
                document.put("performedDateTime", deliveryProcedure.getPerformedDateTime());
                document.put("endDateString", deliveryProcedure.getEndDateString());
                document.put("endTimeString", deliveryProcedure.getEndTimeString());
                document.put("endDate", deliveryProcedure.getEndDate());
                documents.add(document);
            }
        }
        return documents;
    }
    public static List<Document> getPayerInfoMap(List<PayerInfo> list) {
        List<HashMap<String,String>> mapList = new ArrayList<>();
        List<Document> documents = new LinkedList<>();
//        for(PayerInfo payerInfo: list) {
//            HashMap<String, String> patientMap = new HashMap<>();
//            patientMap.put("payerCode", payerInfo.getPayerCode());
//            patientMap.put("coverageStartDate", payerInfo.getCoverageStartDate());
//            patientMap.put("coverageEndDate", payerInfo.getCoverageEndDate());
//            patientMap.put("coverageStartDateString", payerInfo.getCoverageStartDateString());
//            patientMap.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
//            mapList.add(patientMap);
//        }


        for(PayerInfo payerInfo: list) {
            Document document = new Document();
            document.put("payerCode", payerInfo.getPayerCode());
            document.put("coverageStartDate", payerInfo.getCoverageStartDate());
            document.put("coverageEndDate", payerInfo.getCoverageEndDate());
            document.put("coverageStartDateString", payerInfo.getCoverageStartDateString());
            document.put("coverageEndDateString", payerInfo.getCoverageEndDateString());
            documents.add(document);
        }

        return documents;
    }
    public static Date getParsedDateInRequiredFormat(String date, String format){
        SimpleDateFormat sdformat = new SimpleDateFormat(format);
        try{
            return sdformat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
