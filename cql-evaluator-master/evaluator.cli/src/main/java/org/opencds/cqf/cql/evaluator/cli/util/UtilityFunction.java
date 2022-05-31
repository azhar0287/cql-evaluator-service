package org.opencds.cqf.cql.evaluator.cli.util;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.opencds.cqf.cql.engine.retrieve.RetrieveProvider;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class UtilityFunction {

    public List<RetrieveProvider> mapToRetrieveProvider(int skip, int limit, String fhirVersion, List<LibraryOptions> libraries) {
        DBConnection db = new DBConnection();
        PatientData patientData ;
        List<RetrieveProvider> retrieveProviders = new ArrayList<>();

        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();

        List<Document> documents = db.getConditionalData(libraries.get(0).context.contextValue, "ep_encounter_fhir", skip, limit);
        for(Document document : documents) {
            patientData = new PatientData();
            patientData.setId(document.get("id").toString());
            patientData.setBirthDate(getConvertedDate(document.get("birthDate").toString()));
            patientData.setGender(document.get("gender").toString());
            Object o = document.get("payerCodes");

            List<String> payerCodes = new ObjectMapper().convertValue(o, new TypeReference<List<String>>() {});

            patientData.setPayerCodes(payerCodes);

            bundle = (IBaseBundle) selectedParser.parseResource(document.toJson());
            RetrieveProvider retrieveProvider;
            retrieveProvider = new BundleRetrieveProvider(fhirContext, bundle, patientData);
            retrieveProviders.add(retrieveProvider);
        }
        return retrieveProviders;
    }

    Date getConvertedDate(String birthDate) {
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(birthDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public Map<String,String> assignCodeToType(List<String> payerCodes) {
        DBConnection db = new DBConnection();
        Map<String,String> codeTypes = new HashMap<>();
        String oid;
        for (String pCode : payerCodes) {
            Document document = db.getOidInfo(pCode, "dictionary_ep_2022_code");
            if(document != null) {
                oid = (String) document.get("oid");
                if(oid.equalsIgnoreCase(CODE_TYPE_COMMERCIAL)) {
                    codeTypes.put(pCode, CODE_TYPE_COMMERCIAL);
                }
                if(oid.equalsIgnoreCase(CODE_TYPE_MEDICAID)) {
                    codeTypes.put(pCode, CODE_TYPE_MEDICAID);
                }
                if(oid.equalsIgnoreCase(CODE_TYPE_MEDICARE)) {
                    codeTypes.put(pCode, CODE_TYPE_MEDICARE);
                }
            }
            if(pCode.equalsIgnoreCase("MCD")) {
                codeTypes.put(pCode, CODE_TYPE_MEDICAID);
            }
        }
        return codeTypes;
    }

    public void updatePayerCodes(List<String> payerCodes) {
        int flag1 = 0;
        int flag2 = 0;
        int flag3  = 0;
        Map<String,String> codeTypes;
        if(payerCodes.size() == 2) {
            codeTypes = assignCodeToType(payerCodes);
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

    public void saveScoreFile(HashMap<String, Map<String, Object>> finalResult, HashMap<String, PatientData> infoMap, Date measureDate, CSVPrinter csvPrinter) {
        try {
            List<String> data;
            PatientData patientData;
            Map<String, Object> exp;
            List<String> payerCodes;
            List<String> codeCheckList = new ArrayList<>();
            codeCheckList.add("MCS");
            codeCheckList.add("MCR");
            codeCheckList.add("MP");
            codeCheckList.add("MC");

            for(Map.Entry<String, Map<String, Object>> map : finalResult.entrySet()) {

                patientData = infoMap.get(map.getKey());
                exp = map.getValue();
                payerCodes = patientData.getPayerCodes();

                this.updatePayerCodes(payerCodes);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

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
            System.out.println("Data has written to score file");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    String getIntegerString(boolean value) {
        if(value) {
            return "1";
        } else {
            return "0";
        }
    }

    String getGenderSymbol(String gender) {
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

    public String getAge(Date birthday, Date date) {

        LocalDate dob = convertToLocalDateViaInstant(birthday);
        LocalDate curDate = convertToLocalDateViaInstant(date);
        Period period = Period.between(dob, curDate);
        System.out.println("Age: "+period.getYears());
        return String.valueOf(period.getYears());
    }
}