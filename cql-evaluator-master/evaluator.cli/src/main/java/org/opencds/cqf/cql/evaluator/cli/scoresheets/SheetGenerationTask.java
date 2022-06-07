package org.opencds.cqf.cql.evaluator.cli.scoresheets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class SheetGenerationTask implements Runnable {
    public static Logger LOGGER  = LogManager.getLogger(SheetGenerationTask.class);

    UtilityFunction utilityFunction = new UtilityFunction();
    DBConnection db;
    DbFunctions dbFunctions;
    CSVPrinter csvPrinter;

    int skip = 0;
    int batchSize = 500;

    public SheetGenerationTask(UtilityFunction utilityFunction, DBConnection db, DbFunctions dbFunctions, int skip, CSVPrinter csvPrinter) {
        this.utilityFunction = utilityFunction;
        this.db = db;
        this.dbFunctions = dbFunctions;
        this.skip = skip;
        this.csvPrinter = csvPrinter;
    }

    @Override
    public void run() {
        LOGGER.info("Thread is processing for sheet"+skip);
        try {
            this.generateSheetCCS();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void generateSheetCCS() throws IOException, ParseException {
        int totalCount = dbFunctions.getDataCount("ep_cql_processed_data", db);

        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;


        documents = dbFunctions.getConditionalData("NoId", "ep_cql_processed_data", skip, batchSize, db);
        generateSheet(documents, measureDate, csvPrinter, db);
        documents.clear();
        documents = null;
    }

    public void generateSheet(List<Document> documents, Date measureDate, CSVPrinter csvPrinter, DBConnection db) throws IOException {
        try {
            List<String> sheetObj;
            List<String> payerCodes;
            List<String> codeCheckList = utilityFunction.checkCodeForCCS();
            for(Document document : documents) {
//                System.out.println("Processing patient: "+document.getString("id"));
                if(document.getBoolean("Age and Gender")) {
                    Object object = document.get("payerCodes");
                    payerCodes = new ObjectMapper().convertValue(object, new TypeReference<List<String>>() {});
                    utilityFunction.updatePayerCodes(payerCodes, dbFunctions, db);  //update payer codes for Commercial/Medicaid and Commercial/Medicare conditions

                    for(int i=0; i< payerCodes.size(); i++) {
                        sheetObj = new ArrayList<>();
                        sheetObj.add(document.getString("id"));
                        sheetObj.add("CCS"); //Measure
                        String payerCode = payerCodes.get(i);
                        sheetObj.add(String.valueOf(payerCode));
                        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Enrolled During Participation Period For CE")));
                        sheetObj.add("0"); //event
                        if(document.getBoolean("Exclusions") || codeCheckList.stream().anyMatch(str -> str.trim().equals(payerCode))) {
                            sheetObj.add("0"); //Epop
                        }
                        else {
                            sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator"))); //Epop
                        }
                        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Denominator Exceptions"))); //exc
                        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Numerator")));
                        sheetObj.add("0"); //Rexl
                        sheetObj.add(utilityFunction.getIntegerString(document.getBoolean("Exclusions"))); //RexclId
                        sheetObj.add(utilityFunction.getAge(document.getDate("birthDate"), measureDate));
                        sheetObj.add(utilityFunction.getGenderSymbol(document.getString("gender")));
                        csvPrinter.printRecord(sheetObj);

                    }
                }
                csvPrinter.flush();
                }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
