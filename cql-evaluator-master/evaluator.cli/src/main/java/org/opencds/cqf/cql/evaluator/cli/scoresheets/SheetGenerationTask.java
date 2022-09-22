package org.opencds.cqf.cql.evaluator.cli.scoresheets;

import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration.*;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.EP_CQL_PROCESSED_DATA;

public class SheetGenerationTask {
    public static Logger LOGGER  = LogManager.getLogger(SheetGenerationTask.class);
    SheetGenerationService sheetGenerationService = new SheetGenerationService();
    UtilityFunction utilityFunction;
    DBConnection db;
    DbFunctions dbFunctions;
    CSVPrinter csvPrinter;
    Map<String,String> stringDictionaryMap=new HashMap<>();

    int skip;
    int batchSize = 500;

    public SheetGenerationTask(UtilityFunction utilityFunction, DBConnection db, DbFunctions dbFunctions, int skip, CSVPrinter csvPrinter) {
        this.utilityFunction = utilityFunction;
        this.db = db;
        this.dbFunctions = dbFunctions;
        this.skip = skip;
        this.csvPrinter = csvPrinter;
    }


    public void generateSheetForDMSE() throws IOException, ParseException {
        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;
        documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        DmseScoreSheet dmseScoreSheet=new DmseScoreSheet();
        dmseScoreSheet.generateSheet(documents, measureDate, csvPrinter, db);
//        sheetGenerationService.generateSheetForDMSE(documents, measureDate, csvPrinter, db);
        documents.clear();
    }

    public void generateSheetForDSFE() throws IOException, ParseException {
        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;
        documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        DsfeScoreSheet dsfeScoreSheet=new DsfeScoreSheet();
        dsfeScoreSheet.generateSheet(documents, measureDate, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForCISE() throws IOException, ParseException {
        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;
        documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        CisScoreSheet cisScoreSheet=new CisScoreSheet();
        cisScoreSheet.generateSheet(documents, measureDate, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForASFE() throws IOException {
        List<Document> documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        AsfeScoreSheet asfeScoreSheet=new AsfeScoreSheet();
        asfeScoreSheet.generateSheet(documents, csvPrinter, db);
        documents.clear();
    }

    public void generateSheetForDRRE() throws IOException, ParseException {
        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;
        documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        DrreScoreSheet drreScoreSheet=new DrreScoreSheet();
        drreScoreSheet.generateSheet(documents, measureDate, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForAPME() throws IOException, ParseException {
        List<Document> documents;
        documents = dbFunctions.getSortedConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        ApmeScoreSheet apmeScoreSheet=new ApmeScoreSheet();
        apmeScoreSheet.generateSheet(documents, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForAPM() throws IOException, ParseException {
        List<Document> documents;
        documents = dbFunctions.getSortedConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        ApmScoreSheet apmScoreSheet=new ApmScoreSheet();
        apmScoreSheet.generateSheet(documents, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForUOP() throws IOException, ParseException {
        List<Document> documents;
        documents = dbFunctions.getSortedConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        UopScoreSheet uopScoreSheet=new UopScoreSheet();
        uopScoreSheet.generateSheet(documents, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForFUM() throws IOException, ParseException {
        List<Document> documents;
        documents = dbFunctions.getSortedConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        FumScoreSheet fumScoreSheet=new FumScoreSheet();
        fumScoreSheet.generateSheet(documents, csvPrinter, db,stringDictionaryMap);
        documents.clear();
    }

    public void generateSheetForCCS() throws IOException, ParseException {
        Date measureDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31");
        List<Document> documents;
        documents = dbFunctions.getConditionalData(EP_CQL_PROCESSED_DATA, skip, batchSize, db);
        sheetGenerationService.generateSheetForCCS(documents, measureDate, csvPrinter, db);
        documents.clear();
    }



}
