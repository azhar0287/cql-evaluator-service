package org.opencds.cqf.cql.evaluator.cli;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.client.model.Indexes;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.opencds.cqf.cql.evaluator.cli.command.CliCommand;

import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ContextParameter;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ModelParameter;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.SheetGenerationTask;
import org.opencds.cqf.cql.evaluator.cli.service.ProcessPatientService;
import org.opencds.cqf.cql.evaluator.cli.util.Constant;
import org.opencds.cqf.cql.evaluator.cli.util.ThreadTaskCompleted;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import picocli.CommandLine;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;
import static org.opencds.cqf.cql.evaluator.cli.util.Constant.TERMINOLOGY;

public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    private static ByteArrayOutputStream outContent;
    private static ByteArrayOutputStream errContent;
    private static final PrintStream originalOut = System.out;
    private static final PrintStream originalErr = System.err;

    //    private static final String testResourceRelativePath = "evaluator.cli/src/main/resources"; //for Jar
    //  private static final String testResourceRelativePath = "evaluator.cli/src/main/resources";
    private static String testResourcePath = null;
    public static List<String> failedPatients = new ArrayList<>();

    static {
        File file = new File(testResourceRelativePath);
        testResourcePath = file.getAbsolutePath();
        System.out.println(String.format("Test resource directory: %s", testResourcePath));
    }


    public static LibraryOptions setupLibrary() {
        ContextParameter context = new ContextParameter(CONTEXT, "TEST");
        ModelParameter modelParameter = new ModelParameter(MODEL, MODEL_URL);
        LibraryOptions libraryOptions = new LibraryOptions (FHIR_VERSION, LIBRARY_URL, LIBRARY_NAME, FHIR_VERSION, TERMINOLOGY, context, modelParameter);
        return libraryOptions;
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Processing start");
        DBConnection connection = DBConnection.getConnection(); //setting up connection
        DbFunctions dbFunctions = new DbFunctions();

        connection.collection = connection.database.getCollection("ep_cql_processed_data");
        connection.collection.createIndex(Indexes.ascending("id"));

//        To Process Patien/ts
        processPatients(dbFunctions, connection);
        insertFailedPatient(dbFunctions, connection,"ep_cql_Pnde_Sample_Sheet_failed_patients");

//        //Process Single Patient
//        String patientId = "95759";
//        processSinglePatient(patientId, dbFunctions, connection);
          ////////////////

        //To generate Sheet and failed patients
//        generateSheet(dbFunctions, connection, new UtilityFunction());
//        insertFailedPatient(dbFunctions, connection,"ep_cql_Pnde_Sample_Sheet_failed_patients");



        //This function is not in our use now, can be refactored later
        //To Process Patients
        //processRemainingPatients(dbFunctions, connection);

    }

    public static void processSinglePatient(String patientId, DbFunctions dbFunctions, DBConnection connection) {
        List<LibraryOptions> libraryOptions = new ArrayList<>();
        libraryOptions.add(setupLibrary());

        ThreadTaskCompleted isTaskCompleted = new ThreadTaskCompleted();
        ProcessPatientService processPatientService = new ProcessPatientService(0, libraryOptions, connection, 1, isTaskCompleted);
        processPatientService.singleDataProcessing(patientId);

        System.out.println("Finished");
    }

//        public static void processPatients(DbFunctions dbFunctions, DBConnection connection) {
//
//        System.out.println("Patient processing has started");
//
//        List<ThreadTaskCompleted> isAllTasksCompleted = new LinkedList<>();
//        int totalCount = dbFunctions.getDataCount(Constant.MAIN_FHIR_COLLECTION_NAME, connection);
//
//        int totalSkips = (int) Math.ceil(totalCount/10);
//        int totalSkipped = 0;
//        if(totalCount % 10 > 0) { //remaining missing records issue fix
//            totalSkips+=1;
//        }
//
//        List<LibraryOptions> libraryOptions = new ArrayList<>();
//        libraryOptions.add(setupLibrary());
//
//        //Patient processing
//        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        for(int i=0; i < totalSkips; i++) {
//            //connection = new DBConnection();
//            ThreadTaskCompleted isTaskCompleted = new ThreadTaskCompleted();
//            isAllTasksCompleted.add(isTaskCompleted);
//            executorService.submit(new ProcessPatientService(totalSkipped, libraryOptions, connection, totalCount, isTaskCompleted));
//            totalSkipped += 10;
//            //connection.closeConnection();
//
//        }
//
//        /*Shutting Down service*/
//        while(true) {
//            if(dbFunctions.isAllTasksCompletedByThreads(isAllTasksCompleted)){
//                LOGGER.info("****** Patients are processed");
//                break;
//            }
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        System.out.println("Patients Processing has completed");
//        executorService.shutdown();
//    }
    public static void processPatients(DbFunctions dbFunctions, DBConnection connection) {

        System.out.println("Patient processing has started");

        List<ThreadTaskCompleted> isAllTasksCompleted = new LinkedList<>();
        int totalCount = dbFunctions.getDataCount(Constant.MAIN_FHIR_COLLECTION_NAME, connection);

        int totalSkips = (int) Math.ceil(totalCount/10);
        int totalSkipped = 0;
        if(totalCount % 10 > 0) { //remaining missing records issue fix
            totalSkips+=1;
        }

        List<LibraryOptions> libraryOptions = new ArrayList<>();
        libraryOptions.add(setupLibrary());

        //Patient processing
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i=0; i < totalSkips; i++) {
            ////////////////////////////////////////////////////////////////////////for multithread
            ThreadTaskCompleted isTaskCompleted = new ThreadTaskCompleted();
            isAllTasksCompleted.add(isTaskCompleted);
            executorService.submit(new ProcessPatientService(totalSkipped, libraryOptions, connection, totalCount, isTaskCompleted));
            ////////////////////////////////////////////////////////////////////////for multithread
//            ProcessPatientService processPatientService = new ProcessPatientService(totalSkipped, libraryOptions, connection, totalCount);
//            processPatientService.dataBatchingAndProcessing();
            totalSkipped += 10;
        }

        /*Shutting Down service*/
        ////////////////////////////////////////////////////////////////////////for multithread
        while(true) {
            if(dbFunctions.isAllTasksCompletedByThreads(isAllTasksCompleted)){
                LOGGER.info("****** Patients are processed");
                break;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ////////////////////////////////////////////////////////////////////////for multithread
        System.out.println("Patients Processing has completed");
        executorService.shutdown();
    }

    public static void processRemainingPatients(DbFunctions dbFunctions, DBConnection connection) {

        System.out.println("Patient processing has started");

        int totalCount = dbFunctions.getDataCount(FHIR_UNPROCESSED_COLLECTION_NAME, connection);
        if(totalCount>0) {
            List<LibraryOptions> libraryOptions = new ArrayList<>();
            libraryOptions.add(setupLibrary());

            ProcessPatientService processPatientService = new ProcessPatientService(libraryOptions, connection, totalCount);
            processPatientService.processRemainingPatients();
        }
    }

    public static void generateSheet(DbFunctions dbFunctions, DBConnection connection, UtilityFunction utilityFunction) throws IOException, ParseException {
        System.out.println("Sheet generation has started");
        List<ThreadTaskCompleted> isAllTasksCompleted=new LinkedList<>();
        int totalDataCountForSheet = dbFunctions.getDataCount("ep_cql_processed_data", connection);
        int totalSkipped = 0;
        int totalSkipsForSheet = (int) Math.ceil(totalDataCountForSheet/500) ;
        if(totalDataCountForSheet % 500 > 0) { //remaining missing records issue fix
            totalSkipsForSheet+=1; //for remaining records
        }
        CSVPrinter csvPrinter = utilityFunction.setupSheetHeaders();
        ExecutorService executorServiceForSheet = Executors.newFixedThreadPool(10);

        for(int i=0; i<totalSkipsForSheet; i++) {
            SheetGenerationTask sheetGenerationTask = new SheetGenerationTask(utilityFunction, connection, dbFunctions, totalSkipped, csvPrinter);
            sheetGenerationTask.generateSheetForPrse();
            System.out.println("Iteration: "+i);
            totalSkipped+=500;
        }

        /*Shutting Down service*/
        while(true) {
            if(dbFunctions.isAllTasksCompletedByThreads(isAllTasksCompleted)){
                LOGGER.info("****** Patients are processed");
                break;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executorServiceForSheet.shutdown();
        System.out.println("Sheet generation has completed");
    }

    public static void insertFailedPatient(DbFunctions dbFunctions, DBConnection dbConnection,String collectionName) {
        List<Document>documents = new ArrayList<>();
        for (String failedPatient : failedPatients) {
            Document document = new Document();
            document.put("id", failedPatient);
            documents.add(document);
        }
        dbFunctions.insertFailedPatients(collectionName, documents, dbConnection);
    }

//    public static int run(String[] args) {
//        Objects.requireNonNull(args);
//        CommandLine cli = new CommandLine(new CliCommand());
//        return cli.execute(args);
//    }

}