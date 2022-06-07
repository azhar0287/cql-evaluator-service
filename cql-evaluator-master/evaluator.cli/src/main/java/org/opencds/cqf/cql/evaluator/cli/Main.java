package org.opencds.cqf.cql.evaluator.cli;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencds.cqf.cql.evaluator.cli.command.CliCommand;

import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ContextParameter;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ModelParameter;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.SheetGenerationTask;
import org.opencds.cqf.cql.evaluator.cli.service.ProcessPatientService;
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

    private static final String testResourceRelativePath = "evaluator.cli/src/main/resources"; //for Jar
    //private static final String testResourceRelativePath = "evaluator.cli/src/main/resources";
    private static String testResourcePath = null;

    static {
        File file = new File(testResourceRelativePath);
        testResourcePath = file.getAbsolutePath();
        System.out.println(String.format("Test resource directory: %s", testResourcePath));
    }

    public static void setUpStreams() {
        outContent = new ByteArrayOutputStream();
        errContent = new ByteArrayOutputStream();

        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    public static void restoreStreams() {
        String sysOut = outContent.toString();
        String sysError = errContent.toString();

        System.setOut(originalOut);
        System.setErr(originalErr);

        System.out.println(sysOut);
        System.err.println(sysError);
    }

    public static LibraryOptions setupLibrary() {
        ContextParameter context = new ContextParameter(CONTEXT, "TEST");
        ModelParameter modelParameter = new ModelParameter(MODEL, MODEL_URL);
        LibraryOptions libraryOptions = new LibraryOptions (FHIR_VERSION, LIBRARY_URL, LIBRARY_NAME, FHIR_VERSION, TERMINOLOGY, context, modelParameter);
        return libraryOptions;
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Processing start");
        DBConnection connection = new DBConnection(); //setting up connection
        DbFunctions dbFunctions = new DbFunctions();
        UtilityFunction utilityFunction = new UtilityFunction();


        int totalCount = dbFunctions.getDataCount("ep_encounter_fhir_AllData", connection);
        int totalSkips = (int) Math.floor(totalCount/10);
        totalSkips+=1; //for remaining records
        // Setting up cql and helper libraries
        List<LibraryOptions> libraryOptions = new ArrayList<>();
        libraryOptions.add(setupLibrary());

        //Patient processing
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i=0; i<totalSkips; i++) {
            executorService.execute(new ProcessPatientService(i, libraryOptions, connection, totalCount));
        }
        System.out.println("Patients Processing has completed");

        /*
        //Sheet Generation
        int totalDataCountForSheet = dbFunctions.getDataCount("ep_cql_processed_data", connection);
        int totalSkipsForSheet = (int) Math.ceil(totalDataCountForSheet/500) ;
        totalSkipsForSheet+=1; //for remaining records

        CSVPrinter csvPrinter = utilityFunction.setupSheetHeaders();
        ExecutorService executorServiceForSheet = Executors.newFixedThreadPool(10);
        for(int i=0; i<totalSkipsForSheet; i++) {
            executorServiceForSheet.execute(new SheetGenerationTask(utilityFunction, connection, dbFunctions, i, csvPrinter));
            System.out.println("Iteration: "+i);
        }
        System.out.println("Sheet generation has completed");
        */
    }

    public static void sheetGeneration() throws IOException, ParseException {
        UtilityFunction utilityFunction = new UtilityFunction();
        CSVPrinter csvPrinter = utilityFunction.setupSheetHeaders();

        LOGGER.info("Sheet Generation has started: ");

       // SheetGenerationTask sheetGenerationService = new SheetGenerationTask();
       // sheetGenerationService.generateSheetCCS();

        LOGGER.info("Sheet generation has completed");
    }

    public static int run(String[] args) {
        Objects.requireNonNull(args);
        CommandLine cli = new CommandLine(new CliCommand());
        return cli.execute(args);
    }

    public static void CSS_HEDIS_MY2022(){
        setUpStreams();
        String folderName="/CCS_HEDIS_MY2022";
        String mainLibrary="CCS_HEDIS_MY2022";
        String[] args = new String[]{
                "cql",
                "-fv=R4",
                "-lu="+ testResourcePath + folderName,
                "-ln="+mainLibrary,
                "-m=FHIR",
                "-mu=" + testResourcePath + folderName,
                "-t=" + testResourcePath + folderName+"/vocabulary/ValueSet",
                "-c=Patient",
                "-cv=182669"
        };

        Main.run(args);

        String output = outContent.toString();
        System.out.println("Test here");
        restoreStreams();
    }
    public static void testingAISE_Hedis_My2022(){
        setUpStreams();
        String folderName="/AISE_HEDIS_MY2022";
        String mainLibrary="AISE_HEDIS_MY2022";
        String[] args = new String[]{
                "cql",
                "-fv=R4",
                "-lu="+ testResourcePath + folderName,
                "-ln="+mainLibrary,
                "-m=FHIR",
                "-mu=" + testResourcePath + folderName,
                "-t=" + testResourcePath + folderName+"/vocabulary/ValueSet",
                "-c=Patient",
                "-cv=185233"
        };

        Main.run(args);

        String output = outContent.toString();
        System.out.println("Test here");
        restoreStreams();
    }

    public static void BCSE_HEDIS_MY2022(){
        setUpStreams();
        String folderName="/HedisMeasureTesting";
        String mainLibrary="BCSE_HEDIS_MY2022";
        String[] args = new String[]{
                "cql",
                "-fv=R4",
                "-lu="+ testResourcePath + folderName,
                "-ln="+mainLibrary,
                "-m=FHIR",
                "-mu=" + testResourcePath + folderName,
                "-t=" + testResourcePath + folderName+"/vocabulary/ValueSet",
                "-c=Patient",
                "-cv=Patient-18"
        };

        Main.run(args);

        String output = outContent.toString();
        System.out.println("Test here");
        restoreStreams();
    }
}