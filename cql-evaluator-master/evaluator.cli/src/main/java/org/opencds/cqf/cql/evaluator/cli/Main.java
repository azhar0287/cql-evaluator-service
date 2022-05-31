package org.opencds.cqf.cql.evaluator.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencds.cqf.cql.evaluator.cli.command.CliCommand;

import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.service.ProcessPatientService;
import picocli.CommandLine;

public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    private static ByteArrayOutputStream outContent;
    private static ByteArrayOutputStream errContent;
    private static final PrintStream originalOut = System.out;
    private static final PrintStream originalErr = System.err;
    private static final String testResourceRelativePath = "evaluator.cli/src/main/resources";
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


    public static void main(String[] args) throws Exception {
        LOGGER.info("Processing start");
        //Main.CSS_HEDIS_MY2022();

        ProcessPatientService processPatientService = new ProcessPatientService();
        LibraryOptions libraryOptions = processPatientService.setupLibrary();
        processPatientService.libraries.add(libraryOptions);
        processPatientService.dataBatchingAndProcessing();

        int exitCode = run(args);
        System.exit(exitCode);
    }

    public static int run(String[] args) {
        Objects.requireNonNull(args);
        CommandLine cli = new CommandLine(new CliCommand());
        return cli.execute(args);
    }

    public static void CSS_HEDIS_MY2022(){
        setUpStreams();
        /*String folderName="/CCS_HEDIS_MY2022";
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
                "-cv=186905"
        };

        Main.run(args);
        */
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