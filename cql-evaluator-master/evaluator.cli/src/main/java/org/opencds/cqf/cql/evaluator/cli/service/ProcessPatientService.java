package org.opencds.cqf.cql.evaluator.cli.service;

import ca.uhn.fhir.context.FhirVersionEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptionsMapper;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.r4.model.Bundle;
import org.opencds.cqf.cql.engine.execution.EvaluationResult;
import org.opencds.cqf.cql.engine.model.ModelResolver;
import org.opencds.cqf.cql.engine.retrieve.RetrieveProvider;
import org.opencds.cqf.cql.engine.terminology.TerminologyProvider;
import org.opencds.cqf.cql.evaluator.CqlEvaluator;
import org.opencds.cqf.cql.evaluator.builder.Constants;
import org.opencds.cqf.cql.evaluator.builder.CqlEvaluatorBuilder;
import org.opencds.cqf.cql.evaluator.builder.DataProviderFactory;
import org.opencds.cqf.cql.evaluator.builder.EndpointInfo;
import org.opencds.cqf.cql.evaluator.cli.command.CqlCommand;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ContextParameter;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.ModelParameter;
import org.opencds.cqf.cql.evaluator.cli.mappers.SheetInputMapper;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import org.opencds.cqf.cql.evaluator.cql2elm.content.LibraryContentProvider;
import org.opencds.cqf.cql.evaluator.dagger.CqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.dagger.DaggerCqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;
import java.io.FileWriter;
import java.text.ParseException;
import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class ProcessPatientService {
    public static Logger LOGGER  = LogManager.getLogger(CqlCommand.class);

    public String optionsPath;

    public List<LibraryOptions> libraries = new ArrayList<>();

    public Map<String, LibraryContentProvider> libraryContentProviderIndex = new HashMap<>();
    public Map<String, TerminologyProvider> terminologyProviderIndex = new HashMap<>();
    UtilityFunction utilityFunction = new UtilityFunction();

    public LibraryOptions setupLibrary() {
        ContextParameter context = new ContextParameter(CONTEXT, "TEST");
        ModelParameter modelParameter = new ModelParameter(MODEL, MODEL_URL);
        LibraryOptions libraryOptions = new LibraryOptions (FHIR_VERSION, LIBRARY_URL, LIBRARY_NAME, FHIR_VERSION, TERMINOLOGY, context, modelParameter);
        return libraryOptions;
    }

    public void refreshValueSetBundles(Bundle valueSetBundle , Bundle copySetBundle, List<Bundle.BundleEntryComponent> valueSetEntry ) {
        copySetBundle = valueSetBundle.copy();
        valueSetEntry = copySetBundle.getEntry();
    }

    public void dataBatchingAndProcessing() throws Exception {
        long startTime = System.currentTimeMillis();
        List<SheetInputMapper> sheetInputMapper = new ArrayList<>();
        DBConnection db = new DBConnection();
        //int totalCount = db.getDataCount("ep_encounter_fhir");
        int totalCount = 5000;
        int skip = 0;
        int limit = 200;
        List<RetrieveProvider> retrieveProviders = new ArrayList<>();
        String SAMPLE_CSV_FILE = "C:\\Projects\\cql-evaluator-service\\cql-evaluator-master\\evaluator.cli\\src\\main\\resources\\sample.csv";
        String[] header = { "MemId", "Meas", "Payer","CE","Event","Epop","Excl","Num","RExcl","RExclD","Age","Gender"};
        FileWriter writer = new FileWriter(SAMPLE_CSV_FILE, true);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header));

        int batchSize = 200;
        int entriesLeft = 0;
        int entriesProcessed = 0;

        for(int i=0; i<totalCount; i++) {
            entriesLeft = totalCount - entriesProcessed;
            if(entriesLeft >= batchSize) {
                retrieveProviders.clear();
                retrieveProviders = utilityFunction.mapToRetrieveProvider(skip, batchSize, libraries.get(0).fhirVersion, libraries);
                sheetInputMapper.add(processAndSavePatients(retrieveProviders, csvPrinter));
                i+=batchSize-1;
                skip += batchSize;
                entriesProcessed +=batchSize;
            }
            else {
                retrieveProviders.clear();
                retrieveProviders = utilityFunction.mapToRetrieveProvider(skip, batchSize, libraries.get(0).fhirVersion, libraries);
                sheetInputMapper.add(processAndSavePatients(retrieveProviders, csvPrinter));
                i+=entriesLeft;
                entriesProcessed+=entriesLeft;
            }
            LOGGER.info("Iteration"+i+" has completed: Skip"+skip+" Limit"+batchSize);
        }
        long stopTime = System.currentTimeMillis();
        long milliseconds = stopTime - startTime;
        System.out.println( ((milliseconds)/1000) / 60);
    }

    SheetInputMapper processAndSavePatients(List<RetrieveProvider> retrieveProviders, CSVPrinter csvPrinter) throws InterruptedException, ParseException {

        HashMap<String, PatientData> infoMap = new HashMap<>();
        HashMap<String, Map<String, Object>> finalResult = new HashMap<>();
        int chaipi = 0;
        CqlEvaluator evaluator = null;
        TerminologyProvider backupTerminologyProvider = null;
        LOGGER.info("Patient List Size: "+retrieveProviders.size());
        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(libraries.get(0).fhirVersion);
        CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder()
                .fhirContext(fhirVersionEnum.newContext()).build();

        CqlTranslatorOptions options = null;
        if (optionsPath != null) {
            options = CqlTranslatorOptionsMapper.fromFile(optionsPath);
        }

        for (LibraryOptions library : libraries) {
            CqlEvaluatorBuilder cqlEvaluatorBuilder = cqlEvaluatorComponent.createBuilder();

            if (options != null) {
                cqlEvaluatorBuilder.withCqlTranslatorOptions(options);
            }

            LibraryContentProvider libraryContentProvider = libraryContentProviderIndex.get(library.libraryUrl);

            if (libraryContentProvider == null) {
                libraryContentProvider = cqlEvaluatorComponent.createLibraryContentProviderFactory()
                        .create(new EndpointInfo().setAddress(library.libraryUrl));
                this.libraryContentProviderIndex.put(library.libraryUrl, libraryContentProvider);
            }

            cqlEvaluatorBuilder.withLibraryContentProvider(libraryContentProvider);

            if (library.terminologyUrl != null) {
                TerminologyProvider terminologyProvider = this.terminologyProviderIndex.get(library.terminologyUrl);
                if (terminologyProvider == null) {
                    terminologyProvider = cqlEvaluatorComponent.createTerminologyProviderFactory()
                            .create(new EndpointInfo().setAddress(library.terminologyUrl));
                    this.terminologyProviderIndex.put(library.terminologyUrl, terminologyProvider);
                }
                cqlEvaluatorBuilder.withTerminologyProvider(terminologyProvider);
                backupTerminologyProvider = terminologyProvider;
            }

            Triple<String, ModelResolver, RetrieveProvider> dataProvider = null;
            DataProviderFactory dataProviderFactory = cqlEvaluatorComponent.createDataProviderFactory();
            if (library.model != null) {
                dataProvider = dataProviderFactory.create(new EndpointInfo().setAddress(library.model.modelUrl));
            }
            // default to FHIR
            else {
                dataProvider = dataProviderFactory.create(new EndpointInfo().setType(Constants.HL7_FHIR_FILES_CODE));
            }

            //Changes need to be made here
            RetrieveProvider bundleRetrieveProvider =  dataProvider.getRight(); // here value sets are added

            List<Bundle.BundleEntryComponent> valueSetEntry = null, valueSetEntryTemp = null;
            Bundle valueSetBundle = null;
            Bundle copySetBundle = null;
            if(bundleRetrieveProvider instanceof BundleRetrieveProvider) {
                //having value sets entries
                BundleRetrieveProvider bundleRetrieveProvider1 = (BundleRetrieveProvider) bundleRetrieveProvider;

                if(bundleRetrieveProvider1.bundle instanceof Bundle) {
                    valueSetBundle = (Bundle)bundleRetrieveProvider1.bundle;
                    copySetBundle = valueSetBundle.copy();
                    valueSetEntry = copySetBundle.getEntry();
                }
            }

            //having Patient data entries
            for(RetrieveProvider retrieveProvider : retrieveProviders) {
                PatientData patientData;
                library.context.contextValue = ((BundleRetrieveProvider) retrieveProvider).getPatientData().getId();
                String patientId = ((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString();
                LOGGER.info("Patient Id in Loop "+patientId);
                refreshValueSetBundles(valueSetBundle, copySetBundle, valueSetEntry);
                valueSetEntry = valueSetBundle.copy().getEntry();
                valueSetEntryTemp = valueSetEntry; //tem having value set entries

                if(retrieveProvider instanceof BundleRetrieveProvider) {

                    BundleRetrieveProvider retrieveProvider1 = (BundleRetrieveProvider) retrieveProvider;
                    if(retrieveProvider1.bundle instanceof Bundle) {
                        Bundle patientDataBundle = (Bundle)retrieveProvider1.bundle;
                        valueSetEntryTemp.addAll(patientDataBundle.getEntry()); //adding value sets + patient entries

                        RetrieveProvider finalPatientData;

                        Bundle bundle1 = new Bundle();
                        for (Bundle.BundleEntryComponent bundle :valueSetEntryTemp) {
                            bundle1.addEntry(bundle);  //value set EntryTemp to new Bundle
                        }

                        finalPatientData = new BundleRetrieveProvider(fhirVersionEnum.newContext(), bundle1);

                        //Processing
                        cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getLeft(), dataProvider.getMiddle(), finalPatientData);


                        if(chaipi == 0) {
                            evaluator = cqlEvaluatorBuilder.build();
                        }

                        if(finalPatientData instanceof BundleRetrieveProvider) {
                            BundleRetrieveProvider bundleRetrieveProvider1 = (BundleRetrieveProvider) finalPatientData;
                            bundleRetrieveProvider1.setTerminologyProvider(backupTerminologyProvider);
                            bundleRetrieveProvider1.setExpandValueSets(true);
                        }

                        chaipi++;
                        VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);
                        Pair<String, Object> contextParameter = null;


                        library.context.contextValue = patientId;
                        if (library.context != null) {
                            contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
                        }

                        EvaluationResult result = evaluator.evaluate(identifier, contextParameter);
                        System.out.println("Evaluation has done: "+chaipi);

                        patientData = ((BundleRetrieveProvider) retrieveProvider).getPatientData();
                        infoMap.put(patientId, patientData);

                        finalResult.put(patientId, result.expressionResults);
                        LOGGER.info("Patient processed: "+patientId);
                        if(chaipi %15 == 0) {
                            Thread.sleep(1000);
                        }
                    }
                }
            }
        }
        return new SheetInputMapper(infoMap, finalResult);
    }
}
