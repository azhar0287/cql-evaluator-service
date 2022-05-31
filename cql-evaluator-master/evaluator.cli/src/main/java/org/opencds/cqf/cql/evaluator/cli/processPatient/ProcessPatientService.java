package org.opencds.cqf.cql.evaluator.cli.processPatient;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptionsMapper;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.instance.model.api.IBaseBundle;
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
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.util.Util;
import org.opencds.cqf.cql.evaluator.cql2elm.content.LibraryContentProvider;
import org.opencds.cqf.cql.evaluator.dagger.CqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.dagger.DaggerCqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;

import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProcessPatientService {
    public static Logger LOGGER  = LogManager.getLogger(CqlCommand.class);
    public String fhirVersion;
    public String optionsPath;

    List<LibraryOptions> libraries = new ArrayList<>();


    private Map<String, LibraryContentProvider> libraryContentProviderIndex = new HashMap<>();
    private Map<String, TerminologyProvider> terminologyProviderIndex = new HashMap<>();

    List<RetrieveProvider> mapToRetrieveProvider(int skip, int limit) {
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

    void refreshValueSetBundles(Bundle valueSetBundle , Bundle copySetBundle, List<Bundle.BundleEntryComponent> valueSetEntry  ) {
        copySetBundle = valueSetBundle.copy();
        valueSetEntry = copySetBundle.getEntry();
    }
    
    public Integer call() throws Exception {
        long startTime = System.currentTimeMillis();

        DBConnection db = new DBConnection();
        //int totalCount = db.getDataCount("ep_encounter_fhir");
        int totalCount = 75419;
        LOGGER.info("total Data count: "+totalCount);
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
                retrieveProviders = mapToRetrieveProvider(skip, batchSize);
                processAndSavePatients(retrieveProviders, csvPrinter);
                i+=batchSize-1;
                skip += batchSize;
                entriesProcessed +=batchSize;
            }
            else {
                retrieveProviders.clear();
                retrieveProviders = mapToRetrieveProvider(skip, batchSize);
                processAndSavePatients(retrieveProviders, csvPrinter);
                i+=entriesLeft;
                entriesProcessed+=entriesLeft;
            }

            LOGGER.info("Iteration"+i+" has completed: Skip"+skip+" Limit"+limit);
        }
        long stopTime = System.currentTimeMillis();
        long milliseconds = stopTime - startTime;
        System.out.println( ((milliseconds)/1000) / 60);

        return 0;
    }

    Integer processAndSavePatients(List<RetrieveProvider> retrieveProviders, CSVPrinter csvPrinter) throws InterruptedException, ParseException {

        HashMap<String, PatientData> infoMap = new HashMap<>();
        HashMap<String, Map<String, Object>> finalResult = new HashMap<>();
        int chaipi = 0;
        CqlEvaluator evaluator = null;
        TerminologyProvider backupTerminologyProvider = null;
        LOGGER.info("Patient List Size: "+retrieveProviders.size());
        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
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
        Util util = new Util();
        util.saveScoreFile(finalResult, infoMap, new SimpleDateFormat("yyyy-MM-dd").parse("2022-12-31"), csvPrinter);
        LOGGER.info("Data batch has sent for score sheet generation");
        finalResult.clear();
        return 0;
    }

}
