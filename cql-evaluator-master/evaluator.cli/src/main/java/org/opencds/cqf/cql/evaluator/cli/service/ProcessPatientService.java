package org.opencds.cqf.cql.evaluator.cli.service;

import ca.uhn.fhir.context.FhirVersionEnum;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
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
import org.opencds.cqf.cql.evaluator.cli.Main;
import org.opencds.cqf.cql.evaluator.cli.command.CqlCommand;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cli.db.DbFunctions;
import org.opencds.cqf.cql.evaluator.cli.libraryparameter.LibraryOptions;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration.PdseScoreSheet;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration.PndeScoreSheet;
import org.opencds.cqf.cql.evaluator.cli.scoresheets.MeasureWiseSheetGeneration.PrseScoreSheet;
import org.opencds.cqf.cql.evaluator.cli.util.Constant;
import org.opencds.cqf.cql.evaluator.cli.util.ThreadTaskCompleted;
import org.opencds.cqf.cql.evaluator.cli.util.UtilityFunction;
import org.opencds.cqf.cql.evaluator.cql2elm.content.LibraryContentProvider;
import org.opencds.cqf.cql.evaluator.dagger.CqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.dagger.DaggerCqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;
import org.opencds.cqf.cql.evaluator.engine.retrieve.PayerInfo;

import java.util.*;

import static org.opencds.cqf.cql.evaluator.cli.util.Constant.*;

public class ProcessPatientService implements Runnable {

    public static Logger LOGGER  = LogManager.getLogger(CqlCommand.class);
    public static int processCounter = 0;
    public String optionsPath;
    public List<LibraryOptions> libraries;
    public Map<String, LibraryContentProvider> libraryContentProviderIndex = new HashMap<>();
    public Map<String, TerminologyProvider> terminologyProviderIndex = new HashMap<>();
    UtilityFunction utilityFunction = new UtilityFunction();
    DBConnection dbConnection;
    DbFunctions dbFunctions = new DbFunctions();
    int skip = 0;
    int batchSize = 10;
    int totalCount;
    ThreadTaskCompleted threadTaskCompleted;

    public ProcessPatientService() {

    }

    public ProcessPatientService(int skip, List<LibraryOptions> libraries, DBConnection connection, int totalCount, ThreadTaskCompleted threadTaskCompleted) {
        this.libraries = libraries;
        this.skip = skip;
        this.dbConnection = connection;
        this.totalCount = totalCount;
        this.threadTaskCompleted = threadTaskCompleted;
    }
    public ProcessPatientService(int skip, List<LibraryOptions> libraries, DBConnection connection, int totalCount) {
        this.libraries = libraries;
        this.skip = skip;
        this.dbConnection = connection;
        this.totalCount = totalCount;
    }
    public ProcessPatientService( List<LibraryOptions> libraries, DBConnection connection, int totalCount) {
        this.libraries = libraries;
        this.dbConnection = connection;
        this.totalCount = totalCount;
        this.batchSize=totalCount;
    }

    public void refreshValueSetBundles(Bundle valueSetBundle , Bundle copySetBundle, List<Bundle.BundleEntryComponent> valueSetEntry ) {
        copySetBundle = valueSetBundle.copy();
        valueSetEntry = copySetBundle.getEntry();
    }


    public void singleDataProcessing(String patientId) {
        List<RetrieveProvider> retrieveProviders;
        retrieveProviders = utilityFunction.mapToRetrieveProviderForSingle(patientId, skip, 1, libraries.get(0).fhirVersion, libraries, dbFunctions, dbConnection, Constant.MAIN_FHIR_COLLECTION_NAME);
        processAndSavePatients(retrieveProviders, dbFunctions);
//        threadTaskCompleted.isTaskCompleted = true;

    }

    public void processRemainingPatients(){
        List<RetrieveProvider> retrieveProviders;
        retrieveProviders = utilityFunction.mapToRetrieveProvider(skip, batchSize, libraries.get(0).fhirVersion, libraries, dbFunctions, dbConnection, FHIR_UNPROCESSED_COLLECTION_NAME);
        List<Document> processedPatients= processAndSaveUnprocessedPatients(retrieveProviders, dbFunctions);
        int a=0;

    }

    public void dataBatchingAndProcessing() {
        List<RetrieveProvider> retrieveProviders;
        retrieveProviders = utilityFunction.mapToRetrieveProvider(skip, batchSize, libraries.get(0).fhirVersion, libraries, dbFunctions, dbConnection,Constant.MAIN_FHIR_COLLECTION_NAME);
        processAndSavePatients(retrieveProviders, dbFunctions);
//        threadTaskCompleted.isTaskCompleted = true;
    }

    List<Document> processAndSavePatients(List<RetrieveProvider> retrieveProviders, DbFunctions dbFunctions) {
        List<Document> documents = new ArrayList<>();
        String globalPatientId = "";
        try {
            int chaipi = 0;
            CqlEvaluator evaluator = null;
            TerminologyProvider backupTerminologyProvider = null;
            LOGGER.info("Patient List Size: "+retrieveProviders.size());
            FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(libraries.get(0).fhirVersion);
            CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder().fhirContext(fhirVersionEnum.newContext()).build();

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
                    String patientId = "";
                    try{


                        //for(int i=0; i<retrieveProviders.size(); i++) {

                        org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData patientData;
                        library.context.contextValue = ((BundleRetrieveProvider) retrieveProvider).getPatientData().getId();
                         patientId = ((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString();
//                    LOGGER.info("Patient Id in Loop "+patientId);
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
                                processCounter++;
                                VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);
                                Pair<String, Object> contextParameter = null;


                                library.context.contextValue = patientId;
                                if (library.context != null) {
                                    contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
                                }
                                //                  LOGGER.info("Patient being processed in engine: "+patientId);
                                System.out.println("Patient being processed in engine: "+patientId);
                                globalPatientId = library.context.contextValue;
                                EvaluationResult result = evaluator.evaluate(identifier, contextParameter);

                                patientData = ((BundleRetrieveProvider) retrieveProvider).getPatientData();
                                documents.add(PrseScoreSheet.createDocumentForPrseResult(result.expressionResults, patientData));
                                if(documents.size() > 15) {
                                    dbFunctions.insertProcessedDataInDb("ep_cql_processed_data", documents, dbConnection);
                                    System.out.println("Going to add 15 patients in db, and Thread is going to sleep");
                                    Thread.sleep(100);
                                    documents.clear();
                                }
                            }
                        }

                    }
                    catch(Exception e){
                        LOGGER.error(e.getMessage()+" PatientId: "+globalPatientId, e);
                        Main.failedPatients.add(patientId);
                    }
                }
                retrieveProviders.clear();
                retrieveProviders = null;
                if(documents.size() > 0 ) {
                    dbFunctions.insertProcessedDataInDb("ep_cql_processed_data", documents, dbConnection);
                    documents.clear();
                }
                documents = null;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage()+" PatientId: "+globalPatientId, e);

            Main.failedPatients.add(globalPatientId);

        }
        return documents;
    }


    List<Document> processAndSaveUnprocessedPatients(List<RetrieveProvider> retrieveProviders, DbFunctions dbFunctions) {
        List<Document> documents = new ArrayList<>();
        String globalPatientId = "";
        int count=0;
//        while(count<totalCount) {
        try {
            int chaipi = 0;
            CqlEvaluator evaluator = null;
            TerminologyProvider backupTerminologyProvider = null;
            LOGGER.info("Patient List Size: " + retrieveProviders.size());
            FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(libraries.get(0).fhirVersion);
            CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder().fhirContext(fhirVersionEnum.newContext()).build();

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
                RetrieveProvider bundleRetrieveProvider = dataProvider.getRight(); // here value sets are added

                List<Bundle.BundleEntryComponent> valueSetEntry = null, valueSetEntryTemp = null;
                Bundle valueSetBundle = null;
                Bundle copySetBundle = null;
                if (bundleRetrieveProvider instanceof BundleRetrieveProvider) {
                    //having value sets entries
                    BundleRetrieveProvider bundleRetrieveProvider1 = (BundleRetrieveProvider) bundleRetrieveProvider;

                    if (bundleRetrieveProvider1.bundle instanceof Bundle) {
                        valueSetBundle = (Bundle) bundleRetrieveProvider1.bundle;
                        copySetBundle = valueSetBundle.copy();
                        valueSetEntry = copySetBundle.getEntry();
                    }
                }

                //having Patient data entries
                for (RetrieveProvider retrieveProvider : retrieveProviders) {
                    try{
                        //for(int i=0; i<retrieveProviders.size(); i++) {

                        PatientData patientData;
                        library.context.contextValue = ((BundleRetrieveProvider) retrieveProvider).getPatientData().getId();
                        String patientId = ((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString();
//                    LOGGER.info("Patient Id in Loop "+patientId);
                        refreshValueSetBundles(valueSetBundle, copySetBundle, valueSetEntry);
                        valueSetEntry = valueSetBundle.copy().getEntry();
                        valueSetEntryTemp = valueSetEntry; //tem having value set entries

                        if (retrieveProvider instanceof BundleRetrieveProvider) {

                            BundleRetrieveProvider retrieveProvider1 = (BundleRetrieveProvider) retrieveProvider;
                            if (retrieveProvider1.bundle instanceof Bundle) {
                                Bundle patientDataBundle = (Bundle) retrieveProvider1.bundle;
                                valueSetEntryTemp.addAll(patientDataBundle.getEntry()); //adding value sets + patient entries

                                RetrieveProvider finalPatientData;

                                Bundle bundle1 = new Bundle();
                                for (Bundle.BundleEntryComponent bundle : valueSetEntryTemp) {
                                    bundle1.addEntry(bundle);  //value set EntryTemp to new Bundle
                                }

                                finalPatientData = new BundleRetrieveProvider(fhirVersionEnum.newContext(), bundle1);

                                //Processing
                                cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getLeft(), dataProvider.getMiddle(), finalPatientData);


                                if (chaipi == 0) {
                                    evaluator = cqlEvaluatorBuilder.build();
                                }

                                if (finalPatientData instanceof BundleRetrieveProvider) {
                                    BundleRetrieveProvider bundleRetrieveProvider1 = (BundleRetrieveProvider) finalPatientData;
                                    bundleRetrieveProvider1.setTerminologyProvider(backupTerminologyProvider);
                                    bundleRetrieveProvider1.setExpandValueSets(true);
                                }

                                chaipi++;
                                processCounter++;
                                VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);
                                Pair<String, Object> contextParameter = null;


                                library.context.contextValue = patientId;
                                if (library.context != null) {
                                    contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
                                }
                                //                  LOGGER.info("Patient being processed in engine: "+patientId);
                                System.out.println("Patient being processed in engine: " + patientId);
                                globalPatientId = library.context.contextValue;
                                EvaluationResult result = evaluator.evaluate(identifier, contextParameter);

                                patientData = (((BundleRetrieveProvider) retrieveProvider).getPatientData());
                                documents.add(this.createDocumentForResult(result.expressionResults, patientData));
                                count++;
                                if (documents.size() > 15) {
                                    dbFunctions.insertProcessedDataInDb("ep_cql_processed_data", documents, dbConnection);
                                    System.out.println("Going to add 15 patients in db, and Thread is going to sleep");
                                    Thread.sleep(100);
                                    documents.clear();
                                }
                            }
                        }
                    }catch(Exception e){
                        LOGGER.error(e.getMessage() + " PatientId: " + globalPatientId, e);

                        Main.failedPatients.add(globalPatientId);
                    }
                }
                retrieveProviders.clear();
                retrieveProviders = null;
                if (documents.size() > 0) {
                    dbFunctions.insertProcessedDataInDb("ep_cql_processed_data", documents, dbConnection);
                    documents.clear();
                }
                documents = null;
            }
        } catch (Exception e) {
//                LOGGER.error(e.getMessage() + " PatientId: " + globalPatientId, e);
//
//                Main.failedPatients.add(globalPatientId);
//                count++;

        }
//        }
        return documents;
    }


    public List<Document> getPayerInfoMap(List<PayerInfo> list) {
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

    public Document createDocumentForResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());


        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("PHQ-9 Modified for Teens");
        expressionResults.remove("PHQ-9 Assessment With A Score Documented");
        expressionResults.remove("PHQ-9 Assessment With A Score Documented aa");
        expressionResults.remove("Interactive Outpatient Encounter With A Diagnosis Of Major Depression Or Dysthymia");
        expressionResults.remove("Assessment Period One");
        expressionResults.remove("April 30 of Measurement Period");
        expressionResults.remove("Assessment Period Two");
        expressionResults.remove("May 1 of Measurement Period");
        expressionResults.remove("August 31 of Measurement Period");
        expressionResults.remove("Assessment Period Three");
        expressionResults.remove("September 1 of Measurement Period");


        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }



    @Override
    public void run() {
        try {
            LOGGER.info("Thread is processing "+skip);
            this.dataBatchingAndProcessing();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
