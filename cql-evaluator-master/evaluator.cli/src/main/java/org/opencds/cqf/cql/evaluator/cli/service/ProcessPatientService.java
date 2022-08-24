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
        //threadTaskCompleted.isTaskCompleted = true;

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
        //threadTaskCompleted.isTaskCompleted = true;
    }

    List<Document> processAndSavePatients(List<RetrieveProvider> retrieveProviders, DbFunctions dbFunctions) {
        List<Document> documents = new LinkedList<>();
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
                    //for(int i=0; i<retrieveProviders.size(); i++) {

                    PatientData patientData;
                    library.context.contextValue = ((BundleRetrieveProvider) retrieveProvider).getPatientData().getId();
                    String patientId = ((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString();
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
                            //documents.add(this.createDocumentForDSFEResult(result.expressionResults, patientData));
                            //documents.add(this.createDocumentForCISEResult(result.expressionResults, patientData));
                            //documents.add(this.createDocumentForASFEResult(result.expressionResults, patientData));
                            //documents.add(this.createDocumentForDRREResult(result.expressionResults, patientData));
                            //documents.add(this.createDocumentForAMPEResult(result.expressionResults, patientData));
                            //documents.add(this.createDocumentForUOPResult(result.expressionResults,patientData));
                            documents.add(this.createDocumentForFUMResult(result.expressionResults,patientData));
                            if(documents.size() > 15) {
                                dbFunctions.insertProcessedDataInDb(EP_CQL_PROCESSED_DATA, documents, dbConnection);
                                System.out.println("Going to add 15 patients in db, and Thread is going to sleep");
                                Thread.sleep(100);
                                documents.clear();
                            }
                        }
                    }
                }
                retrieveProviders.clear();
                retrieveProviders = null;
                if(documents.size() > 0 ) {
                    dbFunctions.insertProcessedDataInDb(EP_CQL_PROCESSED_DATA, documents, dbConnection);
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

                                patientData = ((BundleRetrieveProvider) retrieveProvider).getPatientData();
                                //documents.add(this.createDocumentForDRREResult(result.expressionResults, patientData));
                                //documents.add(this.createDocumentForAMPEResult(result.expressionResults, patientData));
                                //documents.add(this.createDocumentForUOPResult(result.expressionResults,patientData));
                                documents.add(this.createDocumentForFUMResult(result.expressionResults,patientData));
                                count++;
                                if (documents.size() > 15) {
                                    dbFunctions.insertProcessedDataInDb(EP_CQL_PROCESSED_DATA, documents, dbConnection);
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
                    dbFunctions.insertProcessedDataInDb(EP_CQL_PROCESSED_DATA, documents, dbConnection);
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

    public Document createDocumentForDSFEResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());


        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("December 1 of Measurement Period");
        expressionResults.remove("Adolescent Full Length Depression Screen with Positive Result");
        expressionResults.remove("Adolescent Brief Screen with Positive Result");
        expressionResults.remove("Adult Full Length Depression Screen with Positive Result");
        expressionResults.remove("Adult Brief Screen with Positive Result");
        expressionResults.remove("January 1 of Year Prior to Measurement Period");
        expressionResults.remove("Bipolar Disorder Starting during Year Prior to Measurement Period");
        expressionResults.remove("Depression Starting during Year Prior to Measurement Period");
        expressionResults.remove("Adolescent Full Length Depression Screen with Documented Result");
        expressionResults.remove("Adolescent Brief Screen with Documented Result");
        expressionResults.remove("Adult Full Length Depression Screen with Documented Result");
        expressionResults.remove("Adult Brief Screen with Documented Result");
        expressionResults.remove("Follow Up Care on or 30 Days after First Positive Screen");
        expressionResults.remove("First Positive Adult Screen is Brief Screen");
        expressionResults.remove("First Positive Adolescent Screen is Brief Screen");
        expressionResults.remove("First Positive Adolescent Depression Screen between January 1 and December 1");
        expressionResults.remove("First Positive Adult Depression Screen between January 1 and December 1");
        expressionResults.remove("Adolescent Depression Screening with Positive Result between January 1 and December 1");
        expressionResults.remove("Adult Depression Screening with Positive Result between January 1 and December 1");
        expressionResults.remove("Adolescent Depression Screening with Documented Result between January 1 and December 1");
        expressionResults.remove("Adult Depression Screening with Documented Result between January 1 and December 1");
        expressionResults.remove("Has Positive Brief Screen Same Day as Negative Full Length Screen");

        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForCISEResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());


        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("Date of First Birthday");
        expressionResults.remove("Date of Second Birthday");
        expressionResults.remove("Vaccine Administration Interval - On or Between 42 Days and Second Birthday");
        expressionResults.remove("Vaccine Administration Interval - On or Between 6 Months and Second Birthday");
        expressionResults.remove("Date of First Birthday to Date of Second Birthday");
        expressionResults.remove("Vaccine Administration Interval - On or Between First Birthday and Second Birthday");
        expressionResults.remove("Participation Period");
        expressionResults.remove("Has Severe Combined Immunodeficiency");
        expressionResults.remove("Has Immunodeficiency");
        expressionResults.remove("Has HIV");
        expressionResults.remove("Has Lymphoreticular Cancer, Multiple Myeloma or Leukemia");
        expressionResults.remove("Has Intussusception");
        expressionResults.remove("DTaP Vaccinations");
        expressionResults.remove("Meets DTaP Vaccination Requirement");
        expressionResults.remove("Has Anaphylaxis Due to DTap Vaccine");
        expressionResults.remove("Has Encephalitis Due to DTap Vaccine");
        expressionResults.remove("IPV Vaccinations");
        expressionResults.remove("Meets IPV Vaccination Requirement");
        expressionResults.remove("MMR Vaccinations");
        expressionResults.remove("Meets MMR Vaccination Requirement");
        expressionResults.remove("Has History of Measles, Mumps and Rubella Illness");
        expressionResults.remove("HiB Vaccinations");
        expressionResults.remove("Meets HiB Vaccination Requirement");
        expressionResults.remove("Has Anaphylaxis Due to HiB Vaccine");
        expressionResults.remove("HepB Vaccinations");
        expressionResults.remove("Newborn HepB Vaccinations");
        expressionResults.remove("Meets HepB Vaccination Requirement");
        expressionResults.remove("Has Anaphylaxis Due to Hep B Vaccine or History of Hep B");
        expressionResults.remove("VZV Vaccinations");
        expressionResults.remove("Meets VZV Vaccination Requirement");
        expressionResults.remove("Has History of VZV");
        expressionResults.remove("PCV Vaccinations");
        expressionResults.remove("Meets PCV Vaccination Requirement");
        expressionResults.remove("HepA Vaccinations");
        expressionResults.remove("Meets HepA Vaccination Requirement");
        expressionResults.remove("Has History of Hepatitis A");
        expressionResults.remove("RV 2 Dose Vaccinations");
        expressionResults.remove("RV 3 Dose Vaccinations");
        expressionResults.remove("Meets Rotavirus Vaccination Requirement");
        expressionResults.remove("Has Anaphylaxis Due to Rotavirus Vaccine");
        expressionResults.remove("Influenza Vaccinations");
        expressionResults.remove("LAIV Vaccinations");
        expressionResults.remove("Meets Influenza Vaccination Requirement");
        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForASFEResult(Map<String, Object> expressionResults, PatientData patientData){
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());

        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("January 1 of Year Prior to Measurement Period");
        expressionResults.remove("November 1 of the Measurement Period");
        expressionResults.remove("Patient is 65 or Over");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("AUDIT Screen with Positive Result");
        expressionResults.remove("AUDIT-C Screen with Positive Result");
        expressionResults.remove("Single-Question Screen with Positive Result");
        expressionResults.remove("Unhealthy Alcohol Use Screen with Positive Result Between January 1 and November 1");
        expressionResults.remove("Alcohol Use Disorder starting during Year Prior to Measurement Period");
        expressionResults.remove("Dementia starting on or before end of Measurement Period");
        expressionResults.remove("AUDIT Screen with Documented Result");
        expressionResults.remove("AUDIT-C Screen with Documented Result");
        expressionResults.remove("Single-Question Screen with Documented Result");
        expressionResults.remove("First Positive Screen");
        expressionResults.remove("Counseling or Other Follow-up Care on or 60 Days after First Positive Screen");

        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForDRREResult(Map<String, Object> expressionResults, PatientData patientData){
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());
        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("May 1 of Year Prior to Measurement Period");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("April 30 of Measurement Period");
        expressionResults.remove("Intake Period");
        expressionResults.remove("PHQ-9 Modified For Teens");
        expressionResults.remove("PHQ-9 Assessments");
        expressionResults.remove("Index Episode Start Date");
        expressionResults.remove("Index Episode Start Date 123456");
        expressionResults.remove("Index Episode Start Date Orignal");
        expressionResults.remove("Depression Follow Up Period");
        expressionResults.remove("Last PHQ9 Assessment During Depression Follow Up Period");

        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForAMPEResult(Map<String, Object> expressionResults, PatientData patientData){
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());
        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Member Claims");
        expressionResults.remove("Antipsychotic Medication");
        expressionResults.remove("Antipsychotics on Different Days");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("Glucose Testing During Measurement Period");
        expressionResults.remove("HbA1c Testing During Measurement Period");
        expressionResults.remove("LDLC Testing During Measurement Period");
        expressionResults.remove("Cholesterol Testing During Measurement Period");

        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForUOPResult(Map<String, Object> expressionResults, PatientData patientData){
        Document document = new Document();
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag",patientData.getHospiceFlag());
        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Medication Dispensed from 4 or More Different Pharmacies during Measurement Period");
        expressionResults.remove("Medication Dispensed from 4 or More Different Prescribers during Measurement Period");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("Opioid Medication Coverage Intervals");
        expressionResults.remove("Opioid Medication Coverage Days");
        expressionResults.remove("Opioid Medication Valuesets");
        expressionResults.remove("Member Claims");
        expressionResults.remove("Member Claim Responses");
        expressionResults.remove("Pharmacy Claim With Opioid Medication");
        expressionResults.remove("Two Opioid Medications Dispensed on Different Dates of Service");

        document.putAll(expressionResults); /* Mapping into Document*/
        return document;
    }

    public Document createDocumentForFUMResult(Map<String, Object> expressionResults, PatientData patientData) {
        Document document = new Document();
        System.out.println("Here");
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag", patientData.getHospiceFlag());
        /* Removing extra fields also giving codex error*/
        expressionResults.remove("Patient");
        expressionResults.remove("Member Claims");
        expressionResults.remove("Mental Illness or Intentional Self-Harm");
        expressionResults.remove("December 1 of the Measurement Period");
        expressionResults.remove("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm");
        expressionResults.remove("Member Coverage");
        expressionResults.remove("Eligible ED Visits");
        expressionResults.remove("Eligible ED Visits Not Followed by Inpatient Admission");
        expressionResults.remove("First Eligible ED Visits per 31 Day Period");
        expressionResults.remove("ED Visits with Hospice Intervention or Encounter");
        expressionResults.remove("Follow Up Visits");
        expressionResults.remove("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm");


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
