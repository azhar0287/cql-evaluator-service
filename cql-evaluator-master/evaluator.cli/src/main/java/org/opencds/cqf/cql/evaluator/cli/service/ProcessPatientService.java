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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

    static int  getSize(Object obj){
        return convertObjectToList(obj).size();
    }

    public static List<Document> convertDateObjectToList(Object obj) {
        List<?> list = new ArrayList<>();
        List<Document> documentList=new LinkedList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[])obj);
        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>)obj);
        }
        if(list.size()>0){
            for(Object date:list){
                Document document=new Document();
                document.put("eligibleEdDate",date.toString());
                documentList.add(document);
            }
        }
        return documentList;
    }


    public static List<?> convertObjectToList(Object obj) {
        List<?> list = new ArrayList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[])obj);
        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>)obj);
        }
        return list;
    }



    public static String getSecondEligibleEdVisitDate(Object obj,int index){
        List<?> list = new ArrayList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[])obj);

        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>)obj);
        }
        return list.get(index).toString();
    }

    public static String getSelfHarmDate(Object obj,int index){
        List<?> list = new ArrayList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[])obj);

        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>)obj);
        }
        return list.get(index).toString().substring(9,19);
    }


    public static Date getDateAdded(String dateString,int addDays){
        try {
            // date will be like this 2022-12-21
            String year=dateString.substring(0,4);
            String month=dateString.substring(5,7);
            String day=dateString.substring(8,10);
            // today
            Calendar calendar = Calendar.getInstance();
            Date tempDate=new Date();
            calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
            calendar.set(Calendar.DATE, Integer.parseInt(day));
            calendar.set(Calendar.YEAR, Integer.parseInt(year));
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.set(Calendar.AM_PM,Calendar.AM);
            calendar.add(Calendar.HOUR_OF_DAY, 5);
            calendar.add(Calendar.DAY_OF_YEAR, addDays);
            tempDate = calendar.getTime();
            DateFormat df = new SimpleDateFormat("yyyyMMdd");
            String nowAsISO = df.format(tempDate);
            return df.parse(nowAsISO);

        }catch (Exception e){
            System.out.println("Error while parsing date to increase date "+e);
            return null;
        }
    }
    public static boolean compareDate(Date date1,Date date2){
        //It returns the value 0 if the argument Date is equal to this Date.
        //It returns a value less than 0 if this Date is before the Date argument.
        //It returns a value greater than 0 if this Date is after the Date argument.
        int value= date1.compareTo(date2);
        if(value>=0){
           return true;
        }
        return false;
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
        document.put("id", patientData.getId());
        document.put("birthDate", patientData.getBirthDate());
        document.put("gender", patientData.getGender());
        document.put("payerCodes", getPayerInfoMap(patientData.getPayerInfo()));
        document.put("hospiceFlag", patientData.getHospiceFlag());
        document.put("Eligible ED Visits",getSize(expressionResults.get("Eligible ED Visits")));
        document.put("Denominator 1",getSize(expressionResults.get("Denominator 1")));
        document.put("Denominator 2",getSize(expressionResults.get("Denominator 2")));
        document.put("Numerator 1",getSize(expressionResults.get("Numerator 1")));
        document.put("Numerator 2",getSize(expressionResults.get("Numerator 2")));
        document.put("Exclusions 1",getSize(expressionResults.get("Exclusions 1")));
        document.put("Exclusions 2",getSize(expressionResults.get("Exclusions 2")));
        document.put("ED Exclusions 2",getSize(expressionResults.get("SelfHarm with Hospice Intervention or Encounter")));
        document.put("Initial Population 1",getSize(expressionResults.get("Initial Population 1")));
        document.put("Initial Population 2",getSize(expressionResults.get("Initial Population 2")));
        document.put("Member Coverage",getSize(expressionResults.get("Member Coverage")));
        document.put("Eligible ED Visits Not Followed by Inpatient Admission",getSize(expressionResults.get("Eligible ED Visits Not Followed by Inpatient Admission")));
        document.put("First Eligible ED Visits per 31 Day Period Dates",convertDateObjectToList(expressionResults.get("First Eligible ED Visits per 31 Day Period")));
        document.put("Follow Up Visits",getSize(expressionResults.get("Follow Up Visits")));
        document.put("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm",getSize(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm")));
        document.put("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm Size",getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")));
        if(getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")) > 0){
            document.put("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm",getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0));
        }
        UtilityFunction utilityFunction=new UtilityFunction();

        /// LOGICS
        if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) > 0){
            // Numerator 1 Logic
            if(getSize(expressionResults.get("Numerator 1")) == 0){
                document.put("numerator1Exist",false);
                if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) == 1){
                    document.put("eligibleVisitMoreThan1Num1",false);
                    String firstEncounter=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("numerator1ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstEncounter));
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2){
                    document.put("eligibleVisitMoreThan1Num1",true);
                    String firstEncounter=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("numerator1ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstEncounter));
                    document.put("numerator1ExistAge2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1)));
                }
                else{
                    document.put("eligibleVisitMoreThan1Num1",false);
                    if(getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")) > 0){
                        String firstString=getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0);
                        document.put("numerator1ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstString));
                    }
                    else{
                        document.put("numerator1ExistAge1","0");
                    }
                }
            }
            else{
                document.put("numerator1Exist",true);
                // means numerator 1 size = 1
                if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2 && getSize(expressionResults.get("Numerator 1")) == 1) {
                    document.put("eligibleVisitMoreThan1Num1",true);
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstDateString));
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterThirtyDaysAddDate=getDateAdded(firstDateString,30);
                    // get numerator 1 date
                    String num1DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 1"),0);
                    Date num1Date=getDateAdded(num1DateString,0);


                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num1Date);
                    int firstEncounterDateGreater=encounterThirtyDaysAddDate.compareTo(num1Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }
                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1);
                    document.put("eligibleVisitMoreThan1Num1Age2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),secondDateString));
                    Date secondDateEncounter=getDateAdded(secondDateString,0);
                    Date secondDateEncounterThirtyDaysAddDate=getDateAdded(secondDateString,30);

                    int secondEncounterDateLesser=secondDateEncounter.compareTo(num1Date);
                    int secondEncounterDateGreater=secondDateEncounterThirtyDaysAddDate.compareTo(num1Date);


                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM30B Num1",true);
                    }
                    else {
                        document.put("FUM30B Num1",false);
                    }
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2 && getSize(expressionResults.get("Numerator 1")) >= 2) {
                    document.put("eligibleVisitMoreThan1Num1",true);
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstDateString));
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterThirtyDaysAddDate=getDateAdded(firstDateString,30);
                    // get numerator 1 date
                    String num1DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 1"),0);
                    Date num1Date=getDateAdded(num1DateString,0);


                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num1Date);
                    int firstEncounterDateGreater=encounterThirtyDaysAddDate.compareTo(num1Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }
                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1);
                    document.put("eligibleVisitMoreThan1Num1Age2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),secondDateString));
                    Date secondDateEncounter=getDateAdded(secondDateString,0);
                    Date secondDateEncounterThirtyDaysAddDate=getDateAdded(secondDateString,30);

                    String num1DateString1=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 1"),1);
                    Date num1Date1=getDateAdded(num1DateString1,0);

                    int secondEncounterDateLesser=secondDateEncounter.compareTo(num1Date1);
                    int secondEncounterDateGreater=secondDateEncounterThirtyDaysAddDate.compareTo(num1Date1);
                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM30B Num1",true);
                    }
                    else {
                        document.put("FUM30B Num1",false);
                    }
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) == 1 ) {
                    document.put("eligibleVisitMoreThan1Num1",false);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0)));
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterThirtyDaysAddDate=getDateAdded(firstDateString,30);
                    // get numerator 1 date
                    String num1DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 1"),0);
                    Date num1Date=getDateAdded(num1DateString,0);


                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num1Date);
                    int firstEncounterDateGreater=encounterThirtyDaysAddDate.compareTo(num1Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }

                }
            }

            // Numerator 2 Logic
            if(getSize(expressionResults.get("Numerator 2")) == 0){
                document.put("numerator2Exist",false);
                if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) == 1){
                    document.put("eligibleVisitMoreThan1Num2",false);
                    document.put("numerator2ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0)));
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2){
                    document.put("eligibleVisitMoreThan1Num2",true);
                    document.put("numerator2ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0)));
                    document.put("numerator2ExistAge2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1)));

                }
                else{
                    document.put("eligibleVisitMoreThan1Num2",false);
                    if(getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")) > 0){
                        String firstString=getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0);
                        document.put("numerator2ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstString));
                    }
                    else{
                        document.put("numerator2ExistAge1","0");
                    }
                }

            }
            else{
                document.put("numerator2Exist",true);
                // means numerator 1 size = 1
                if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2 && getSize(expressionResults.get("Numerator 2")) == 1) {
                    document.put("eligibleVisitMoreThan1Num2",true);
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstDateString));
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterSevenDaysAddDate=getDateAdded(firstDateString,7);
                    // get numerator 2 date
                    String num2DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 2"),0);
                    Date num2Date=getDateAdded(num2DateString,0);

                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstEncounterDateGreater=encounterSevenDaysAddDate.compareTo(num2Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }

                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1);
                    document.put("eligibleVisitMoreThan1Num2Age2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),secondDateString));
                    Date secondDateEncounter=getDateAdded(secondDateString,0);
                    Date secondDateEncounterSevenDaysAddDate=getDateAdded(secondDateString,7);

                    int secondEncounterDateLesser=secondDateEncounter.compareTo(num2Date);
                    int secondEncounterDateGreater=secondDateEncounterSevenDaysAddDate.compareTo(num2Date);


                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM7B Num2",true);
                    }
                    else {
                        document.put("FUM7B Num2",false);
                    }
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) >= 2 && getSize(expressionResults.get("Numerator 2")) >=2) {
                    document.put("eligibleVisitMoreThan1Num2",true);
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstDateString));
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterSevenDaysAddDate=getDateAdded(firstDateString,7);
                    // get numerator 2 date
                    String num2DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 2"),0);
                    Date num2Date=getDateAdded(num2DateString,0);

                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstEncounterDateGreater=encounterSevenDaysAddDate.compareTo(num2Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }
                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),1);
                    document.put("eligibleVisitMoreThan1Num2Age2",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),secondDateString));
                    String num2DateString2=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 2"),1);
                    Date num2Date2=getDateAdded(num2DateString2,0);
                    Date secondDateEncounter=getDateAdded(secondDateString,0);
                    Date secondDateEncounterSevenDaysAddDate=getDateAdded(secondDateString,7);

                    int secondEncounterDateLesser=secondDateEncounter.compareTo(num2Date2);
                    int secondEncounterDateGreater=secondDateEncounterSevenDaysAddDate.compareTo(num2Date2);
                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM7B Num2",true);
                    }
                    else {
                        document.put("FUM7B Num2",false);
                    }
                }
                else if(getSize(expressionResults.get("First Eligible ED Visits per 31 Day Period")) == 1) {
                    document.put("eligibleVisitMoreThan1Num2",false);
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0)));
                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("First Eligible ED Visits per 31 Day Period"),0);
                    Date encounterFirstDate=getDateAdded(firstDateString,0);
                    Date encounterSevenDaysAddDate=getDateAdded(firstDateString,7);
                    // get numerator 2 date
                    String num2DateString=getSecondEligibleEdVisitDate(expressionResults.get("Numerator 2"),0);
                    Date num2Date=getDateAdded(num2DateString,0);

                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstEncounterDateGreater=encounterSevenDaysAddDate.compareTo(num2Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }

                }
            }
        }
        else{
            if(getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")) == 1){
                String firstString=getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0);
                Date encounterOrignalDate=getDateAdded(firstString,0);
                Date encounterthirtyDate=getDateAdded(firstString,30);
                Date encountersevenDate=getDateAdded(firstString,7);

                if(getSize(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm")) >= 2){
                    boolean thirtyA=false;
                    boolean sevenA=false;
                    document.put("numerator1Exist",true);
                    document.put("numerator2Exist",true);
                    document.put("eligibleVisitMoreThan1Num1",false);
                    document.put("eligibleVisitMoreThan1Num2",false);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));

                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),0);
                    Date numDate=getDateAdded(firstDateString,0);
                    int firstEncounterDateLesser=encounterOrignalDate.compareTo(numDate);
                    int firstEncounterDateGreater=encounterthirtyDate.compareTo(numDate);

                    int sevenEncounterDateLesser=encounterOrignalDate.compareTo(numDate);
                    int sevenEncounterDateGreater=encountersevenDate.compareTo(numDate);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0){
                        thirtyA=true;
                    }

                    if(sevenEncounterDateLesser <=0 && sevenEncounterDateGreater>=0){
                        sevenA=true;
                    }



                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),1);
                    Date numDate1=getDateAdded(secondDateString,0);

                     firstEncounterDateLesser=encounterOrignalDate.compareTo(numDate1);
                     firstEncounterDateGreater=encounterthirtyDate.compareTo(numDate1);


                     sevenEncounterDateLesser=encounterOrignalDate.compareTo(numDate1);
                     sevenEncounterDateGreater=encountersevenDate.compareTo(numDate1);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0){
                        thirtyA=true;
                    }

                    if(sevenEncounterDateLesser <=0 && sevenEncounterDateGreater>=0){
                        sevenA=true;
                    }


                    if(thirtyA){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }

                    if(sevenA){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }

                }
                else if(getSize(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm")) == 1){
                    document.put("numerator1Exist",true);
                    document.put("numerator2Exist",true);
                    document.put("eligibleVisitMoreThan1Num1",false);
                    document.put("eligibleVisitMoreThan1Num2",false);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));

                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),0);
                    Date numDate=getDateAdded(firstDateString,0);
                    int firstEncounterDateLesser=encounterOrignalDate.compareTo(numDate);
                    int firstEncounterDateGreater=encounterthirtyDate.compareTo(numDate);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }


                    int sevenEncounterDateGreater=encountersevenDate.compareTo(numDate);
                    if(firstEncounterDateLesser <=0 && sevenEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }

                }
                else{
                    document.put("numerator1Exist",false);
                    document.put("numerator2Exist",false);
                    document.put("numerator1ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstString));
                    document.put("numerator2ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),firstString));
                }
            }
            else if(getSize(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm")) > 1){
                String firstString=getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0);
                Date encounterFirstDate=getDateAdded(firstString,0);
                Date encounterFirstthirtyDate=getDateAdded(firstString,30);
                Date encounterFirstSevenDate=getDateAdded(firstString,7);


                String SecondString=getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),1);
                Date encounterSecondDate=getDateAdded(SecondString,0);
                Date encounterSecondThirtyDate=getDateAdded(SecondString,30);
                Date encounterSecondSevenDate=getDateAdded(SecondString,7);


                if(getSize(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm")) == 1){
                    document.put("numerator1Exist",true);
                    document.put("numerator2Exist",true);
                    document.put("eligibleVisitMoreThan1Num1",true);
                    document.put("eligibleVisitMoreThan1Num2",true);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),1)));


                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),0);
                    Date num2Date=getDateAdded(firstDateString,0);

                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstEncounterDateGreater=encounterFirstSevenDate.compareTo(num2Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }


                    int secondEncounterDateLesser=encounterSecondDate.compareTo(num2Date);
                    int secondEncounterDateGreater=encounterSecondSevenDate.compareTo(num2Date);


                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM7B Num2",true);
                    }
                    else {
                        document.put("FUM7B Num2", false);
                    }

                    /// FUM30
                    int firstThirtyEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstThirtyEncounterDateGreater=encounterFirstthirtyDate.compareTo(num2Date);

                    if(firstThirtyEncounterDateLesser <=0 && firstThirtyEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }


                    int secondThirtyEncounterDateLesser=encounterSecondDate.compareTo(num2Date);
                    int secondThirtyEncounterDateGreater=encounterSecondThirtyDate.compareTo(num2Date);


                    if(secondThirtyEncounterDateLesser <=0 && secondThirtyEncounterDateGreater>=0 ){
                        document.put("FUM30B Num1",true);
                    }
                    else {
                        document.put("FUM30B Num1", false);
                    }
                }
                else if(getSize(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm")) >= 2){
                    document.put("numerator1Exist",true);
                    document.put("numerator2Exist",true);
                    document.put("eligibleVisitMoreThan1Num1",true);
                    document.put("eligibleVisitMoreThan1Num2",true);
                    document.put("eligibleVisitMoreThan1Num1Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));
                    document.put("eligibleVisitMoreThan1Num2Age1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),1)));


                    String firstDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),0);
                    Date num2Date=getDateAdded(firstDateString,0);

                    /// FUM30
                    int firstThirtyEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstThirtyEncounterDateGreater=encounterFirstthirtyDate.compareTo(num2Date);

                    if(firstThirtyEncounterDateLesser <=0 && firstThirtyEncounterDateGreater>=0 ){
                        document.put("FUM30A Num1",true);
                    }
                    else {
                        document.put("FUM30A Num1",false);
                    }

                    int firstEncounterDateLesser=encounterFirstDate.compareTo(num2Date);
                    int firstEncounterDateGreater=encounterFirstSevenDate.compareTo(num2Date);

                    if(firstEncounterDateLesser <=0 && firstEncounterDateGreater>=0 ){
                        document.put("FUM7A Num2",true);
                    }
                    else {
                        document.put("FUM7A Num2",false);
                    }


                    String secondDateString=getSecondEligibleEdVisitDate(expressionResults.get("Follow Up Visits with Principal Diagnosis of Mental Health Disorder or Intentional Self-Harm"),1);
                    Date numDate=getDateAdded(secondDateString,0);

                    int secondThirtyEncounterDateLesser=encounterSecondDate.compareTo(numDate);
                    int secondThirtyEncounterDateGreater=encounterSecondThirtyDate.compareTo(numDate);
                    if(secondThirtyEncounterDateLesser <=0 && secondThirtyEncounterDateGreater>=0 ){
                        document.put("FUM30B Num1",true);
                    }
                    else {
                        document.put("FUM30B Num1", false);
                    }

                    int secondEncounterDateLesser=encounterSecondDate.compareTo(numDate);
                    int secondEncounterDateGreater=encounterSecondSevenDate.compareTo(numDate);

                    if(secondEncounterDateLesser <=0 && secondEncounterDateGreater>=0 ){
                        document.put("FUM7B Num2",true);
                    }
                    else {
                        document.put("FUM7B Num2", false);
                    }


                }
                else{
                    document.put("numerator1Exist",false);
                    document.put("numerator2Exist",false);
//                    document.put("eligibleVisitMoreThan1Num1",true);
//                    document.put("eligibleVisitMoreThan1Num2",true);
                    document.put("numerator1ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),0)));
                    document.put("numerator2ExistAge1",utilityFunction.getEncounterBasedAge(utilityFunction.getConvertedDateString(patientData.getBirthDate()),getSelfHarmDate(expressionResults.get("ED Visits With Principal Diagnosis of Mental Illness or Intentional Self-Harm"),1)));
                }


            }
            else{
                document.put("numerator1Exist",false);
                document.put("numerator2Exist",false);
                document.put("numerator1ExistAge1","0");
                document.put("numerator2ExistAge1","0");
            }
        }
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
        expressionResults.remove("Denominator 1");
        expressionResults.remove("Denominator 2");
        expressionResults.remove("Numerator 1");
        expressionResults.remove("Numerator 2");
        expressionResults.remove("Exclusions 1");
        expressionResults.remove("Exclusions 2");
        expressionResults.remove("SelfHarm with Hospice Intervention or Encounter");
        expressionResults.remove("Initial Population 1");
        expressionResults.remove("Initial Population 2");

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
