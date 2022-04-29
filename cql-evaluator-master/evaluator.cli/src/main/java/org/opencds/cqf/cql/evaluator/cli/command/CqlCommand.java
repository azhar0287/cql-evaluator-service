package org.opencds.cqf.cql.evaluator.cli.command;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.bson.Document;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptionsMapper;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
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
import org.opencds.cqf.cql.evaluator.builder.data.TypedRetrieveProviderFactory;
import org.opencds.cqf.cql.evaluator.cli.db.DBConnection;
import org.opencds.cqf.cql.evaluator.cql2elm.content.LibraryContentProvider;
import org.opencds.cqf.cql.evaluator.dagger.CqlEvaluatorComponent;
import org.opencds.cqf.cql.evaluator.dagger.DaggerCqlEvaluatorComponent;

import ca.uhn.fhir.context.FhirVersionEnum;
import org.opencds.cqf.cql.evaluator.engine.retrieve.BundleRetrieveProvider;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "cql", mixinStandardHelpOptions = true)
public class CqlCommand implements Callable<Integer> {
    @Option(names = { "-fv", "--fhir-version" }, required = true)
    public String fhirVersion;

    @Option(names= { "-op", "--options-path" })
    public String optionsPath;

    @ArgGroup(multiplicity = "1..*", exclusive = false)
    List<LibraryParameter> libraries;

    static class LibraryParameter {
        @Option(names = { "-lu", "--library-url" }, required = true)
        public String libraryUrl;

        @Option(names = { "-ln", "--library-name" }, required = true)
        public String libraryName;

        @Option(names = { "-lv", "--library-version" })
        public String libraryVersion;

        @Option(names = { "-t", "--terminology-url" })
        public String terminologyUrl;

        @ArgGroup(multiplicity = "0..1", exclusive = false)
        public ModelParameter model;

        @ArgGroup(multiplicity = "0..*", exclusive = false)
        public List<ParameterParameter> parameters;

        @Option(names = { "-e", "--expression" })
        public String[] expression;

        @ArgGroup(multiplicity = "0..1", exclusive = false)
        public ContextParameter context;

        static class ContextParameter {
            @Option(names = { "-c", "--context" })
            public String contextName;

            @Option(names = { "-cv", "--context-value" })
            public String contextValue;
        }

        static class ModelParameter {
            @Option(names = { "-m", "--model" })
            public String modelName;

            @Option(names = { "-mu", "--model-url" })
            public String modelUrl;
        }

        static class ParameterParameter {
            @Option(names = { "-p", "--parameter" })
            public String parameterName;

            @Option(names = { "-pv", "--parameter-value" })
            public String parameterValue;
        }
    }

    private Map<String, LibraryContentProvider> libraryContentProviderIndex = new HashMap<>();
    private Map<String, TerminologyProvider> terminologyProviderIndex = new HashMap<>();


    List<RetrieveProvider> getPatientData() {
        DBConnection db = new DBConnection();

        List<RetrieveProvider> retrieveProviders = new ArrayList<>();

        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();

        List<Document> documents = db.getConditionalData("95550", "ep_encounter_fhir");
        for(Document document:documents) {
            bundle = (IBaseBundle) selectedParser.parseResource(document.toJson());
            RetrieveProvider retrieveProvider = null;
            retrieveProvider = new BundleRetrieveProvider(fhirContext, bundle);
            retrieveProviders.add(retrieveProvider);
        }
        return retrieveProviders;
    }

    void refreshValueSetBundles(Bundle valueSetBundle , Bundle copySetBundle,List<Bundle.BundleEntryComponent> valueSetEntry  ){
        copySetBundle = valueSetBundle.copy();
        valueSetEntry = copySetBundle.getEntry();
    }
    List<String> getPatientIds(){
        StringBuilder builder = new StringBuilder();
        List<String> patientIdList=new LinkedList<>();

        // try block to check for exceptions where
        // object of BufferedReader class us created
        // to read filepath
        try (BufferedReader buffer = new BufferedReader(
                new FileReader("C:\\Projects\\cql-evaluator-service\\cql-evaluator-master\\evaluator.cli\\src\\main\\resources\\patientIds.txt"))) {

            String str;

            // Condition check via buffer.readLine() method
            // holding true upto that the while loop runs
            while ((str = buffer.readLine()) != null) {
                patientIdList.add(str);
                builder.append(str).append("\n");
            }
        }

        // Catch block to handle the exceptions
        catch (IOException e) {

            // Print the line number here exception occured
            // using printStackTrace() method
            e.printStackTrace();
        }

        // Returning a string
        return patientIdList;
    }
    @Override
    public Integer call() throws Exception {



        List<RetrieveProvider> retrieveProviders = getPatientData();
        System.out.println("Patient List Size: "+retrieveProviders.size());
       FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);
        CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder()
                .fhirContext(fhirVersionEnum.newContext()).build();

        CqlTranslatorOptions options = null;
        if (optionsPath != null) {
            options = CqlTranslatorOptionsMapper.fromFile(optionsPath);
        }

        HashMap<String, Map<String, Object>> finalResult = new HashMap<>();
        HashMap<String, Map<String, Object>> finalResultScoreSheetPatients = new HashMap<>();
        int chaipi=0;
        CqlEvaluator evaluator = null;
        List<String> patientIds=getPatientIds();
        for (LibraryParameter library : libraries) {

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
           // RetrieveProvider copyRetrieveProvider  =


            List<Bundle.BundleEntryComponent> valueSetEntry = null, valueSetEntryTemp = null;
            Bundle valueSetBundle = null;
            Bundle copySetBundle = null;
            if(bundleRetrieveProvider instanceof BundleRetrieveProvider)
            {
                //having value sets entries
                BundleRetrieveProvider bundleRetrieveProvider1 = (BundleRetrieveProvider) bundleRetrieveProvider;
                if(bundleRetrieveProvider1.bundle instanceof Bundle){
                    valueSetBundle = (Bundle)bundleRetrieveProvider1.bundle;
                    copySetBundle = valueSetBundle.copy();
                    valueSetEntry = copySetBundle.getEntry();

                }
            }
            
            //having Patient data entries
            for(RetrieveProvider retrieveProvider : retrieveProviders) {
                refreshValueSetBundles(valueSetBundle,copySetBundle,valueSetEntry);
                valueSetEntryTemp = valueSetEntry;

                if(retrieveProvider instanceof BundleRetrieveProvider)
                {
                    BundleRetrieveProvider retrieveProvider1 = (BundleRetrieveProvider) retrieveProvider;
                    if(retrieveProvider1.bundle instanceof Bundle){
                        Bundle bundle2 = (Bundle)retrieveProvider1.bundle;
                        valueSetEntryTemp.addAll(bundle2.getEntry()); //adding value sets + patient entries

                        //Processing
                        cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getLeft(), dataProvider.getMiddle(),
                                bundleRetrieveProvider);

                        if(chaipi==0){
                            evaluator = cqlEvaluatorBuilder.build();
                        }
                        chaipi++;
                         

                        VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);

                        Pair<String, Object> contextParameter = null;

                        if (library.context != null) {
                            contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
                        }

                        EvaluationResult result = evaluator.evaluate(identifier, contextParameter);
                        System.out.println("Adding Patient Result: "+chaipi);
                        finalResult.put(((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString(), result.expressionResults);
                        if(patientIds.equals(((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString())){
                            finalResultScoreSheetPatients.put(((BundleRetrieveProvider) retrieveProvider).bundle.getIdElement().toString(), result.expressionResults);
                            System.out.println("Adding match Patient Result: "+chaipi);
                        }
                    }
                }
            }

        }


        System.out.println("AAAAAAAAA");
        return 0;
    }

    private String tempConvert(Object value) {
        if (value == null) {
            return "null";
        }

        String result = "";
        if (value instanceof Iterable) {
            result += "[";
            Iterable<?> values = (Iterable<?>) value;
            for (Object o : values) {

                result += (tempConvert(o) + ", ");
            }

            if (result.length() > 1) {
                result = result.substring(0, result.length() - 2);
            }

            result += "]";
        } else if (value instanceof IBaseResource) {
            IBaseResource resource = (IBaseResource) value;
            result = resource.fhirType() + (resource.getIdElement() != null && resource.getIdElement().hasIdPart()
                    ? "(id=" + resource.getIdElement().getIdPart() + ")"
                    : "");
        } else if (value instanceof IBase) {
            result = ((IBase) value).fhirType();
        } else if (value instanceof IBaseDatatype) {
            result = ((IBaseDatatype) value).fhirType();
        } else {
            result = value.toString();
        }

        return result;
    }

}
