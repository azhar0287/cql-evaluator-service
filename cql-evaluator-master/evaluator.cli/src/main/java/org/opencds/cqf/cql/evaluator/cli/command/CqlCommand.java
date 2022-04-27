package org.opencds.cqf.cql.evaluator.cli.command;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.cqframework.cql.cql2elm.CqlTranslatorOptions;
import org.cqframework.cql.cql2elm.CqlTranslatorOptionsMapper;
import org.cqframework.cql.elm.execution.VersionedIdentifier;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
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

    @Override
    public Integer call() throws Exception {


        FhirVersionEnum fhirVersionEnum = FhirVersionEnum.valueOf(fhirVersion);

        TypedRetrieveProviderFactory factory;
        Triple<String, ModelResolver, RetrieveProvider> customDataProvider = null;
        RetrieveProvider retrieveProvider = null;

        String resource = "{\n" +
                "    \"_id\" : \"62691f658791290e3cba0949\",\n" +
                "    \"resourceType\" : \"Bundle\",\n" +
                "    \"id\" : \"108314\",\n" +
                "    \"entry\" : [ \n" +
                "        [ \n" +
                "            {\n" +
                "                \"resource\" : {\n" +
                "                    \"resourceType\" : \"Patient\",\n" +
                "                    \"id\" : \"108314\",\n" +
                "                    \"name\" : {\n" +
                "                        \"given\" : [ \n" +
                "                            \"FirstName\"\n" +
                "                        ],\n" +
                "                        \"family\" : \"LastName\"\n" +
                "                    },\n" +
                "                    \"gender\" : \"male\",\n" +
                "                    \"birthDate\" : \"2006-06-19\",\n" +
                "                    \"identifier\" : [ \n" +
                "                        {\n" +
                "                            \"use\" : \"usual\",\n" +
                "                            \"type\" : [],\n" +
                "                            \"system\" : \"\",\n" +
                "                            \"value\" : \"108314\",\n" +
                "                            \"assigner\" : {\n" +
                "                                \"display\" : \"Exchange HMO\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ],\n" +
                "                    \"_class\" : \"com.example.springjsontofhirparser.FhirResources.Patient\"\n" +
                "                },\n" +
                "                \"_class\" : \"com.example.springjsontofhirparser.FhirResources.ResourceChild\"\n" +
                "            }, \n" +
                "            {\n" +
                "                \"resource\" : {\n" +
                "                    \"resourceType\" : \"Coverage\",\n" +
                "                    \"id\" : \"108314\",\n" +
                "                    \"identifier\" : [ \n" +
                "                        {\n" +
                "                            \"use\" : \"usual\",\n" +
                "                            \"type\" : [],\n" +
                "                            \"system\" : \"\",\n" +
                "                            \"value\" : \"108314\",\n" +
                "                            \"assigner\" : {\n" +
                "                                \"display\" : \"Exchange HMO\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ],\n" +
                "                    \"type\" : {\n" +
                "                        \"coding\" : [ \n" +
                "                            {\n" +
                "                                \"system\" : \"urn:oid:HEDIS.Commercial.Custom.Codes.22\",\n" +
                "                                \"version\" : \"\",\n" +
                "                                \"code\" : \"MMO\",\n" +
                "                                \"display\" : \"Commercial Custom code\"\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    },\n" +
                "                    \"policyHolder\" : {\n" +
                "                        \"reference\" : \"Patient/108314\"\n" +
                "                    },\n" +
                "                    \"subscriber\" : {\n" +
                "                        \"reference\" : \"Patient/108314\"\n" +
                "                    },\n" +
                "                    \"beneficiary\" : {\n" +
                "                        \"reference\" : \"Patient/108314\"\n" +
                "                    },\n" +
                "                    \"payor\" : {\n" +
                "                        \"reference\" : \"Organization/Exchange HMO\"\n" +
                "                    },\n" +
                "                    \"period\" : {\n" +
                "                        \"start\" : \"2012-04-03\",\n" +
                "                        \"end\" : \"2023-12-31\"\n" +
                "                    },\n" +
                "                    \"order\" : 0,\n" +
                "                    \"_class\" : \"com.example.springjsontofhirparser.FhirResources.Coverage\"\n" +
                "                },\n" +
                "                \"_class\" : \"com.example.springjsontofhirparser.FhirResources.ResourceChild\"\n" +
                "            }, \n" +
                "            {\n" +
                "                \"resource\" : {\n" +
                "                    \"resourceType\" : \"Encounter\",\n" +
                "                    \"id\" : \"108314\",\n" +
                "                    \"identifier\" : [],\n" +
                "                    \"status\" : \"finished\",\n" +
                "                    \"class\" : {\n" +
                "                        \"system\" : \"urn:oid:\",\n" +
                "                        \"version\" : \"\",\n" +
                "                        \"code\" : \"PersiviaCode\",\n" +
                "                        \"display\" : \"\"\n" +
                "                    },\n" +
                "                    \"type\" : [ \n" +
                "                        {\n" +
                "                            \"coding\" : [ \n" +
                "                                {\n" +
                "                                    \"system\" : \"urn:oid:\",\n" +
                "                                    \"version\" : \"\",\n" +
                "                                    \"code\" : \"PersiviaCode\",\n" +
                "                                    \"display\" : \"\"\n" +
                "                                }\n" +
                "                            ]\n" +
                "                        }\n" +
                "                    ],\n" +
                "                    \"subject\" : {\n" +
                "                        \"reference\" : \"Patient/108314\"\n" +
                "                    },\n" +
                "                    \"diagnosis\" : [],\n" +
                "                    \"participant\" : [],\n" +
                "                    \"period\" : {\n" +
                "                        \"start\" : \"2022-01-01\",\n" +
                "                        \"end\" : \"2022-01-01\"\n" +
                "                    },\n" +
                "                    \"serviceProvider\" : {\n" +
                "                        \"reference\" : \"Practitioner/UNK001\"\n" +
                "                    },\n" +
                "                    \"_class\" : \"com.example.springjsontofhirparser.FhirResources.Encounter\"\n" +
                "                },\n" +
                "                \"_class\" : \"com.example.springjsontofhirparser.FhirResources.ResourceChild\"\n" +
                "            }\n" +
                "        ]\n" +
                "    ],\n" +
                "    \"_class\" : \"com.example.springjsontofhirparser.FhirResources.Bundle\"\n" +
                "}";
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();
        bundle = (IBaseBundle) selectedParser.parseResource(resource);
        retrieveProvider = new BundleRetrieveProvider(fhirContext, bundle);



        CqlEvaluatorComponent cqlEvaluatorComponent = DaggerCqlEvaluatorComponent.builder()
                .fhirContext(fhirVersionEnum.newContext()).build();

        CqlTranslatorOptions options = null;
        if (optionsPath != null) {
            options = CqlTranslatorOptionsMapper.fromFile(optionsPath);
        }

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


            BundleRetrieveProvider bundleRetrieveProvider = (BundleRetrieveProvider) dataProvider.getRight();

            cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getLeft(), dataProvider.getMiddle(),
                    dataProvider.getRight());

            CqlEvaluator evaluator = cqlEvaluatorBuilder.build();

            VersionedIdentifier identifier = new VersionedIdentifier().withId(library.libraryName);

            Pair<String, Object> contextParameter = null;

            if (library.context != null) {
                contextParameter = Pair.of(library.context.contextName, library.context.contextValue);
            }

            EvaluationResult result = evaluator.evaluate(identifier, contextParameter);

            for (Map.Entry<String, Object> libraryEntry : result.expressionResults.entrySet()) {
                System.out.println(libraryEntry.getKey() + "=" + this.tempConvert(libraryEntry.getValue()));
            }

            System.out.println();
        }

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