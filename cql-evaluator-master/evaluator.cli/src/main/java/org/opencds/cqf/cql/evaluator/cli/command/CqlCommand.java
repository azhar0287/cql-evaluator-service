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

        String resource = "{\n" +
                "  \"resourceType\": \"Bundle\",\n" +
                "  \"id\": \"95229\",\n" +
                "  \"entry\": [\n" +
                "    [\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Patient\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"name\": {\n" +
                "            \"given\": [\n" +
                "              \"FirstName\"\n" +
                "            ],\n" +
                "            \"family\": \"LastName\"\n" +
                "          },\n" +
                "          \"gender\": \"male\",\n" +
                "          \"birthDate\": \"1981-03-08\",\n" +
                "          \"identifier\": [\n" +
                "            {\n" +
                "              \"use\": \"usual\",\n" +
                "              \"type\": [],\n" +
                "              \"system\": \"\",\n" +
                "              \"value\": \"95229\",\n" +
                "              \"assigner\": {\n" +
                "                \"display\": \"Commercial HMO\"\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Coverage\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"identifier\": [\n" +
                "            {\n" +
                "              \"use\": \"usual\",\n" +
                "              \"type\": [],\n" +
                "              \"system\": \"\",\n" +
                "              \"value\": \"95229\",\n" +
                "              \"assigner\": {\n" +
                "                \"display\": \"Commercial HMO\"\n" +
                "              }\n" +
                "            }\n" +
                "          ],\n" +
                "          \"type\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:HEDIS.Commercial.Custom.Codes.22\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"HMO\",\n" +
                "                \"display\": \"Commercial Custom code\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"policyHolder\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"subscriber\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"beneficiary\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"payor\": {\n" +
                "            \"reference\": \"Organization/Commercial HMO\"\n" +
                "          },\n" +
                "          \"period\": {\n" +
                "            \"start\": \"2022-02-16\",\n" +
                "            \"end\": \"2023-01-26\"\n" +
                "          },\n" +
                "          \"order\": 0\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Procedure\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"status\": \"in-progress\",\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"90653\",\n" +
                "                \"display\": \"Influenza Vaccination Value Set\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"Influenza Vaccination Value Set\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/90653\"\n" +
                "          },\n" +
                "          \"performedPeriod\": {\n" +
                "            \"start\": \"2010-03-29\",\n" +
                "            \"end\": \"\"\n" +
                "          },\n" +
                "          \"performer\": [],\n" +
                "          \"reasonCode\": [],\n" +
                "          \"report\": [\n" +
                "            {\n" +
                "              \"reference\": \"DiagnosticReport/90653\"\n" +
                "            }\n" +
                "          ],\n" +
                "          \"_class\": \"com.example.springjsontofhirparser.FhirResources.Procedure\"\n" +
                "        },\n" +
                "        \"_class\": \"com.example.springjsontofhirparser.FhirResources.ResourceChild\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Condition\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"clinicalStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-clinical\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"active\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"verificationStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"confirmed\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"category\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"http://terminology.hl7.org/CodeSystem/condition-category\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"problem-list-item\",\n" +
                "                  \"display\": \"Problem List Item\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"severity\": {\n" +
                "            \"coding\": []\n" +
                "          },\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"379.8\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/87d394a5-076c-484b-aa75-e43051841c22\"\n" +
                "          },\n" +
                "          \"onsetDateTime\": \"2010-03-29T10:00:00\",\n" +
                "          \"recordedDate\": \"2010-03-29T10:00:00\",\n" +
                "          \"stage\": [],\n" +
                "          \"_class\": \"com.example.springjsontofhirparser.FhirResources.Condition\"\n" +
                "        },\n" +
                "        \"_class\": \"com.example.springjsontofhirparser.FhirResources.ResourceChild\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Condition\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"clinicalStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-clinical\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"active\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"verificationStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"confirmed\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"category\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"http://terminology.hl7.org/CodeSystem/condition-category\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"problem-list-item\",\n" +
                "                  \"display\": \"Problem List Item\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"severity\": {\n" +
                "            \"coding\": []\n" +
                "          },\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"550.90\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/87d394a5-076c-484b-aa75-e43051841c22\"\n" +
                "          },\n" +
                "          \"onsetDateTime\": \"2010-03-29T10:00:00\",\n" +
                "          \"recordedDate\": \"2010-03-29T10:00:00\",\n" +
                "          \"stage\": []\n" +
                "          \n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Condition\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"clinicalStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-clinical\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"active\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"verificationStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"confirmed\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"category\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"http://terminology.hl7.org/CodeSystem/condition-category\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"problem-list-item\",\n" +
                "                  \"display\": \"Problem List Item\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"severity\": {\n" +
                "            \"coding\": []\n" +
                "          },\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"218.1\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/87d394a5-076c-484b-aa75-e43051841c22\"\n" +
                "          },\n" +
                "          \"onsetDateTime\": \"2010-03-29T10:00:00\",\n" +
                "          \"recordedDate\": \"2010-03-29T10:00:00\",\n" +
                "          \"stage\": []\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Condition\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"clinicalStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-clinical\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"active\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"verificationStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"confirmed\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"category\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"http://terminology.hl7.org/CodeSystem/condition-category\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"problem-list-item\",\n" +
                "                  \"display\": \"Problem List Item\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"severity\": {\n" +
                "            \"coding\": []\n" +
                "          },\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.1462\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"363.55\",\n" +
                "                \"display\": \"Hereditary Choroidal Dystrophies Value Set\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/87d394a5-076c-484b-aa75-e43051841c22\"\n" +
                "          },\n" +
                "          \"onsetDateTime\": \"2010-03-29T10:00:00\",\n" +
                "          \"recordedDate\": \"2010-03-29T10:00:00\",\n" +
                "          \"stage\": []\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Condition\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"clinicalStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-clinical\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"active\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"verificationStatus\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"confirmed\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"category\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"http://terminology.hl7.org/CodeSystem/condition-category\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"problem-list-item\",\n" +
                "                  \"display\": \"Problem List Item\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"severity\": {\n" +
                "            \"coding\": []\n" +
                "          },\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"738554003\",\n" +
                "                \"display\": \"\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/null\"\n" +
                "          },\n" +
                "          \"onsetDateTime\": \"2010-03-29T10:00:00\",\n" +
                "          \"recordedDate\": \"2010-03-29T10:00:00\",\n" +
                "          \"stage\": []\n" +
                "        }\n" +
                "     \n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"_id\": \"95229\",\n" +
                "          \"resourceType\": \"Observation\",\n" +
                "          \"status\": \"final\",\n" +
                "          \"code\": {\n" +
                "            \"coding\": [\n" +
                "              {\n" +
                "                \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                \"version\": \"\",\n" +
                "                \"code\": \"90653\",\n" +
                "                \"display\": \"Influenza Vaccination Value Set\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"text\": \"Influenza Vaccination Value Set\"\n" +
                "          },\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"encounter\": {\n" +
                "            \"reference\": \"Encounter/90653\"\n" +
                "          },\n" +
                "          \"effectivePeriod\": {\n" +
                "            \"start\": \"2010-03-29T10:00:00\",\n" +
                "            \"end\": \"2010-03-29T10:00:00\"\n" +
                "          },\n" +
                "          \"issued\": \"2010-03-29T10:00:00\",\n" +
                "          \"performer\": [\n" +
                "            {\n" +
                "              \"actor\": {\n" +
                "                \"reference\": \"Practitioner/UNK001\",\n" +
                "                \"display\": \"Practitioner/UNK001\"\n" +
                "              }\n" +
                "            }\n" +
                "          ],\n" +
                "          \"component\": []\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Encounter\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"identifier\": [],\n" +
                "          \"status\": \"finished\",\n" +
                "          \"class\": {\n" +
                "            \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "            \"version\": \"\",\n" +
                "            \"code\": \"90653\",\n" +
                "            \"display\": \"Influenza Vaccination Value Set\"\n" +
                "          },\n" +
                "          \"type\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"90653\",\n" +
                "                  \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"diagnosis\": [\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"90653\",\n" +
                "                    \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 1\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"90653\",\n" +
                "                    \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"90653\",\n" +
                "                    \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/Hereditary Choroidal Dystrophies Value Set\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"90653\",\n" +
                "                    \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:2.16.840.1.113883.3.526.3.402\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"90653\",\n" +
                "                    \"display\": \"Influenza Vaccination Value Set\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            }\n" +
                "          ],\n" +
                "          \"participant\": [],\n" +
                "          \"period\": {\n" +
                "            \"start\": \"2010-03-29\",\n" +
                "            \"end\": \"2010-03-29\"\n" +
                "          },\n" +
                "          \"serviceProvider\": {\n" +
                "            \"reference\": \"Practitioner/UNK001\"\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"resource\": {\n" +
                "          \"resourceType\": \"Encounter\",\n" +
                "          \"id\": \"95229\",\n" +
                "          \"identifier\": [],\n" +
                "          \"status\": \"finished\",\n" +
                "          \"class\": {\n" +
                "            \"system\": \"urn:oid:\",\n" +
                "            \"version\": \"\",\n" +
                "            \"code\": \"PersiviaCode\",\n" +
                "            \"display\": \"\"\n" +
                "          },\n" +
                "          \"type\": [\n" +
                "            {\n" +
                "              \"coding\": [\n" +
                "                {\n" +
                "                  \"system\": \"urn:oid:\",\n" +
                "                  \"version\": \"\",\n" +
                "                  \"code\": \"PersiviaCode\",\n" +
                "                  \"display\": \"\"\n" +
                "                }\n" +
                "              ]\n" +
                "            }\n" +
                "          ],\n" +
                "          \"subject\": {\n" +
                "            \"reference\": \"Patient/95229\"\n" +
                "          },\n" +
                "          \"diagnosis\": [\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"PersiviaCode\",\n" +
                "                    \"display\": \"\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 1\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"PersiviaCode\",\n" +
                "                    \"display\": \"\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"PersiviaCode\",\n" +
                "                    \"display\": \"\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/Hereditary Choroidal Dystrophies Value Set\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"PersiviaCode\",\n" +
                "                    \"display\": \"\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            },\n" +
                "            {\n" +
                "              \"condition\": {\n" +
                "                \"reference\": \"Condition/\"\n" +
                "              },\n" +
                "              \"use\": {\n" +
                "                \"coding\": [\n" +
                "                  {\n" +
                "                    \"system\": \"urn:oid:\",\n" +
                "                    \"version\": \"\",\n" +
                "                    \"code\": \"PersiviaCode\",\n" +
                "                    \"display\": \"\"\n" +
                "                  }\n" +
                "                ]\n" +
                "              },\n" +
                "              \"rank\": 0\n" +
                "            }\n" +
                "          ],\n" +
                "          \"participant\": [],\n" +
                "          \"period\": {\n" +
                "            \"start\": \"2022-01-01\",\n" +
                "            \"end\": \"2022-01-01\"\n" +
                "          },\n" +
                "          \"serviceProvider\": {\n" +
                "            \"reference\": \"Practitioner/UNK001\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  ]\n" +
                "}";
        IBaseBundle bundle;
        FhirContext fhirContext = fhirVersionEnum.newContext();
        IParser selectedParser = fhirContext.newJsonParser();
        bundle = (IBaseBundle) selectedParser.parseResource(resource);
        RetrieveProvider retrieveProvider = null;
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

            RetrieveProvider bundleRetrieveProvider =  dataProvider.getRight();



            if(bundleRetrieveProvider instanceof BundleRetrieveProvider)
            {
                BundleRetrieveProvider bundleRetrieveProvider1=(BundleRetrieveProvider)bundleRetrieveProvider;
                if(bundleRetrieveProvider1.bundle instanceof Bundle){
                    Bundle bundle1=(Bundle)bundleRetrieveProvider1.bundle;
                    bundle1.getEntry();
                    if(retrieveProvider instanceof BundleRetrieveProvider)
                    {
                        BundleRetrieveProvider retrieveProvider1=(BundleRetrieveProvider)retrieveProvider;
                        if(retrieveProvider1.bundle instanceof Bundle){
                            Bundle bundle2=(Bundle)retrieveProvider1.bundle;
                            bundle1.getEntry().addAll(bundle2.getEntry());
                        }
                    }
                }
            }


            cqlEvaluatorBuilder.withModelResolverAndRetrieveProvider(dataProvider.getLeft(), dataProvider.getMiddle(),
                   bundleRetrieveProvider);


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
