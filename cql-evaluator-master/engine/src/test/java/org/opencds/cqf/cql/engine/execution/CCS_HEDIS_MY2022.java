package org.opencds.cqf.cql.engine.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.cqframework.cql.cql2elm.CqlTranslator;
import org.cqframework.cql.cql2elm.CqlTranslatorException;
import org.cqframework.cql.elm.execution.Library;
import org.cqframework.cql.elm.tracking.TrackBack;
import org.fhir.ucum.UcumEssenceService;
import org.fhir.ucum.UcumException;
import org.fhir.ucum.UcumService;
import org.testng.annotations.Test;

public class CCS_HEDIS_MY2022 extends CqlExecutionTestBase {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test_nullLibraryLoader_throwsException() {
        new CqlEngine(null);
    }

    public void loadLibrary(String fileName)throws IOException, UcumException {

        library = libraries.get(fileName);
        if (library == null) {
            UcumService ucumService = new UcumEssenceService(UcumEssenceService.class.getResourceAsStream("/ucum-essence.xml"));
            try {
                File cqlFile = new File(URLDecoder.decode(this.getClass().getResource(fileName + ".cql").getFile(), "UTF-8"));

                ArrayList<CqlTranslator.Options> options = new ArrayList<>();
                options.add(CqlTranslator.Options.EnableDateRangeOptimization);
                options.add(CqlTranslator.Options.EnableAnnotations);
                options.add(CqlTranslator.Options.EnableLocators);

                CqlTranslator translator = CqlTranslator.fromFile(cqlFile, getModelManager(), getLibraryManager(), ucumService,
                    options.toArray(new CqlTranslator.Options[options.size()]));

                if (translator.getErrors().size() > 0) {
                    System.err.println("Translation failed due to errors:");
                    ArrayList<String> errors = new ArrayList<>();
                    for (CqlTranslatorException error : translator.getErrors()) {
                        TrackBack tb = error.getLocator();
                        String lines = tb == null ? "[n/a]" : String.format("[%d:%d, %d:%d]",
                            tb.getStartLine(), tb.getStartChar(), tb.getEndLine(), tb.getEndChar());
                        System.err.printf("%s %s%n", lines, error.getMessage());
                        errors.add(lines + error.getMessage());
                    }
                    throw new IllegalArgumentException(errors.toString());
                }

                assertThat(translator.getErrors().size(), is(0));

                jsonFile = new File(cqlFile.getParent(), fileName + ".json");
                jsonFile.createNewFile();

                String json = translator.toJxson();

                PrintWriter pw = new PrintWriter(jsonFile, "UTF-8");
                pw.println(json);
                pw.println();
                pw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            library = JsonCqlLibraryReader.read(jsonFile);
            libraries.put(fileName, library);
        }
    }


    @Test
    public void test_simpleLibrary_returnsResult() throws IOException {
        Library tempLibrary=library;
        for(int i=0;i<tempLibrary.getIncludes().getDef().size();i++){
            try {
                loadLibrary(tempLibrary.getIncludes().getDef().get(i).getPath()+"-"+tempLibrary.getIncludes().getDef().get(i).getVersion());
            } catch (UcumException e) {
                e.printStackTrace();
            }
        }
        LibraryLoader libraryLoader = new InMemoryLibraryLoader(libraries.values());
        //libraries.remove(tempLibrary);
        CqlEngine engine = new CqlEngine(libraryLoader);
        EvaluationResult result = engine.evaluate("CCS_HEDIS_MY2022");
        System.out.println("Test Here");
    }

//    @Test
//    public void test_simpleLibrary_returnsResult() throws IOException {
//        //Library library = this.toLibrary("library Test version '1.0.0'\ndefine X:\n5+5");
//        Library library = this.toLibrary("library CCS_HEDIS_MY2022 version '1.0.0'\n" +
//            "\n" +
//            "using FHIR version '4.0.1'\n" +
//            "\n" +
//            "include FHIRHelpers version '4.0.1' called FHIRHelpers\n" +
//            "include NCQA_HealthPlanEnrollment version '1.0.0' called Enrollment\n" +
//            "include NCQA_Hospice version '1.0.0' called Hospice\n" +
//            "include NCQA_PalliativeCare version '1.0.0' called PalliativeCare\n" +
//            "include NCQA_FHIRBase version '1.0.0' called FHIRBase\n" +
//            "include NCQA_Status version '1.0.0' called Status\n" +
//            "include NCQA_Terminology version '1.0.0' called Terminology\n" +
//            "include NCQA_CQLBase version '1.0.0' called CQLBase\n" +
//            "\n" +
//            "valueset \"Absence of Cervix Diagnosis\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1522'\n" +
//            "valueset \"Cervical Cytology Result or Finding\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1524'\n" +
//            "valueset \"High Risk HPV Test Result or Finding\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1526'\n" +
//            "valueset \"Cervical Cytology Lab Test\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1525'\n" +
//            "valueset \"High Risk HPV Lab Test\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1527'\n" +
//            "valueset \"Hysterectomy With No Residual Cervix\": 'https://www.ncqa.org/fhir/valueset/2.16.840.1.113883.3.464.1004.1523'\n" +
//            "\n" +
//            "parameter \"Measurement Period\" Interval<DateTime>\n" +
//            "\n" +
//            "context Patient\n" +
//            "\n" +
//            "define \"Initial Population\":\n" +
//            "  Patient.gender.value = 'female'\n" +
//            "    and AgeInYearsAt(date from \n" +
//            "      end of \"Measurement Period\"\n" +
//            "    )in Interval[24, 64]\n" +
//            "    and \"Enrolled During Participation Period\"\n" +
//            "\n" +
//            "define \"Enrolled During Participation Period\":\n" +
//            "  if ( exists \"Product Line as of December 31 of Measurement Period\" ProductType\n" +
//            "      where FHIRHelpers.ToCode ( ProductType ) ~ Terminology.\"managed care policy\"\n" +
//            "  ) then ( Enrollment.\"Health Plan Enrollment Criteria\" ( \"Member Coverage\", date from \n" +
//            "    end of \"Measurement Period\", Interval[date from start of \"Measurement Period\" - 2 years, date from \n" +
//            "    end of \"Measurement Period\" - 2 years], 45 )\n" +
//            "      and Enrollment.\"Health Plan Enrollment Criteria\" ( \"Member Coverage\", date from \n" +
//            "      end of \"Measurement Period\", Interval[date from start of \"Measurement Period\" - 1 year, date from \n" +
//            "      end of \"Measurement Period\" - 1 year], 45 )\n" +
//            "      and Enrollment.\"Health Plan Enrollment Criteria\" ( \"Member Coverage\", date from \n" +
//            "      end of \"Measurement Period\", Interval[date from start of \"Measurement Period\", date from \n" +
//            "      end of \"Measurement Period\"], 45 )\n" +
//            "  ) \n" +
//            "    else if ( exists \"Product Line as of December 31 of Measurement Period\" ProductType\n" +
//            "      where FHIRHelpers.ToCode ( ProductType ) ~ Terminology.\"subsidized health program\"\n" +
//            "  ) then ( Enrollment.\"Health Plan Enrollment Criteria\" ( \"Member Coverage\", date from \n" +
//            "    end of \"Measurement Period\", Interval[date from start of \"Measurement Period\", date from \n" +
//            "    end of \"Measurement Period\"], 45 )\n" +
//            "  ) \n" +
//            "    else false\n" +
//            "\n" +
//            "define \"Product Line as of December 31 of Measurement Period\":\n" +
//            "  flatten ( \"Member Coverage\" C\n" +
//            "    where FHIRBase.\"Normalize Interval\" ( C.period ) includes \n" +
//            "    end of Last(CQLBase.\"Sort Date Intervals\"(Enrollment.\"All Coverage Info\"(\"Member Coverage\", Interval[date from start of \"Measurement Period\" - 2 years, date from \n" +
//            "      end of \"Measurement Period\"]).CollapsedFinal)\n" +
//            "    )) Records\n" +
//            "    return Records.type.coding\n" +
//            "\n" +
//            "define \"Member Coverage\":\n" +
//            "  [Coverage] C\n" +
//            "    where FHIRBase.\"Normalize Interval\" ( C.period ) overlaps Interval[start of \"Measurement Period\" - 2 years, \n" +
//            "    end of \"Measurement Period\"]\n" +
//            "\n" +
//            "define \"Denominator\":\n" +
//            "  \"Initial Population\"\n" +
//            "\n" +
//            "define \"Exclusions\":\n" +
//            "  Hospice.\"Hospice Intervention or Encounter\"\n" +
//            "    or PalliativeCare.\"Palliative Care Overlapping Period\" ( \"Measurement Period\" )\n" +
//            "\n" +
//            "define \"Numerator\":\n" +
//            "  exists \"Cervical Cytology Within 3 Years\"\n" +
//            "    or exists \"hrHPV Testing Within 5 Years\"\n" +
//            "\n" +
//            "define \"Cervical Cytology Within 3 Years\":\n" +
//            "  ( [Observation: \"Cervical Cytology Lab Test\"]\n" +
//            "    union [Observation: \"Cervical Cytology Result or Finding\"] ) CervicalCytology\n" +
//            "    where FHIRBase.\"Normalize Interval\" ( CervicalCytology.effective ) during Interval[start of \"Measurement Period\" - 2 years, \n" +
//            "    end of \"Measurement Period\"]\n" +
//            "\n" +
//            "define \"hrHPV Testing Within 5 Years\":\n" +
//            "  ( [Observation: \"High Risk HPV Lab Test\"]\n" +
//            "    union [Observation: \"High Risk HPV Test Result or Finding\"] ) HPVTest\n" +
//            "    where AgeInYearsAt(date from start of FHIRBase.\"Normalize Interval\"(HPVTest.effective))>= 30\n" +
//            "      and FHIRBase.\"Normalize Interval\" ( HPVTest.effective ) during Interval[start of \"Measurement Period\" - 4 years, \n" +
//            "      end of \"Measurement Period\"]\n" +
//            "\n" +
//            "define \"Denominator Exceptions\":\n" +
//            "  exists \"Absence of Cervix\"\n" +
//            "\n" +
//            "define \"Absence of Cervix\":\n" +
//            "  ( ( Status.\"Completed Procedure\" ( [Procedure: \"Hysterectomy With No Residual Cervix\"] ) ) NoCervixHysterectomy\n" +
//            "      where FHIRBase.\"Normalize Interval\" ( NoCervixHysterectomy.performed ) ends on or before \n" +
//            "      end of \"Measurement Period\"\n" +
//            "  )\n" +
//            "    union ( ( Status.\"Active Condition\" ( [Condition: \"Absence of Cervix Diagnosis\"] ) ) NoCervix\n" +
//            "        where FHIRBase.\"Prevalence Period\" ( NoCervix ) starts on or before \n" +
//            "        end of \"Measurement Period\"\n" +
//            "    )");
//
//        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));
//
//        CqlEngine engine = new CqlEngine(libraryLoader);
//
//        EvaluationResult result = engine.evaluate("CCS_HEDIS_MY2022");
//        System.out.println("Test jhere");
//
//    }







    //----------------------------------------------------------------------------------------------------

    /*
    @Test
    public void test_simpleLibraryWithParam_returnsParamValue() throws IOException {
        Library library = this.toLibrary("library Test version '1.0.0'\nparameter IntValue Integer\ndefine X:\nIntValue");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader);

        Map<String,Object> parameters = new HashMap<>();
        parameters.put("IntValue", 10);

        EvaluationResult result = engine.evaluate("Test", parameters);


        Object expResult = result.forExpression("X");

        assertThat(expResult, is(10));
    }


    @Test
    public void test_dataLibrary_noProvider_throwsException() throws IOException {
        Library library = this.toLibrary("library Test version '1.0.0'\nusing FHIR version '3.0.0'\ndefine X:\n5+5");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader);

        engine.evaluate("Test");
    }

    @Test
    public void test_twoExpressions_byLibrary_allReturned() throws IOException {
        Library library = this.toLibrary("library Test version '1.0.0'\ndefine X:\n5+5\ndefine Y: 2 + 2");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader);

        EvaluationResult result = engine.evaluate("Test");

        assertNotNull(result);

        Object expResult = result.forExpression("X");
        assertThat(expResult, is(10));

        expResult = result.forExpression("Y");
        assertThat(expResult, is(4));
    }

    @Test
    public void test_twoExpressions_oneRequested_oneReturned() throws IOException {
        Library library = this.toLibrary("library Test version '1.0.0'\ndefine X:\n5+5\ndefine Y: 2 + 2");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader);

        EvaluationResult result = engine.evaluate("Test", new HashSet<>(Arrays.asList("Y")));

        assertNotNull(result);

        Object expResult = result.forExpression("Y");
        assertThat(expResult, is(4));
    }

    @Test
    public void test_twoLibraries_expressionsForEach() throws IOException {

        Map<org.hl7.elm.r1.VersionedIdentifier, String> libraries = new HashMap<>();
        libraries.put(this.toElmIdentifier("Common", "1.0.0"),
            "library Common version '1.0.0'\ndefine Z:\n5+5\n");
        libraries.put(toElmIdentifier("Test", "1.0.0"),
            "library Test version '1.0.0'\ninclude Common version '1.0.0' named \"Common\"\ndefine X:\n5+5\ndefine Y: 2 + 2\ndefine W: \"Common\".Z + 5");


        LibraryManager libraryManager = this.toLibraryManager(libraries);
        List<CqlTranslatorException> errors = new ArrayList<>();
        List<Library> executableLibraries = new ArrayList<>();
        for (org.hl7.elm.r1.VersionedIdentifier id : libraries.keySet()) {
            TranslatedLibrary translated = libraryManager.resolveLibrary(id, CqlTranslatorOptions.defaultOptions(), errors);
            String json = this.convertToJson(translated.getLibrary());
            executableLibraries.add(this.readJson(json));
        }

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(executableLibraries);

        CqlEngine engine = new CqlEngine(libraryLoader);

        EvaluationResult result = engine.evaluate("Test", new HashSet<>(Arrays.asList("X", "Y", "W")));

        assertNotNull(result);
        assertEquals(3, result.expressionResults.size());
        assertThat(result.forExpression("X"), is(10));
        assertThat(result.forExpression("Y"), is(4));
        assertThat(result.forExpression("W"), is(15));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validationEnabled_validatesTerminology() throws IOException  {
        Library library = this.toLibrary("library Test version '1.0.0'\ncodesystem \"X\" : 'http://example.com'\ndefine X:\n5+5\ndefine Y: 2 + 2");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader, EnumSet.of(CqlEngine.Options.EnableValidation));
        engine.evaluate("Test");
    }

    @Test
    public void validationDisabled_doesNotValidateTerminology() throws IOException {
        Library library = this.toLibrary("library Test version '1.0.0'\ncodesystem \"X\" : 'http://example.com'\ndefine X:\n5+5\ndefine Y: 2 + 2");

        LibraryLoader libraryLoader = new InMemoryLibraryLoader(Collections.singleton(library));

        CqlEngine engine = new CqlEngine(libraryLoader);
        engine.evaluate("Test");
    }*/
}