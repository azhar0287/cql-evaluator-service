package org.opencds.cqf.cql.evaluator.cli.mappers;

import org.opencds.cqf.cql.evaluator.engine.retrieve.PatientData;

import java.util.HashMap;
import java.util.Map;

public class SheetInputMapper {
    public HashMap<String, PatientData> infoMap = new HashMap<>();
    public HashMap<String, Map<String, Object>> finalResult = new HashMap<>();
    public Map<String, Object> documentMap = new HashMap<>();

    public SheetInputMapper() {

    }

    public SheetInputMapper(Map<String, Object> documentMap) {
        this.documentMap = documentMap;
    }

    public SheetInputMapper(HashMap<String, PatientData> infoMap, HashMap<String, Map<String, Object>> finalResult) {
        this.infoMap = infoMap;
        this.finalResult = finalResult;
    }

    public HashMap<String, PatientData> getInfoMap() {
        return infoMap;
    }

    public void setInfoMap(HashMap<String, PatientData> infoMap) {
        this.infoMap = infoMap;
    }

    public HashMap<String, Map<String, Object>> getFinalResult() {
        return finalResult;
    }

    public void setFinalResult(HashMap<String, Map<String, Object>> finalResult) {
        this.finalResult = finalResult;
    }
}
