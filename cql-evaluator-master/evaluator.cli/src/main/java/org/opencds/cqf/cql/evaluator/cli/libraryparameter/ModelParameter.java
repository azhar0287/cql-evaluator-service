package org.opencds.cqf.cql.evaluator.cli.libraryparameter;

public class ModelParameter {
    public String modelName;
    public String modelUrl;

    public ModelParameter() {
    }

    public ModelParameter(String modelName, String modelUrl) {
        this.modelName = modelName;
        this.modelUrl = modelUrl;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getModelUrl() {
        return modelUrl;
    }

    public void setModelUrl(String modelUrl) {
        this.modelUrl = modelUrl;
    }
}