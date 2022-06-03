package org.opencds.cqf.cql.evaluator.cli.libraryparameter;

public class LibraryOptions {
    public String fhirVersion;
    public String libraryUrl;
    public String libraryName;
    public String libraryVersion;
    public String terminologyUrl;
    public String[] expression;

    public ContextParameter context;
    public ModelParameter model;

    public LibraryOptions(String fhirVersion, String libraryUrl, String libraryName, String libraryVersion, String terminologyUrl, ContextParameter context, ModelParameter model) {
        this.fhirVersion = fhirVersion;
        this.libraryUrl = libraryUrl;
        this.libraryName = libraryName;
        this.libraryVersion = libraryVersion;
        this.terminologyUrl = terminologyUrl;
        this.context = context;
        this.model = model;
    }

    public LibraryOptions() {

    }

    public String getFhirVersion() {
        return fhirVersion;
    }

    public void setFhirVersion(String fhirVersion) {
        this.fhirVersion = fhirVersion;
    }

    public String getLibraryUrl() {
        return libraryUrl;
    }

    public void setLibraryUrl(String libraryUrl) {
        this.libraryUrl = libraryUrl;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public String getLibraryVersion() {
        return libraryVersion;
    }

    public void setLibraryVersion(String libraryVersion) {
        this.libraryVersion = libraryVersion;
    }

    public String getTerminologyUrl() {
        return terminologyUrl;
    }

    public void setTerminologyUrl(String terminologyUrl) {
        this.terminologyUrl = terminologyUrl;
    }

    public String[] getExpression() {
        return expression;
    }

    public void setExpression(String[] expression) {
        this.expression = expression;
    }

    public ContextParameter getContext() {
        return context;
    }

    public void setContext(ContextParameter context) {
        this.context = context;
    }

    public ModelParameter getModel() {
        return model;
    }

    public void setModel(ModelParameter model) {
        this.model = model;
    }
}
