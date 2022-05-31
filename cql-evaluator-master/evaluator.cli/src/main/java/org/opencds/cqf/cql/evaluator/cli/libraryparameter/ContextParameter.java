package org.opencds.cqf.cql.evaluator.cli.libraryparameter;

public class ContextParameter {
    public String contextName;
    public String contextValue;

    public ContextParameter() {
    }

    public ContextParameter(String contextName, String contextValue) {
        this.contextName = contextName;
        this.contextValue = contextValue;
    }

    public String getContextName() {
        return contextName;
    }

    public void setContextName(String contextName) {
        this.contextName = contextName;
    }

    public String getContextValue() {
        return contextValue;
    }

    public void setContextValue(String contextValue) {
        this.contextValue = contextValue;
    }
}