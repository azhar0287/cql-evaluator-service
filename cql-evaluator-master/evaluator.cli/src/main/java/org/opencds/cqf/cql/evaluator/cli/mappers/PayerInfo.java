package org.opencds.cqf.cql.evaluator.cli.mappers;

import java.util.Date;


public class PayerInfo {
    public String payerCode;
    Date coverageStartDate;
    Date coverageEndDate;
    String coverageStartDateString;
    String coverageEndDateString;

    public PayerInfo() {
    }

    public String getPayerCode() {
        return payerCode;
    }

    public void setPayerCode(String payerCode) {
        this.payerCode = payerCode;
    }



    public String getCoverageStartDateString() {
        return coverageStartDateString;
    }

    public void setCoverageStartDateString(String coverageStartDateString) {
        this.coverageStartDateString = coverageStartDateString;
    }

    public String getCoverageEndDateString() {
        return coverageEndDateString;
    }

    public void setCoverageEndDateString(String coverageEndDateString) {
        this.coverageEndDateString = coverageEndDateString;
    }

    public Date getCoverageStartDate() {
        return coverageStartDate;
    }

    public void setCoverageStartDate(Date coverageStartDate) {
        this.coverageStartDate = coverageStartDate;
    }

    public Date getCoverageEndDate() {
        return coverageEndDate;
    }

    public void setCoverageEndDate(Date coverageEndDate) {
        this.coverageEndDate = coverageEndDate;
    }
}