package org.opencds.cqf.cql.evaluator.engine.retrieve;


import java.util.Date;

public class DeliveryProcedureInfo {

    public String getProcedureCode() {
        return procedureCode;
    }

    public void setProcedureCode(String procedureCode) {
        this.procedureCode = procedureCode;
    }

    public String getPerformedDateString() {
        return performedDateString;
    }

    public void setPerformedDateString(String performedDateString) {
        this.performedDateString = performedDateString;
    }

    public String getPerformedTimeString() {
        return performedTimeString;
    }

    public void setPerformedTimeString(String performedTimeString) {
        this.performedTimeString = performedTimeString;
    }

    public Date getPerformedDate() {
        return performedDate;
    }

    public void setPerformedDate(Date performedDate) {
        this.performedDate = performedDate;
    }

    public Date getPerformedDateTime() {
        return performedDateTime;
    }

    public void setPerformedDateTime(Date performedDateTime) {
        this.performedDateTime = performedDateTime;
    }

    public String getEndDateString() {
        return endDateString;
    }

    public void setEndDateString(String endDateString) {
        this.endDateString = endDateString;
    }

    public String getEndTimeString() {
        return endTimeString;
    }

    public void setEndTimeString(String endTimeString) {
        this.endTimeString = endTimeString;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    String procedureCode;
    String performedDateString;
    String performedTimeString;
    Date performedDate;
    Date performedDateTime;
    String endDateString;
    String endTimeString;
    String endDate;

}
