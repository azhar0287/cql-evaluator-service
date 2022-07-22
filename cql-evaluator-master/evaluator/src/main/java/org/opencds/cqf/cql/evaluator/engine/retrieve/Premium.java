package org.opencds.cqf.cql.evaluator.engine.retrieve;

import java.util.Date;

public class Premium {
    String hospice;
    String hospiceDateString;
    Date hospiceDate;
    Date hospiceDateTime;
    Date endDate;
    Date endDateTime;
    String startDateString;
    Date startDate;
    Date startDateTime;
    String lti;
    String lis;
    String orec;
    String lisHist;

    public Premium() {
    }

    public Premium(String hospice) {
        this.hospice = hospice;
    }

    public String getHospice() {
        return hospice;
    }

    public void setHospice(String hospice) {
        this.hospice = hospice;
    }

    public String getHospiceDateString() {
        return hospiceDateString;
    }

    public void setHospiceDateString(String hospiceDateString) {
        this.hospiceDateString = hospiceDateString;
    }

    public Date getHospiceDate() {
        return hospiceDate;
    }

    public void setHospiceDate(Date hospiceDate) {
        this.hospiceDate = hospiceDate;
    }

    public Date getHospiceDateTime() {
        return hospiceDateTime;
    }

    public void setHospiceDateTime(Date hospiceDateTime) {
        this.hospiceDateTime = hospiceDateTime;
    }

    public String getStartDateString() {
        return startDateString;
    }

    public void setStartDateString(String startDateString) {
        this.startDateString = startDateString;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(Date startDateTime) {
        this.startDateTime = startDateTime;
    }

    public String getLti() {
        return lti;
    }

    public void setLti(String lti) {
        this.lti = lti;
    }

    public String getLis() {
        return lis;
    }

    public void setLis(String lis) {
        this.lis = lis;
    }

    public String getOrec() {
        return orec;
    }

    public void setOrec(String orec) {
        this.orec = orec;
    }

    public String getLisHist() {
        return lisHist;
    }

    public void setLisHist(String lisHist) {
        this.lisHist = lisHist;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Date getEndDateTime() {
        return endDateTime;
    }

    public void setEndDateTime(Date endDateTime) {
        this.endDateTime = endDateTime;
    }
}