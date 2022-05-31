package org.opencds.cqf.cql.evaluator.engine.retrieve;

import java.util.Date;
import java.util.List;

public class PatientData {
    private String gender;
    private Date birthDate;
    private String id;
    List<String> payerCodes;

    public PatientData() {
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Date getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(Date birthDate) {
        this.birthDate = birthDate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getPayerCodes() {
        return payerCodes;
    }

    public void setPayerCodes(List<String> payerCodes) {
        this.payerCodes = payerCodes;
    }
}