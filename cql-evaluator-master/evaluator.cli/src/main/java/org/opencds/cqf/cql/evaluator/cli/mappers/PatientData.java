package org.opencds.cqf.cql.evaluator.cli.mappers;

import java.util.Date;

public class PatientData {
    private String gender;
    private Date birthDate;
    private String id;
    private String hospiceFlag;
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

    public String getHospiceFlag() {
        return hospiceFlag;
    }

    public void setHospiceFlag(String hospiceFlag) {
        this.hospiceFlag = hospiceFlag;
    }
}
