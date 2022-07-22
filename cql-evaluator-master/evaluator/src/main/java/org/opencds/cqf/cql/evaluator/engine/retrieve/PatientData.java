package org.opencds.cqf.cql.evaluator.engine.retrieve;

import java.util.Date;
import java.util.List;

public class PatientData {
    private String gender;
    private Date birthDate;
    private String id;
    private List<PayerInfo> payerInfo;
    private List<Premium> premium;
    private String hospiceFlag;
    private String ethnicity;
    private String ethnicityCode;
    private String ethnicityDS;
    private String race;
    private String raceCode;
    private String raceDS;

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

    public List<PayerInfo> getPayerInfo() {
        return payerInfo;
    }

    public void setPayerInfo(List<PayerInfo> payerInfo) {
        this.payerInfo = payerInfo;
    }

    public String getHospiceFlag() {
        return hospiceFlag;
    }

    public void setHospiceFlag(String hospiceFlag) {
        this.hospiceFlag = hospiceFlag;
    }

    public List<Premium> getPremium() {
        return premium;
    }

    public void setPremium(List<Premium> premium) {
        this.premium = premium;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public void setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
    }

    public String getEthnicityCode() {
        return ethnicityCode;
    }

    public void setEthnicityCode(String ethnicityCode) {
        this.ethnicityCode = ethnicityCode;
    }

    public String getEthnicityDS() {
        return ethnicityDS;
    }

    public void setEthnicityDS(String ethnicityDS) {
        this.ethnicityDS = ethnicityDS;
    }

    public String getRace() {
        return race;
    }

    public void setRace(String race) {
        this.race = race;
    }

    public String getRaceCode() {
        return raceCode;
    }

    public void setRaceCode(String raceCode) {
        this.raceCode = raceCode;
    }

    public String getRaceDS() {
        return raceDS;
    }

    public void setRaceDS(String raceDS) {
        this.raceDS = raceDS;
    }

}
