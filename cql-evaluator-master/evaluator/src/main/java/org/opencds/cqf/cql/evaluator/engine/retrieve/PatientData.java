package org.opencds.cqf.cql.evaluator.engine.retrieve;

import java.util.Date;
import java.util.List;

public class PatientData {
    private String gender;
    private Date birthDate;
    private String id;
    List<PayerInfo> payerInfo;
    private String hospiceFlag;

    private Boolean uopNumeratorA;
    private Boolean uopNumeratorB;
    private Boolean uopNumeratorC;

    private Boolean uopDenominatorA;
    private Boolean uopDenominatorB;
    private Boolean uopDenominatorC;

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

    public Boolean getUopNumeratorA() {
        return uopNumeratorA;
    }

    public void setUopNumeratorA(Boolean uopNumeratorA) {
        this.uopNumeratorA = uopNumeratorA;
    }

    public Boolean getUopNumeratorB() {
        return uopNumeratorB;
    }

    public void setUopNumeratorB(Boolean uopNumeratorB) {
        this.uopNumeratorB = uopNumeratorB;
    }

    public Boolean getUopNumeratorC() {
        return uopNumeratorC;
    }

    public void setUopNumeratorC(Boolean uopNumeratorC) {
        this.uopNumeratorC = uopNumeratorC;
    }

    public Boolean getUopDenominatorA() {
        return uopDenominatorA;
    }

    public void setUopDenominatorA(Boolean uopDenominatorA) {
        this.uopDenominatorA = uopDenominatorA;
    }

    public Boolean getUopDenominatorB() {
        return uopDenominatorB;
    }

    public void setUopDenominatorB(Boolean uopDenominatorB) {
        this.uopDenominatorB = uopDenominatorB;
    }

    public Boolean getUopDenominatorC() {
        return uopDenominatorC;
    }

    public void setUopDenominatorC(Boolean uopDenominatorC) {
        this.uopDenominatorC = uopDenominatorC;
    }
}
