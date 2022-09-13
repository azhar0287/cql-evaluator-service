package org.opencds.cqf.cql.evaluator.engine.retrieve;


import java.util.Date;
import java.util.List;

public class PatientData {
    private String gender;
    private Date birthDate;
    private String id;
    List<PayerInfo> payerInfo;
    private String hospiceFlag;
    private String pndNumerator1A="0";
    private String pndNumerator1B="0";

    private String prsNumeratorInflA="noOne";

    public String getPrsNumeratorInflA() {
        return prsNumeratorInflA;
    }

    public void setPrsNumeratorInflA(String prsNumeratorInflA) {
        this.prsNumeratorInflA = prsNumeratorInflA;
    }

    public String getPrsNumeratorInflB() {
        return prsNumeratorInflB;
    }

    public void setPrsNumeratorInflB(String prsNumeratorInflB) {
        this.prsNumeratorInflB = prsNumeratorInflB;
    }

    public String getPrsNumeratorTdapA() {
        return prsNumeratorTdapA;
    }

    public void setPrsNumeratorTdapA(String prsNumeratorTdapA) {
        this.prsNumeratorTdapA = prsNumeratorTdapA;
    }

    public String getPrsNumeratorTdapB() {
        return prsNumeratorTdapB;
    }

    public void setPrsNumeratorTdapB(String prsNumeratorTdapB) {
        this.prsNumeratorTdapB = prsNumeratorTdapB;
    }

    private String prsNumeratorInflB="noOne";

    private String prsNumeratorTdapA="noOne";

    private String prsNumeratorTdapB="noOne";

    public String getPndNumeratorForAorB() {
        return pndNumeratorForAorB;
    }

    public void setPndNumeratorForAorB(String pndNumeratorForAorB) {
        this.pndNumeratorForAorB = pndNumeratorForAorB;
    }

    private String pndNumeratorForAorB="noOne";
    public String getPndNumerator1A() {
        return pndNumerator1A;
    }

    public void setPndNumerator1A(String pndNumerator1A) {
        this.pndNumerator1A = pndNumerator1A;
    }

    public String getPndNumerator1B() {
        return pndNumerator1B;
    }

    public void setPndNumerator1B(String pndNumerator1B) {
        this.pndNumerator1B = pndNumerator1B;
    }



    public List<DeliveryProcedureInfo> getDeliveryProcedureInfos() {
        return deliveryProcedureInfos;
    }

    public void setDeliveryProcedureInfos(List<DeliveryProcedureInfo> deliveryProcedureInfos) {
        this.deliveryProcedureInfos = deliveryProcedureInfos;
    }

    List<DeliveryProcedureInfo> deliveryProcedureInfos;

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
}
