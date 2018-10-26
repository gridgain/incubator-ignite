package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Sensitivity {

  @QuerySqlField
  private String positionConstituentId;

  @QuerySqlField(index=true, orderedGroups={@QuerySqlField.Group(
          name = "s_riskType_label2_idx", order = 0)})
  private String riskType;

  @QuerySqlField(index=true)
  private String qualifier;

  @QuerySqlField
  private String bucket;

  @QuerySqlField(index=true)
  private String label1;

  @QuerySqlField(index=true,orderedGroups={@QuerySqlField.Group(
          name = "s_riskType_label2_idx", order = 1)})
  private String label2;

  @QuerySqlField
  private Double amount;

  @QuerySqlField
  private String amountCurrency;

  @QuerySqlField
  private Double amountUSD;

  public Sensitivity(String positionConstituentId, String riskType, String qualifier, String bucket,
      String label1, String label2, Double amount, String amountCurrency, Double amountUSD) {
    this.positionConstituentId = positionConstituentId;
    this.riskType = riskType;
    this.qualifier = qualifier;
    this.bucket = bucket;
    this.label1 = label1;
    this.label2 = label2;
    this.amount = amount;
    this.amountCurrency = amountCurrency;
    this.amountUSD = amountUSD;
  }

  public Sensitivity() {
  }

  public String getPositionConstituentId() {
    return positionConstituentId;
  }

  public void setPositionConstituentId(String positionConstituentId) {
    this.positionConstituentId = positionConstituentId;
  }

  public String getRiskType() {
    return riskType;
  }

  public void setRiskType(String riskType) {
    this.riskType = riskType;
  }

  public String getQualifier() {
    return qualifier;
  }

  public void setQualifier(String qualifier) {
    this.qualifier = qualifier;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getLabel1() {
    return label1;
  }

  public void setLabel1(String label1) {
    this.label1 = label1;
  }

  public String getLabel2() {
    return label2;
  }

  public void setLabel2(String label2) {
    this.label2 = label2;
  }

  public Double getAmount() {
    return amount;
  }

  public void setAmount(Double amount) {
    this.amount = amount;
  }

  public String getAmountCurrency() {
    return amountCurrency;
  }

  public void setAmountCurrency(String amountCurrency) {
    this.amountCurrency = amountCurrency;
  }

  public Double getAmountUSD() {
    return amountUSD;
  }

  public void setAmountUSD(Double amountUSD) {
    this.amountUSD = amountUSD;
  }
}
