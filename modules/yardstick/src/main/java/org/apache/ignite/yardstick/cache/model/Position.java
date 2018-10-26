package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Position {

  @QuerySqlField
  private  String BDRBookID;

  @QuerySqlField
  private  String contractStatusType;

  @QuerySqlField
  private  String involvedPartyID;

  @QuerySqlField
  private  String involvedPartyIDSourceType;

  @QuerySqlField
  private  String positionDate;

  @QuerySqlField
  private  String positionLevelName;

  @QuerySqlField
  private  String positionShortLongCode;

  @QuerySqlField
  private  String positionSourceSystemID;

  @QuerySqlField
  private  String positionType;

  @QuerySqlField
  private  String sourceAccountID;

  @QuerySqlField
  private  String sourceCollateralID;

  @QuerySqlField
  private  String sourceContractVersionNumber;

  @QuerySqlField
  private  String sourceFacilityID;

  @QuerySqlField
  private  String sourceInstrumentID;

  @QuerySqlField
  private  String sourceTradeGroupID;

  @QuerySqlField
  private  String sourceTradeID;

  @QuerySqlField
  private  String sourceTradeLegID;

  @QuerySqlField
  private  String tradeSourceSystemID;

  @QuerySqlField
  private  String underlyingSourceInstrumentID;

  @QuerySqlField
  private  String actualSettlementDate;

  @QuerySqlField
  private  String brokerCDRID;

  @QuerySqlField
  private  String centralCounterpartyCDRID;

  @QuerySqlField
  private  String collateralSecuredFlag;

  @QuerySqlField
  private  String contractCurrency;

  @QuerySqlField
  private  String contractDate;

  @QuerySqlField
  private  String contractGroupID;

  @QuerySqlField
  private  String contractMaturityDate;

  @QuerySqlField
  private  String contractUpdateDate;

  @QuerySqlField
  private  String contractualSettlementDate;

  @QuerySqlField
  private  String counterpartyGlobalBookID;

  @QuerySqlField
  private  String encumberedFlag;

  @QuerySqlField
  private  String hedgeType;

  @QuerySqlField
  private  String LRMProductClassificationID;

  @QuerySqlField
  private  String marketRiskProductClassificationID;

  @QuerySqlField
  private  String nettingAgreementID;

  @QuerySqlField
  private  String payReceiveCode;

  @QuerySqlField(index=true)
  private  String positionDecoratorID;

  @QuerySqlField
  private  String positionDecoratorType;

  @QuerySqlField
  private  String positionQuantity;

  @QuerySqlField
  private  String positionQuantityType;

  @QuerySqlField
  private  String positionSettlementDate;

  @QuerySqlField
  private  String preferredStatusID;

  @QuerySqlField
  private  String preferredTypeID;

  @QuerySqlField
  private  String relatedContractID;

  @QuerySqlField
  private  String relatedContractType;

  @QuerySqlField
  private  String riskCounterpartyCDRID;

  @QuerySqlField
  private  String SEFExecutedFlag;

  @QuerySqlField
  private  String SEFUSI;

  @QuerySqlField
  private  String SEFUSINameSpace;

  @QuerySqlField
  private  String settlementSourceSystemID;

  @QuerySqlField
  private  String settlementStatusType;

  @QuerySqlField
  private  String settlementType;

  @QuerySqlField
  private  String sourceBookID;

  @QuerySqlField(index = true)
  private  String sourceBookName;

  @QuerySqlField
  private  String sourceBookingTransitID;

  @QuerySqlField
  private  String sourceCounterpartyContactPersonID;

  @QuerySqlField
  private  String sourceCounterpartyContactPersonName;

  @QuerySqlField
  private  String sourceCounterpartyShortName;

  @QuerySqlField
  private  String sourceEnteredUserID;

  @QuerySqlField
  private  String sourceEnteredUserLongName;

  @QuerySqlField
  private  String sourceEnteredUserShortName;

  @QuerySqlField
  private  String sourceInvolvedPartyID;

  @QuerySqlField
  private  String sourceProductClassification;

  @QuerySqlField
  private  String sourceRBCPrimaryID;

  @QuerySqlField
  private  String sourceRBCPrimaryLegalEntityID;

  @QuerySqlField
  private  String sourceRBCPrimaryShortName;

  @QuerySqlField
  private  String sourceResponsibilityTransitID;

  @QuerySqlField
  private  String sourceSalesPersonID;

  @QuerySqlField
  private  String sourceSubBookName;

  @QuerySqlField
  private  String sourceTradeDeskDesignationType;

  @QuerySqlField
  private  String sourceTradeLegDescription;

  @QuerySqlField
  private  String sourceTradeMediumType;

  @QuerySqlField
  private  String sourceTraderID;

  @QuerySqlField
  private  String subLedgerProductCode;

  @QuerySqlField
  private  String subLedgerProductSubCode;

  @QuerySqlField
  private  String tradeCapacityType;

  @QuerySqlField
  private  String tradeEnteredDate;

  @QuerySqlField
  private  String tradeExecutionUTCDateTime;

  @QuerySqlField
  private  String tradeGroupRoleTypeID;

  @QuerySqlField
  private  String tradeGroupTradeCount;

  @QuerySqlField
  private  String tradeGroupTradeSequenceNumber;

  @QuerySqlField
  private  String tradeGroupTypeID;

  @QuerySqlField
  private  String tradeLegTypeID;

  @QuerySqlField
  private  String tradeStatusUpdateDateTime;

  @QuerySqlField
  private  String tradeStatusUpdateUTCDateTime;

  @QuerySqlField
  private  String tradeUpdateUTCDateTime;

  @QuerySqlField
  private  String tradeUpdatedDateTime;

  @QuerySqlField
  private  String tradeVerificationDateTime;

  @QuerySqlField
  private  String USI;

  @QuerySqlField
  private  String USINameSpace;

  @QuerySqlField
  private  String sourceFundID;

  @QuerySqlField
  private  String investmentControlType;

  @QuerySqlField
  private  String baselRiskWeightApproachType;

  @QuerySqlField
  private  String nettingAgreementFlag;

  @QuerySqlField
  private  String sourceParentFundID;

  @QuerySqlField
  private  String sourceParentHierarchy;

  @QuerySqlField
  private  String bookingTransitID;

  @QuerySqlField
  private  String responsibilityTransitID;

  @QuerySqlField
  private  String settlementServiceType;

  @QuerySqlField
  private  String sourceParentAccountID;

  @QuerySqlField
  private  String transferPricingTransitID;

  @QuerySqlField
  private  String masterAgreementType;

  @QuerySqlField
  private  String eodcAssetClassType;

  @QuerySqlField
  private  String eodcProductType;

  @QuerySqlField
  private  String eodcProductSubType;

  @QuerySqlField
  private  String OTCTradedFlag;

  @QuerySqlField
  private  String positionConstituentFlag;

  @QuerySqlField
  private  String sourceParentStructureID;

  @QuerySqlField
  private  String underlyingAssetID;

  @QuerySqlField
  private  String underlyingAssetType;

  public Position(String positionDate,
                  String positionLevelName,
                  String  contractCurrency,
                  String BDRBookID,
                  String involvedPartyID,
                  String  positionSourceSystemID,
                  String positionType,
                  String sourceTradeID,
                  String tradeSourceSystemID,
                  String LRMProductClassificationID,
                  String positionDecoratorType,
                  String positionDecoratorID,
                  String sourceBookID,
                  String sourceBookName) {

    this.BDRBookID = BDRBookID;
    this.involvedPartyID = involvedPartyID;
    this.positionDate = positionDate;
    this.positionLevelName = positionLevelName;
    this.positionSourceSystemID = positionSourceSystemID;
    this.positionType = positionType;
    this.sourceTradeID = sourceTradeID;
    this.tradeSourceSystemID = tradeSourceSystemID;
    this.contractCurrency = contractCurrency;
    this.LRMProductClassificationID = LRMProductClassificationID;
    this.positionDecoratorID = positionDecoratorID;
    this.positionDecoratorType = positionDecoratorType;
    this.sourceBookID = sourceBookID;
    this.sourceBookName = sourceBookName;
  }

  public Position() {
  }

  public String getBDRBookID() {
    return BDRBookID;
  }

  public void setBDRBookID(String BDRBookID) {
    this.BDRBookID = BDRBookID;
  }

  public String getContractStatusType() {
    return contractStatusType;
  }

  public void setContractStatusType(String contractStatusType) {
    this.contractStatusType = contractStatusType;
  }

  public String getInvolvedPartyID() {
    return involvedPartyID;
  }

  public void setInvolvedPartyID(String involvedPartyID) {
    this.involvedPartyID = involvedPartyID;
  }

  public String getInvolvedPartyIDSourceType() {
    return involvedPartyIDSourceType;
  }

  public void setInvolvedPartyIDSourceType(String involvedPartyIDSourceType) {
    this.involvedPartyIDSourceType = involvedPartyIDSourceType;
  }

  public String getPositionDate() {
    return positionDate;
  }

  public void setPositionDate(String positionDate) {
    this.positionDate = positionDate;
  }

  public String getPositionLevelName() {
    return positionLevelName;
  }

  public void setPositionLevelName(String positionLevelName) {
    this.positionLevelName = positionLevelName;
  }

  public String getPositionShortLongCode() {
    return positionShortLongCode;
  }

  public void setPositionShortLongCode(String positionShortLongCode) {
    this.positionShortLongCode = positionShortLongCode;
  }

  public String getPositionSourceSystemID() {
    return positionSourceSystemID;
  }

  public void setPositionSourceSystemID(String positionSourceSystemID) {
    this.positionSourceSystemID = positionSourceSystemID;
  }

  public String getPositionType() {
    return positionType;
  }

  public void setPositionType(String positionType) {
    this.positionType = positionType;
  }

  public String getSourceAccountID() {
    return sourceAccountID;
  }

  public void setSourceAccountID(String sourceAccountID) {
    this.sourceAccountID = sourceAccountID;
  }

  public String getSourceCollateralID() {
    return sourceCollateralID;
  }

  public void setSourceCollateralID(String sourceCollateralID) {
    this.sourceCollateralID = sourceCollateralID;
  }

  public String getSourceContractVersionNumber() {
    return sourceContractVersionNumber;
  }

  public void setSourceContractVersionNumber(String sourceContractVersionNumber) {
    this.sourceContractVersionNumber = sourceContractVersionNumber;
  }

  public String getSourceFacilityID() {
    return sourceFacilityID;
  }

  public void setSourceFacilityID(String sourceFacilityID) {
    this.sourceFacilityID = sourceFacilityID;
  }

  public String getSourceInstrumentID() {
    return sourceInstrumentID;
  }

  public void setSourceInstrumentID(String sourceInstrumentID) {
    this.sourceInstrumentID = sourceInstrumentID;
  }

  public String getSourceTradeGroupID() {
    return sourceTradeGroupID;
  }

  public void setSourceTradeGroupID(String sourceTradeGroupID) {
    this.sourceTradeGroupID = sourceTradeGroupID;
  }

  public String getSourceTradeID() {
    return sourceTradeID;
  }

  public void setSourceTradeID(String sourceTradeID) {
    this.sourceTradeID = sourceTradeID;
  }

  public String getSourceTradeLegID() {
    return sourceTradeLegID;
  }

  public void setSourceTradeLegID(String sourceTradeLegID) {
    this.sourceTradeLegID = sourceTradeLegID;
  }

  public String getTradeSourceSystemID() {
    return tradeSourceSystemID;
  }

  public void setTradeSourceSystemID(String tradeSourceSystemID) {
    this.tradeSourceSystemID = tradeSourceSystemID;
  }

  public String getUnderlyingSourceInstrumentID() {
    return underlyingSourceInstrumentID;
  }

  public void setUnderlyingSourceInstrumentID(String underlyingSourceInstrumentID) {
    this.underlyingSourceInstrumentID = underlyingSourceInstrumentID;
  }

  public String getActualSettlementDate() {
    return actualSettlementDate;
  }

  public void setActualSettlementDate(String actualSettlementDate) {
    this.actualSettlementDate = actualSettlementDate;
  }

  public String getBrokerCDRID() {
    return brokerCDRID;
  }

  public void setBrokerCDRID(String brokerCDRID) {
    this.brokerCDRID = brokerCDRID;
  }

  public String getCentralCounterpartyCDRID() {
    return centralCounterpartyCDRID;
  }

  public void setCentralCounterpartyCDRID(String centralCounterpartyCDRID) {
    this.centralCounterpartyCDRID = centralCounterpartyCDRID;
  }

  public String getCollateralSecuredFlag() {
    return collateralSecuredFlag;
  }

  public void setCollateralSecuredFlag(String collateralSecuredFlag) {
    this.collateralSecuredFlag = collateralSecuredFlag;
  }

  public String getContractCurrency() {
    return contractCurrency;
  }

  public void setContractCurrency(String contractCurrency) {
    this.contractCurrency = contractCurrency;
  }

  public String getContractDate() {
    return contractDate;
  }

  public void setContractDate(String contractDate) {
    this.contractDate = contractDate;
  }

  public String getContractGroupID() {
    return contractGroupID;
  }

  public void setContractGroupID(String contractGroupID) {
    this.contractGroupID = contractGroupID;
  }

  public String getContractMaturityDate() {
    return contractMaturityDate;
  }

  public void setContractMaturityDate(String contractMaturityDate) {
    this.contractMaturityDate = contractMaturityDate;
  }

  public String getContractUpdateDate() {
    return contractUpdateDate;
  }

  public void setContractUpdateDate(String contractUpdateDate) {
    this.contractUpdateDate = contractUpdateDate;
  }

  public String getContractualSettlementDate() {
    return contractualSettlementDate;
  }

  public void setContractualSettlementDate(String contractualSettlementDate) {
    this.contractualSettlementDate = contractualSettlementDate;
  }

  public String getCounterpartyGlobalBookID() {
    return counterpartyGlobalBookID;
  }

  public void setCounterpartyGlobalBookID(String counterpartyGlobalBookID) {
    this.counterpartyGlobalBookID = counterpartyGlobalBookID;
  }

  public String getEncumberedFlag() {
    return encumberedFlag;
  }

  public void setEncumberedFlag(String encumberedFlag) {
    this.encumberedFlag = encumberedFlag;
  }

  public String getHedgeType() {
    return hedgeType;
  }

  public void setHedgeType(String hedgeType) {
    this.hedgeType = hedgeType;
  }

  public String getLRMProductClassificationID() {
    return LRMProductClassificationID;
  }

  public void setLRMProductClassificationID(String LRMProductClassificationID) {
    this.LRMProductClassificationID = LRMProductClassificationID;
  }

  public String getMarketRiskProductClassificationID() {
    return marketRiskProductClassificationID;
  }

  public void setMarketRiskProductClassificationID(String marketRiskProductClassificationID) {
    this.marketRiskProductClassificationID = marketRiskProductClassificationID;
  }

  public String getNettingAgreementID() {
    return nettingAgreementID;
  }

  public void setNettingAgreementID(String nettingAgreementID) {
    this.nettingAgreementID = nettingAgreementID;
  }

  public String getPayReceiveCode() {
    return payReceiveCode;
  }

  public void setPayReceiveCode(String payReceiveCode) {
    this.payReceiveCode = payReceiveCode;
  }

  public String getPositionDecoratorID() {
    return positionDecoratorID;
  }

  public void setPositionDecoratorID(String positionDecoratorID) {
    this.positionDecoratorID = positionDecoratorID;
  }

  public String getPositionDecoratorType() {
    return positionDecoratorType;
  }

  public void setPositionDecoratorType(String positionDecoratorType) {
    this.positionDecoratorType = positionDecoratorType;
  }

  public String getPositionQuantity() {
    return positionQuantity;
  }

  public void setPositionQuantity(String positionQuantity) {
    this.positionQuantity = positionQuantity;
  }

  public String getPositionQuantityType() {
    return positionQuantityType;
  }

  public void setPositionQuantityType(String positionQuantityType) {
    this.positionQuantityType = positionQuantityType;
  }

  public String getPositionSettlementDate() {
    return positionSettlementDate;
  }

  public void setPositionSettlementDate(String positionSettlementDate) {
    this.positionSettlementDate = positionSettlementDate;
  }

  public String getPreferredStatusID() {
    return preferredStatusID;
  }

  public void setPreferredStatusID(String preferredStatusID) {
    this.preferredStatusID = preferredStatusID;
  }

  public String getPreferredTypeID() {
    return preferredTypeID;
  }

  public void setPreferredTypeID(String preferredTypeID) {
    this.preferredTypeID = preferredTypeID;
  }

  public String getRelatedContractID() {
    return relatedContractID;
  }

  public void setRelatedContractID(String relatedContractID) {
    this.relatedContractID = relatedContractID;
  }

  public String getRelatedContractType() {
    return relatedContractType;
  }

  public void setRelatedContractType(String relatedContractType) {
    this.relatedContractType = relatedContractType;
  }

  public String getRiskCounterpartyCDRID() {
    return riskCounterpartyCDRID;
  }

  public void setRiskCounterpartyCDRID(String riskCounterpartyCDRID) {
    this.riskCounterpartyCDRID = riskCounterpartyCDRID;
  }

  public String getSEFExecutedFlag() {
    return SEFExecutedFlag;
  }

  public void setSEFExecutedFlag(String SEFExecutedFlag) {
    this.SEFExecutedFlag = SEFExecutedFlag;
  }

  public String getSEFUSI() {
    return SEFUSI;
  }

  public void setSEFUSI(String SEFUSI) {
    this.SEFUSI = SEFUSI;
  }

  public String getSEFUSINameSpace() {
    return SEFUSINameSpace;
  }

  public void setSEFUSINameSpace(String SEFUSINameSpace) {
    this.SEFUSINameSpace = SEFUSINameSpace;
  }

  public String getSettlementSourceSystemID() {
    return settlementSourceSystemID;
  }

  public void setSettlementSourceSystemID(String settlementSourceSystemID) {
    this.settlementSourceSystemID = settlementSourceSystemID;
  }

  public String getSettlementStatusType() {
    return settlementStatusType;
  }

  public void setSettlementStatusType(String settlementStatusType) {
    this.settlementStatusType = settlementStatusType;
  }

  public String getSettlementType() {
    return settlementType;
  }

  public void setSettlementType(String settlementType) {
    this.settlementType = settlementType;
  }

  public String getSourceBookID() {
    return sourceBookID;
  }

  public void setSourceBookID(String sourceBookID) {
    this.sourceBookID = sourceBookID;
  }

  public String getSourceBookName() {
    return sourceBookName;
  }

  public void setSourceBookName(String sourceBookName) {
    this.sourceBookName = sourceBookName;
  }

  public String getSourceBookingTransitID() {
    return sourceBookingTransitID;
  }

  public void setSourceBookingTransitID(String sourceBookingTransitID) {
    this.sourceBookingTransitID = sourceBookingTransitID;
  }

  public String getSourceCounterpartyContactPersonID() {
    return sourceCounterpartyContactPersonID;
  }

  public void setSourceCounterpartyContactPersonID(String sourceCounterpartyContactPersonID) {
    this.sourceCounterpartyContactPersonID = sourceCounterpartyContactPersonID;
  }

  public String getSourceCounterpartyContactPersonName() {
    return sourceCounterpartyContactPersonName;
  }

  public void setSourceCounterpartyContactPersonName(String sourceCounterpartyContactPersonName) {
    this.sourceCounterpartyContactPersonName = sourceCounterpartyContactPersonName;
  }

  public String getSourceCounterpartyShortName() {
    return sourceCounterpartyShortName;
  }

  public void setSourceCounterpartyShortName(String sourceCounterpartyShortName) {
    this.sourceCounterpartyShortName = sourceCounterpartyShortName;
  }

  public String getSourceEnteredUserID() {
    return sourceEnteredUserID;
  }

  public void setSourceEnteredUserID(String sourceEnteredUserID) {
    this.sourceEnteredUserID = sourceEnteredUserID;
  }

  public String getSourceEnteredUserLongName() {
    return sourceEnteredUserLongName;
  }

  public void setSourceEnteredUserLongName(String sourceEnteredUserLongName) {
    this.sourceEnteredUserLongName = sourceEnteredUserLongName;
  }

  public String getSourceEnteredUserShortName() {
    return sourceEnteredUserShortName;
  }

  public void setSourceEnteredUserShortName(String sourceEnteredUserShortName) {
    this.sourceEnteredUserShortName = sourceEnteredUserShortName;
  }

  public String getSourceInvolvedPartyID() {
    return sourceInvolvedPartyID;
  }

  public void setSourceInvolvedPartyID(String sourceInvolvedPartyID) {
    this.sourceInvolvedPartyID = sourceInvolvedPartyID;
  }

  public String getSourceProductClassification() {
    return sourceProductClassification;
  }

  public void setSourceProductClassification(String sourceProductClassification) {
    this.sourceProductClassification = sourceProductClassification;
  }

  public String getSourceRBCPrimaryID() {
    return sourceRBCPrimaryID;
  }

  public void setSourceRBCPrimaryID(String sourceRBCPrimaryID) {
    this.sourceRBCPrimaryID = sourceRBCPrimaryID;
  }

  public String getSourceRBCPrimaryLegalEntityID() {
    return sourceRBCPrimaryLegalEntityID;
  }

  public void setSourceRBCPrimaryLegalEntityID(String sourceRBCPrimaryLegalEntityID) {
    this.sourceRBCPrimaryLegalEntityID = sourceRBCPrimaryLegalEntityID;
  }

  public String getSourceRBCPrimaryShortName() {
    return sourceRBCPrimaryShortName;
  }

  public void setSourceRBCPrimaryShortName(String sourceRBCPrimaryShortName) {
    this.sourceRBCPrimaryShortName = sourceRBCPrimaryShortName;
  }

  public String getSourceResponsibilityTransitID() {
    return sourceResponsibilityTransitID;
  }

  public void setSourceResponsibilityTransitID(String sourceResponsibilityTransitID) {
    this.sourceResponsibilityTransitID = sourceResponsibilityTransitID;
  }

  public String getSourceSalesPersonID() {
    return sourceSalesPersonID;
  }

  public void setSourceSalesPersonID(String sourceSalesPersonID) {
    this.sourceSalesPersonID = sourceSalesPersonID;
  }

  public String getSourceSubBookName() {
    return sourceSubBookName;
  }

  public void setSourceSubBookName(String sourceSubBookName) {
    this.sourceSubBookName = sourceSubBookName;
  }

  public String getSourceTradeDeskDesignationType() {
    return sourceTradeDeskDesignationType;
  }

  public void setSourceTradeDeskDesignationType(String sourceTradeDeskDesignationType) {
    this.sourceTradeDeskDesignationType = sourceTradeDeskDesignationType;
  }

  public String getSourceTradeLegDescription() {
    return sourceTradeLegDescription;
  }

  public void setSourceTradeLegDescription(String sourceTradeLegDescription) {
    this.sourceTradeLegDescription = sourceTradeLegDescription;
  }

  public String getSourceTradeMediumType() {
    return sourceTradeMediumType;
  }

  public void setSourceTradeMediumType(String sourceTradeMediumType) {
    this.sourceTradeMediumType = sourceTradeMediumType;
  }

  public String getSourceTraderID() {
    return sourceTraderID;
  }

  public void setSourceTraderID(String sourceTraderID) {
    this.sourceTraderID = sourceTraderID;
  }

  public String getSubLedgerProductCode() {
    return subLedgerProductCode;
  }

  public void setSubLedgerProductCode(String subLedgerProductCode) {
    this.subLedgerProductCode = subLedgerProductCode;
  }

  public String getSubLedgerProductSubCode() {
    return subLedgerProductSubCode;
  }

  public void setSubLedgerProductSubCode(String subLedgerProductSubCode) {
    this.subLedgerProductSubCode = subLedgerProductSubCode;
  }

  public String getTradeCapacityType() {
    return tradeCapacityType;
  }

  public void setTradeCapacityType(String tradeCapacityType) {
    this.tradeCapacityType = tradeCapacityType;
  }

  public String getTradeEnteredDate() {
    return tradeEnteredDate;
  }

  public void setTradeEnteredDate(String tradeEnteredDate) {
    this.tradeEnteredDate = tradeEnteredDate;
  }

  public String getTradeExecutionUTCDateTime() {
    return tradeExecutionUTCDateTime;
  }

  public void setTradeExecutionUTCDateTime(String tradeExecutionUTCDateTime) {
    this.tradeExecutionUTCDateTime = tradeExecutionUTCDateTime;
  }

  public String getTradeGroupRoleTypeID() {
    return tradeGroupRoleTypeID;
  }

  public void setTradeGroupRoleTypeID(String tradeGroupRoleTypeID) {
    this.tradeGroupRoleTypeID = tradeGroupRoleTypeID;
  }

  public String getTradeGroupTradeCount() {
    return tradeGroupTradeCount;
  }

  public void setTradeGroupTradeCount(String tradeGroupTradeCount) {
    this.tradeGroupTradeCount = tradeGroupTradeCount;
  }

  public String getTradeGroupTradeSequenceNumber() {
    return tradeGroupTradeSequenceNumber;
  }

  public void setTradeGroupTradeSequenceNumber(String tradeGroupTradeSequenceNumber) {
    this.tradeGroupTradeSequenceNumber = tradeGroupTradeSequenceNumber;
  }

  public String getTradeGroupTypeID() {
    return tradeGroupTypeID;
  }

  public void setTradeGroupTypeID(String tradeGroupTypeID) {
    this.tradeGroupTypeID = tradeGroupTypeID;
  }

  public String getTradeLegTypeID() {
    return tradeLegTypeID;
  }

  public void setTradeLegTypeID(String tradeLegTypeID) {
    this.tradeLegTypeID = tradeLegTypeID;
  }

  public String getTradeStatusUpdateDateTime() {
    return tradeStatusUpdateDateTime;
  }

  public void setTradeStatusUpdateDateTime(String tradeStatusUpdateDateTime) {
    this.tradeStatusUpdateDateTime = tradeStatusUpdateDateTime;
  }

  public String getTradeStatusUpdateUTCDateTime() {
    return tradeStatusUpdateUTCDateTime;
  }

  public void setTradeStatusUpdateUTCDateTime(String tradeStatusUpdateUTCDateTime) {
    this.tradeStatusUpdateUTCDateTime = tradeStatusUpdateUTCDateTime;
  }

  public String getTradeUpdateUTCDateTime() {
    return tradeUpdateUTCDateTime;
  }

  public void setTradeUpdateUTCDateTime(String tradeUpdateUTCDateTime) {
    this.tradeUpdateUTCDateTime = tradeUpdateUTCDateTime;
  }

  public String getTradeUpdatedDateTime() {
    return tradeUpdatedDateTime;
  }

  public void setTradeUpdatedDateTime(String tradeUpdatedDateTime) {
    this.tradeUpdatedDateTime = tradeUpdatedDateTime;
  }

  public String getTradeVerificationDateTime() {
    return tradeVerificationDateTime;
  }

  public void setTradeVerificationDateTime(String tradeVerificationDateTime) {
    this.tradeVerificationDateTime = tradeVerificationDateTime;
  }

  public String getUSI() {
    return USI;
  }

  public void setUSI(String USI) {
    this.USI = USI;
  }

  public String getUSINameSpace() {
    return USINameSpace;
  }

  public void setUSINameSpace(String USINameSpace) {
    this.USINameSpace = USINameSpace;
  }

  public String getSourceFundID() {
    return sourceFundID;
  }

  public void setSourceFundID(String sourceFundID) {
    this.sourceFundID = sourceFundID;
  }

  public String getInvestmentControlType() {
    return investmentControlType;
  }

  public void setInvestmentControlType(String investmentControlType) {
    this.investmentControlType = investmentControlType;
  }

  public String getBaselRiskWeightApproachType() {
    return baselRiskWeightApproachType;
  }

  public void setBaselRiskWeightApproachType(String baselRiskWeightApproachType) {
    this.baselRiskWeightApproachType = baselRiskWeightApproachType;
  }

  public String getNettingAgreementFlag() {
    return nettingAgreementFlag;
  }

  public void setNettingAgreementFlag(String nettingAgreementFlag) {
    this.nettingAgreementFlag = nettingAgreementFlag;
  }

  public String getSourceParentFundID() {
    return sourceParentFundID;
  }

  public void setSourceParentFundID(String sourceParentFundID) {
    this.sourceParentFundID = sourceParentFundID;
  }

  public String getSourceParentHierarchy() {
    return sourceParentHierarchy;
  }

  public void setSourceParentHierarchy(String sourceParentHierarchy) {
    this.sourceParentHierarchy = sourceParentHierarchy;
  }

  public String getBookingTransitID() {
    return bookingTransitID;
  }

  public void setBookingTransitID(String bookingTransitID) {
    this.bookingTransitID = bookingTransitID;
  }

  public String getResponsibilityTransitID() {
    return responsibilityTransitID;
  }

  public void setResponsibilityTransitID(String responsibilityTransitID) {
    this.responsibilityTransitID = responsibilityTransitID;
  }

  public String getSettlementServiceType() {
    return settlementServiceType;
  }

  public void setSettlementServiceType(String settlementServiceType) {
    this.settlementServiceType = settlementServiceType;
  }

  public String getSourceParentAccountID() {
    return sourceParentAccountID;
  }

  public void setSourceParentAccountID(String sourceParentAccountID) {
    this.sourceParentAccountID = sourceParentAccountID;
  }

  public String getTransferPricingTransitID() {
    return transferPricingTransitID;
  }

  public void setTransferPricingTransitID(String transferPricingTransitID) {
    this.transferPricingTransitID = transferPricingTransitID;
  }

  public String getMasterAgreementType() {
    return masterAgreementType;
  }

  public void setMasterAgreementType(String masterAgreementType) {
    this.masterAgreementType = masterAgreementType;
  }

  public String getEodcAssetClassType() {
    return eodcAssetClassType;
  }

  public void setEodcAssetClassType(String eodcAssetClassType) {
    this.eodcAssetClassType = eodcAssetClassType;
  }

  public String getEodcProductType() {
    return eodcProductType;
  }

  public void setEodcProductType(String eodcProductType) {
    this.eodcProductType = eodcProductType;
  }

  public String getEodcProductSubType() {
    return eodcProductSubType;
  }

  public void setEodcProductSubType(String eodcProductSubType) {
    this.eodcProductSubType = eodcProductSubType;
  }

  public String getOTCTradedFlag() {
    return OTCTradedFlag;
  }

  public void setOTCTradedFlag(String OTCTradedFlag) {
    this.OTCTradedFlag = OTCTradedFlag;
  }

  public String getPositionConstituentFlag() {
    return positionConstituentFlag;
  }

  public void setPositionConstituentFlag(String positionConstituentFlag) {
    this.positionConstituentFlag = positionConstituentFlag;
  }

  public String getSourceParentStructureID() {
    return sourceParentStructureID;
  }

  public void setSourceParentStructureID(String sourceParentStructureID) {
    this.sourceParentStructureID = sourceParentStructureID;
  }

  public String getUnderlyingAssetID() {
    return underlyingAssetID;
  }

  public void setUnderlyingAssetID(String underlyingAssetID) {
    this.underlyingAssetID = underlyingAssetID;
  }

  public String getUnderlyingAssetType() {
    return underlyingAssetType;
  }

  public void setUnderlyingAssetType(String underlyingAssetType) {
    this.underlyingAssetType = underlyingAssetType;
  }
}
