select count(*) from [customer].[dim_BusinessPartner];
select count(*) from [commercial].[dim_Contract];
select count(*) from [commercial].[dim_ContractItem];
select count(*) from [customer].[dim_ContractAccount];
select count(*) from [commercial].[dim_BusinessPartnerMarketingAttributes];
select count(*) from [customer].[dim_BusinessPartnerContact];
select count(*) from [commercial].[dim_Premise];
select count(*) from [commercial].[dim_Installation];
select count(*) from [customer_health].[dim_Installation_s];
select count(*) from [dv_commercial].[dim_InstallationHistory];
select count(*) from [commercial].[dim_InstallationRegisterRate];
select count(*) from [commercial].[dim_Device];
select count(*) from [commercial].[dim_InstallationPointOfDelivery];
select count(*) from [commercial].[dim_Pricekey_cr];
select count(*) from [commercial].[dim_ContractItemRateRelationship_cr];
select count(*) from [commercial].[dim_RatePriceRelationship_cr];
select count(*) FROM [commercial].[dim_MyAccount];
select count(*) FROM [customer].[dim_ValueAddedService_cr];
select count(*) FROM [customer].[dim_ConcessionCard];
select count(*) FROM [commercial].[fact_SentInvoice];
select count(*) FROM [commercial].[dim_Invoice];
select count(*) FROM [commercial].[dim_ContractDiscountHistory_cr];
select count(*) FROM [commercial].[fact_ContractDebt];
select count(*) FROM [commercial].[fact_ContractPayment];
select count(*) FROM [commercial].[dim_PaymentDocument];
select count(*) FROM [customer].[dim_HardshipCustomer];
select count(*) FROM [commercial].[fact_ContractDunning];
select count(*) FROM [commercial].[dim_BillSmoothing_cr];
select count(*) FROM [commercial].[dim_PromiseToPay_cr];
select count(*) FROM [commercial].[fact_PromiseToPay];
select count(*) FROM [commercial].[fact_BillSmoothing];
select count(*) FROM [commercial].[fact_ContractOtherCharges];


