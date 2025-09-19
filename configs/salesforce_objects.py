# configs/salesforce_objects.py
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass(frozen=True)
class ObjectSpec:
    api_name: str                  # Salesforce API name
    fields: List[str]              # SELECT field list for SOQL
    required_cols: List[str]       # minimal columns expected to exist
    group_by: List[str]            # columns to group by in process step
    metrics: Dict[str, List[str]]  # numeric columns -> ["sum","mean","min","max","count"]
    where: Optional[str] = ""      # optional WHERE (keep blank to be portable)

# Keep required_cols minimal so the pipeline works across orgs.
OBJECT_SPECS: Dict[str, ObjectSpec] = {
    # ---- Core CRM ----
    "Account": ObjectSpec(
        api_name="Account",
        fields=["Id","Name","Phone","Website","BillingCity","BillingState","Industry","AnnualRevenue"],
        required_cols=["Id","Name"],
        group_by=["BillingState"],
        metrics={"AnnualRevenue": ["sum","mean","count"]},
    ),
    "Contact": ObjectSpec(
        api_name="Contact",
        fields=["Id","FirstName","LastName","Email","Phone","MailingCity","MailingState","AccountId"],
        required_cols=["Id","LastName"],
        group_by=["MailingState"],
        metrics={"Id": ["count"]},
    ),
    "Lead": ObjectSpec(
        api_name="Lead",
        fields=["Id","FirstName","LastName","Company","Status","LeadSource","Email","Phone","City","State"],
        required_cols=["Id","Status"],
        group_by=["Status","LeadSource"],
        metrics={"Id": ["count"]},
    ),
    "Opportunity": ObjectSpec(
        api_name="Opportunity",
        fields=["Id","Name","StageName","Amount","CloseDate","OwnerId","AccountId","Type"],
        required_cols=["Id","StageName"],
        group_by=["StageName"],
        metrics={"Amount": ["sum","mean","count"]},
    ),
    "OpportunityLineItem": ObjectSpec(
        api_name="OpportunityLineItem",
        fields=["Id","OpportunityId","Product2Id","Quantity","UnitPrice","TotalPrice"],
        required_cols=["Id","OpportunityId"],
        group_by=["Product2Id"],
        metrics={"Quantity": ["sum"], "UnitPrice": ["mean"], "TotalPrice": ["sum","count"]},
    ),
    "Case": ObjectSpec(
        api_name="Case",
        fields=["Id","CaseNumber","Status","Priority","Origin","AccountId","ContactId","OwnerId"],
        required_cols=["Id","Status"],
        group_by=["Status","Priority"],
        metrics={"Id": ["count"]},
    ),
    "Task": ObjectSpec(
        api_name="Task",
        fields=["Id","Subject","Status","Priority","OwnerId","WhatId","WhoId","ActivityDate"],
        required_cols=["Id"],
        group_by=["Status","Priority"],
        metrics={"Id": ["count"]},
    ),
    "Event": ObjectSpec(
        api_name="Event",
        fields=["Id","Subject","StartDateTime","EndDateTime","OwnerId","WhatId","Location"],
        required_cols=["Id"],
        group_by=["OwnerId"],
        metrics={"__DURATION_HOURS__": ["sum","mean","count"]},  # computed in process
    ),
    "Campaign": ObjectSpec(
        api_name="Campaign",
        fields=["Id","Name","Status","Type","StartDate","EndDate","BudgetedCost","ActualCost"],
        required_cols=["Id","Name"],
        group_by=["Status","Type"],
        metrics={"BudgetedCost": ["sum"], "ActualCost": ["sum","count"]},
    ),
    "CampaignMember": ObjectSpec(
        api_name="CampaignMember",
        fields=["Id","CampaignId","ContactId","LeadId","Status"],
        required_cols=["Id","CampaignId"],
        group_by=["Status"],
        metrics={"Id": ["count"]},
    ),

    # ---- Catalog / Pricing ----
    "Product2": ObjectSpec(
        api_name="Product2",
        fields=["Id","Name","ProductCode","Family","IsActive"],
        required_cols=["Id","Name"],
        group_by=["Family","IsActive"],
        metrics={"Id": ["count"]},
    ),
    "Pricebook2": ObjectSpec(
        api_name="Pricebook2",
        fields=["Id","Name","IsActive"],
        required_cols=["Id","Name"],
        group_by=["IsActive"],
        metrics={"Id": ["count"]},
    ),
    "PricebookEntry": ObjectSpec(
        api_name="PricebookEntry",
        fields=["Id","Pricebook2Id","Product2Id","UnitPrice","IsActive","UseStandardPrice"],
        required_cols=["Id","Product2Id"],
        group_by=["Pricebook2Id","IsActive"],
        metrics={"UnitPrice": ["mean","count"]},
    ),

    # ---- Orders / Quotes / Contracts / Assets ----
    "Order": ObjectSpec(
        api_name="Order",
        fields=["Id","AccountId","Status","TotalAmount","EffectiveDate","OwnerId"],
        required_cols=["Id","Status"],
        group_by=["Status"],
        metrics={"TotalAmount": ["sum","mean","count"]},
    ),
    "OrderItem": ObjectSpec(
        api_name="OrderItem",
        fields=["Id","OrderId","Product2Id","Quantity","UnitPrice","TotalPrice"],
        required_cols=["Id","OrderId"],
        group_by=["Product2Id"],
        metrics={"Quantity": ["sum"], "UnitPrice": ["mean"], "TotalPrice": ["sum","count"]},
    ),
    "Quote": ObjectSpec(
        api_name="Quote",
        fields=["Id","Name","OpportunityId","Status","GrandTotal","ExpirationDate"],
        required_cols=["Id","Status"],
        group_by=["Status"],
        metrics={"GrandTotal": ["sum","mean","count"]},
    ),
    "QuoteLineItem": ObjectSpec(
        api_name="QuoteLineItem",
        fields=["Id","QuoteId","Product2Id","Quantity","UnitPrice","TotalPrice"],
        required_cols=["Id","QuoteId"],
        group_by=["Product2Id"],
        metrics={"Quantity": ["sum"], "UnitPrice": ["mean"], "TotalPrice": ["sum","count"]},
    ),
    "Contract": ObjectSpec(
        api_name="Contract",
        fields=["Id","AccountId","Status","StartDate","EndDate","OwnerId"],
        required_cols=["Id","Status"],
        group_by=["Status"],
        metrics={"Id": ["count"]},
    ),
    "Asset": ObjectSpec(
        api_name="Asset",
        fields=["Id","AccountId","ContactId","Product2Id","Status","InstallDate"],
        required_cols=["Id","Status"],
        group_by=["Status"],
        metrics={"Id": ["count"]},
    ),

    # ---- Files / Notes ----
    "Note": ObjectSpec(
        api_name="Note",
        fields=["Id","Title","IsPrivate","OwnerId","ParentId"],
        required_cols=["Id","Title"],
        group_by=["IsPrivate"],
        metrics={"Id": ["count"]},
    ),
    "ContentDocument": ObjectSpec(
        api_name="ContentDocument",
        fields=["Id","Title","FileType","LatestPublishedVersionId","OwnerId"],
        required_cols=["Id","Title"],
        group_by=["FileType"],
        metrics={"Id": ["count"]},
    ),
    "ContentVersion": ObjectSpec(
        api_name="ContentVersion",
        fields=["Id","Title","FileExtension","VersionDataSize","ContentDocumentId"],
        required_cols=["Id","Title"],
        group_by=["FileExtension"],
        metrics={"VersionDataSize": ["sum","mean","count"]},
    ),

    # ---- Admin ----
    "User": ObjectSpec(
        api_name="User",
        fields=["Id","Name","Username","Email","IsActive","Alias","TimeZoneSidKey"],
        required_cols=["Id","Name"],
        group_by=["IsActive","TimeZoneSidKey"],
        metrics={"Id": ["count"]},
    ),

    # Example placeholder for a CUSTOM OBJECT (client can add more):
    # "MyObject__c": ObjectSpec(
    #     api_name="MyObject__c",
    #     fields=["Id","Name","CustomField__c","Amount__c","Status__c"],
    #     required_cols=["Id","Name"],
    #     group_by=["Status__c"],
    #     metrics={"Amount__c": ["sum","mean","count"]},
    # ),
}
