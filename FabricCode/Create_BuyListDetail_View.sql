-- =============================================================================
-- CREATE VIEW scut.vw_BuyList_DetailInfo
-- VMI Analytics Lakehouse — scut schema
-- =============================================================================
--
-- PURPOSE:
--     Live shortcut view replicating dbo.vw_BuyList_DetailInfo from the ERP
--     Fabric Data Warehouse. References scut schema shortcut tables — always
--     reflects current ERP data at query time, no snapshot or copy.
--
-- SOURCE VIEW DEFINITION:
--     Obtained via sys.sql_modules from ERP endpoint, May 2026.
--     Original view created by Jeremy Simpson (jsimpson@sazerac.com).
--     BuyListDetailKey added by Jeremy to link to Orders_Base.
--
-- BASE TABLES (all in scut schema as Fabric shortcuts):
--     scut.Buylist_Item       bi   — BuyList item records (535K rows)
--     scut.Buylist_Header     bh   — BuyList header records (2,123 rows)
--     scut.ItemMaster         im   — Item master (112K rows)
--     scut.Buylist_Customer   bc   — BuyList customer relationships (3,948 rows)
--     scut.CustomerMaster     cm   — Customer master (37K rows)
--
-- JOIN STRUCTURE:
--     Buylist_Item INNER JOIN Buylist_Header ON BUYLISTKEY
--     Buylist_Item LEFT JOIN ItemMaster ON ITEMKEY = ItemKey
--     Buylist_Item LEFT JOIN Buylist_Customer ON BUYLISTKEY
--     Buylist_Customer LEFT JOIN CustomerMaster ON CUSTOMERKEY = CustomerKey
--
-- ROW COUNT NOTE:
--     Row count (~942K) exceeds Buylist_Item (535K) because Buylist_Customer
--     fans out one row per customer per BuyList. One BuyList with multiple
--     delivery customers produces multiple rows per item.
--
-- COLUMN SCOPE:
--     This view includes the VMI-relevant column subset identified by the
--     VMI & Inventory Planning team. The full ERP view has 193 columns.
--     Audit/modified-by columns and surrogate keys are intentionally excluded.
--     Two columns added beyond the original team selection:
--         BuylistItemComplianceStatus — state compliance approval status
--         ItemType                    — Finished Goods / Raw Material etc.
--
-- HOW TO RUN:
--     1. Open the VMI_Analytics_Lakehouse SQL analytics endpoint in Fabric
--     2. Open a new query
--     3. Run this entire script
--     4. View will appear under scut schema in the object explorer
--
-- HOW TO QUERY:
--     SQL:   SELECT * FROM scut.vw_BuyList_DetailInfo
--     Spark: df = spark.table("scut.vw_BuyList_DetailInfo")
--
-- =============================================================================

CREATE VIEW scut.vw_BuyList_DetailInfo AS

SELECT

    -- -------------------------------------------------------------------------
    -- BUYLIST ITEM  (source: scut.Buylist_Item bi)
    -- -------------------------------------------------------------------------
    bi.COMPANY_CODE                     AS Company,
    bi.BUYING_LIST_CODE                 AS BuyListCode,
    bi.ITEM_CODE                        AS ItemCode,
    bi.STATE_ITEM_REFERENCE_NUMBER      AS StateReferenceNumber,
    -- NOTE: StateReferenceNumber is the distributor-facing SKU / product
    -- identifier despite the name suggesting a state code. This is the
    -- field used for the SKU crosswalk join in the VMI pipeline.
    -- Do NOT confuse with CustomerCrossReference — they are separate fields.
    bi.CUSTOMER_CROSS_REFERENCE         AS CustomerCrossReference,
    bi.COMMERCIAL_BU                    AS CommercialBu,
    bi.NABCA_STATE_ITEM_NUMBER          AS NABCA_ItemNumber,
    bi.[STATUS]                         AS BuylistItemStatus,
    bi.ORDER_TYPE                       AS BuylistItemOrderType,
    bi.SHIP_POINT1                      AS ShipPoint1,
    bi.[START_DATE]                     AS BuylistItemStartDate,
    bi.END_DATE                         AS BuylistItemEndDate,
    bi.REGISTRATION_KEY                 AS RegistrationKey,
    bi.COMPLIANCE_STATUS                AS BuylistItemComplianceStatus,

    -- -------------------------------------------------------------------------
    -- BUYLIST HEADER  (source: scut.Buylist_Header bh)
    -- -------------------------------------------------------------------------
    bh.BUYING_LIST_DESCRIPTION          AS BuyingListDescription,
    bh.[STATE]                          AS [State],
    bh.FROM_DATE                        AS BuylistHeaderFromDate,
    bh.TO_DATE                          AS BuylistToDate,
    bh.[STATUS]                         AS BuyListHeaderStatus,

    -- -------------------------------------------------------------------------
    -- ITEM MASTER  (source: scut.ItemMaster im)
    -- -------------------------------------------------------------------------
    im.TransitionSKU,
    im.SKUDescription,
    im.ItemType,
    im.BottleSize,
    im.BottleSizeinML,
    im.StandardSizeDescription,
    im.SizeCode,
    im.SizeCodeDescription,
    im.BrandDescription,
    im.BrandOwnership,
    im.SubBrandDescription,
    im.PriceTierDescription,
    im.PackageType,
    im.PackageTypeDescription,
    im.StatusFlag                       AS ItemMasterStatusFlag,
    im.StatusFlagDescription            AS ItemMasterStatusFlagDescription,
    im.CoPackInd,
    im.ItemGroupMinorDescription,
    im.SCCCode,
    im.UPCCode,
    im.FullCost,
    im.Litreage_9L,
    im.[Subdivision/SupplierDescription],
    im.ABAItemType,
    im.WeightperStandardUnit,
    im.StandardCaseSize,
    im.BottlesPerCase,

    -- -------------------------------------------------------------------------
    -- BUYLIST CUSTOMER  (source: scut.Buylist_Customer bc)
    -- -------------------------------------------------------------------------
    bc.[STATUS]                         AS BuylistCustomerStatus,
    bc.CUSTOMER                         AS Customer,
    bc.DELIVERY_ADDRESS_CODE            AS CustomerDeliveryAddressCode,

    -- -------------------------------------------------------------------------
    -- CUSTOMER MASTER  (source: scut.CustomerMaster cm)
    -- -------------------------------------------------------------------------
    cm.CustomerFullName,
    cm.CustomerName,
    cm.CustomerStatus,
    cm.VMIIndicator,
    cm.SubChannel,
    cm.DepletionCode,
    cm.DeliveryType,
    cm.InvoiceAddressCode,
    cm.BuyingListCode,
    cm.LeadTimeInDays,
    cm.DepletionCustomerNumber,
    cm.DepletionDelCode,

    -- -------------------------------------------------------------------------
    -- COMPUTED  (matches ERP view definition exactly)
    -- -------------------------------------------------------------------------
    CONCAT(
        bi.ITEM_CODE,
        bc.CUSTOMER,
        bi.COMPANY_CODE,
        bc.DELIVERY_ADDRESS_CODE,
        bi.SHIP_POINT1
    )                                   AS BuyListDetailKey
    -- BuyListDetailKey joins to vw_Orders_Base on the same computed key.
    -- Format: ItemCode + Customer + Company + DeliveryAddressCode + ShipPoint1

FROM scut.Buylist_Item bi

INNER JOIN scut.Buylist_Header bh
    ON bi.BUYLISTKEY = bh.BUYLISTKEY

LEFT JOIN scut.ItemMaster im
    ON bi.ITEMKEY = im.ItemKey

LEFT JOIN scut.Buylist_Customer bc
    ON bi.BUYLISTKEY = bc.BUYLISTKEY

LEFT JOIN scut.CustomerMaster cm
    ON bc.CUSTOMERKEY = cm.CustomerKey

-- =============================================================================
-- END OF FILE
-- =============================================================================
