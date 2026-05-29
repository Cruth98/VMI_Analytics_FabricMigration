-- =============================================================================
-- CREATE VIEW scut.vw_Orders_Base
-- VMI Analytics Lakehouse — scut schema
-- =============================================================================
--
-- PURPOSE:
--     Live shortcut view replicating dbo.vw_Orders_Base from the ERP Fabric
--     Data Warehouse. References scut schema shortcut tables — always reflects
--     current ERP data at query time, no snapshot or copy.
--
-- SOURCE VIEW DEFINITION:
--     Obtained via sys.sql_modules from ERP endpoint, May 2026.
--     Original view created by Jeremy Simpson (jsimpson@sazerac.com).
--
-- BASE TABLES (all in scut schema as Fabric shortcuts):
--     scut.OracleSCMDistributedOrderOrchestrationLineExtract       L
--     scut.OracleSCMDistributedOrderOrchestrationHeaderExtract     H
--     scut.OracleSCMDistributedOrderOrchestrationFulfillLineExtract   (CTE)
--     scut.OracleSCMDistributedOrderOrchestrationOrderAddressExtract  O
--     scut.OracleCommonPartySite                                   OCPS
--     scut.OracleCommonCustomerAccount                             A
--     scut.OracleCommonPartyExtract                                C
--     scut.OracleCommonItemExtract                                 I
--     scut.OracleSCMCommonInvOrgParametersExtract                  N
--     scut.OracleCommonOrganizationUnit                            COU
--     scut.OracleCustomSalesOrdersFullFillLineEFF                  X
--     scut.OracleCustomSubInventoryDetails                         d
--
-- KEY DESIGN NOTES:
--     - LatestFulfillLine CTE deduplicates fulfill lines using ROW_NUMBER()
--       partitioned by source order + line number, ordered by last update
--       date descending. Keeps only the most recent fulfill line per order
--       line. This is what makes vw_Orders_Base safe for quantity aggregation.
--     - Draft and reference orders excluded via HeaderStatusCode filter.
--     - DeliveryAddressCode extracted from PARTYSITENUMBER via SUBSTRING/
--       CHARINDEX logic — middle segment of hyphen-delimited party site number.
--     - OrdersBaseKey is the computed join key to vw_BuyList_DetailInfo:
--       SKU + CustomerNumber + Company + DeliveryAddressCode + OrganizationCode
--     - Commented-out WHERE clause retained from source — do not uncomment
--       here; apply open-orders filter in consuming notebooks/queries.
--
-- CRITICAL USAGE NOTE:
--     Always filter WHERE SKU != 'PALLETSLIPCHG' for any inventory or
--     quantity-based analysis. Charge lines appear alongside product lines
--     and return NULL for Company, Plant, Stockroom and OrganizationCode.
--
-- HOW TO RUN:
--     1. Open VMI_Analytics_Lakehouse SQL analytics endpoint in Fabric
--     2. Open a new query
--     3. Run this entire script
--     4. View will appear under scut schema in the object explorer
--
-- HOW TO QUERY:
--     SQL:   SELECT * FROM scut.vw_Orders_Base
--     Spark: df = spark.table("scut.vw_Orders_Base")
--
-- =============================================================================

CREATE VIEW scut.vw_Orders_Base AS

WITH LatestFulfillLine AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY FULFILLLINESOURCEORDERNUMBER,
                             FULFILLLINESOURCELINENUMBER
                ORDER BY FULFILLLINELASTUPDATEDATE DESC
            ) AS LFL
        FROM scut.OracleSCMDistributedOrderOrchestrationFulfillLineExtract
    ) AS F
    WHERE LFL = 1
)

SELECT
    H.HEADERCUSTOMERPONUMBER                                AS PONumber,
    H.HEADERORDERNUMBER                                     AS OrderNumber,

    CASE
        WHEN L.LINESOURCEORDERSYSTEM = 'SYS21'
        THEN L.LINESOURCELINENUMBER
        ELSE L.LINEDISPLAYLINENUMBER
    END                                                     AS LineNumber,

    H.HEADERORDEREDDATE                                     AS OrderDate,
    CAST(H.HEADERCREATIONDATE AS DATE)                      AS HeaderCreateDate,
    H.HEADERONHOLD                                          AS HeaderOnHold,
    L.LINEONHOLD                                            AS LineOnHold,
    CAST(L.LINECREATIONDATE AS DATE)                        AS LineCreateDate,
    L.LINESCHEDULESHIPDATE                                  AS RequestedShipDate,
    L.LINEACTUALSHIPDATE                                    AS ShipDate,
    H.HEADERSTATUSCODE                                      AS OrderStatus,
    L.LINESTATUSCODE                                        AS LineStatusCode,
    H.HEADERORDERTYPECODE                                   AS HeaderTypeCode,
    L.LINEORDEREDQTY                                        AS OrderQty,
    F.FULFILLLINESHIPPEDQTY                                 AS FulfillLineShippedQTY,

    -- Item identifiers
    L.LINEINVENTORYITEMID                                   AS SKUID,
    I.ITEMBASEPEOITEMNUMBER                                 AS SKU,
    I.ITEMTRANSLATIONPEODESCRIPTION                         AS SKUDesc,
    L.LINEORDEREDUOM                                        AS LineUOM,

    -- Organisation / plant
    SUBSTRING(N.ATTRIBUTE1, 1, 2)                           AS Company,
    SUBSTRING(N.ATTRIBUTE1, 3, 2)                           AS Plant,
    SUBSTRING(d.ATTRIBUTE1, 3, 2)                           AS Stockroom,
    N.ORGANIZATIONCODE                                      AS OrganizationCode,
    COU.ORGANIZATIONUNITTRANSLATIONPEONAME                  AS OrganizationName,
    F.FULFILLLINESUBINVENTORY                               AS LineSubInventory,

    -- Customer identifiers
    H.HEADERSOLDTOPARTYID                                   AS CustomerID,
    A.ACCOUNTNUMBER                                         AS CustomerNumber,

    -- DeliveryAddressCode — middle segment of hyphen-delimited party site number
    SUBSTRING(
        OCPS.PARTYSITENUMBER,
        CHARINDEX('-', OCPS.PARTYSITENUMBER) + 1,
        CHARINDEX('-', OCPS.PARTYSITENUMBER,
            CHARINDEX('-', OCPS.PARTYSITENUMBER) + 1)
        - CHARINDEX('-', OCPS.PARTYSITENUMBER) - 1
    )                                                       AS DeliveryAddressCode,

    C.PARTYNAME                                             AS CustomerName,
    A.ACCOUNTNUMBER                                         AS AccountNumber,
    A.ACCOUNTNAME                                           AS AccountName,

    -- Computed join key — links to scut.vw_BuyList_DetailInfo on BuyListDetailKey
    -- Format: SKU + CustomerNumber + Company + DeliveryAddressCode + OrganizationCode
    CONCAT(
        I.ITEMBASEPEOITEMNUMBER,
        A.ACCOUNTNUMBER,
        SUBSTRING(N.ATTRIBUTE1, 1, 2),
        SUBSTRING(
            OCPS.PARTYSITENUMBER,
            CHARINDEX('-', OCPS.PARTYSITENUMBER) + 1,
            CHARINDEX('-', OCPS.PARTYSITENUMBER,
                CHARINDEX('-', OCPS.PARTYSITENUMBER) + 1)
            - CHARINDEX('-', OCPS.PARTYSITENUMBER) - 1
        ),
        N.ORGANIZATIONCODE
    )                                                       AS OrdersBaseKey

FROM scut.OracleSCMDistributedOrderOrchestrationLineExtract AS L

INNER JOIN scut.OracleSCMDistributedOrderOrchestrationHeaderExtract AS H
    ON  L.LINEHEADERID = H.HEADERID
    AND H.HEADERSTATUSCODE NOT IN ('DOO_DRAFT', 'DOO_REFERENCE')
    AND H.HEADERSOURCEDOCUMENTTYPECODE IS NULL

LEFT JOIN scut.OracleSCMDistributedOrderOrchestrationOrderAddressExtract AS O
    ON  H.HEADERID = O.ORDERADDRESSHEADERID
    AND O.ORDERADDRESSUSETYPE = 'SHIP_TO'

LEFT JOIN scut.OracleCommonPartySite AS OCPS
    ON  O.ORDERADDRESSPARTYSITEID = OCPS.PARTYSITEID

LEFT JOIN scut.OracleCommonCustomerAccount AS A
    ON  H.HEADERSOLDTOPARTYID = A.PARTYID

LEFT JOIN scut.OracleCommonPartyExtract AS C
    ON  H.HEADERSOLDTOPARTYID = C.PARTYID

LEFT JOIN scut.OracleCommonItemExtract AS I
    ON  L.LINEINVENTORYITEMID = I.ITEMBASEPEOINVENTORYITEMID
    AND L.LINEINVENTORYORGANIZATIONID = I.ITEMBASEPEOINVENTORYORGANIZATIONID
    AND I.ITEMTRANSLATIONPEOLANGUAGE = 'US'

LEFT JOIN LatestFulfillLine AS F
    ON  L.LINEID = F.FULFILLLINELINEID
    AND L.LINEHEADERID = F.FULFILLLINEHEADERID
    AND L.LINEINVENTORYORGANIZATIONID = F.FULFILLLINEINVENTORYORGANIZATIONID

LEFT JOIN scut.OracleSCMCommonInvOrgParametersExtract AS N
    ON  F.FULFILLLINEFULFILLORGID = N.ORGANIZATIONID

LEFT JOIN scut.OracleCommonOrganizationUnit AS COU
    ON  F.FULFILLLINEFULFILLORGID = COU.ORGANIZATIONID

LEFT OUTER JOIN scut.OracleCustomSalesOrdersFullFillLineEFF AS X
    ON  F.FULFILLLINEID = X.FULFILL_LINE_ID
    AND X.CONTEXT_CODE = 'Allocation Details'

LEFT OUTER JOIN scut.OracleCustomSubInventoryDetails AS d
    ON  F.FULFILLLINEFULFILLORGID = d.ORGANIZATION_ID
    AND X.ATTRIBUTE_CHAR1 = d.SECONDARY_INVENTORY_NAME

-- WHERE L.LINEACTUALSHIPDATE IS NULL
-- Commented out intentionally — full order lifecycle preserved.
-- Apply open-orders filter in consuming notebooks:
--     WHERE ShipDate IS NULL AND SKU != 'PALLETSLIPCHG'

-- =============================================================================
-- END OF FILE
-- =============================================================================