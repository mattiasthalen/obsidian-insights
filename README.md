# Obsidian Insights
<p style="text-align: center; margin: 0;">
    <img src="https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?w=2000&auto=format" alt="dltHub logo" width="10%" height="10%">
    <img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" alt="SQLMesh logo" width="10%" height="10%">
    <img src="https://mma.prnewswire.com/media/1946939/MotherDuck_Logo.jpg?w=200" alt="MotherDuck logo" width="10%" height="10%">
    <img src="https://www.metabase.com/images/logo-with-wordmark.svg" alt="Metabase logo" width="10%" height="10%">
</p>


Personal project for setting up an end-to-end analytics platform.

### Goals:
- [x] Extract & load [Northwind](https://demodata.grapecity.com/#NorthWind) from REST API to [MotherDuck](https://www.motherduck.com) using [dlt](https://www.dlthub.com).
- [x] Transform using [SQLMesh](https://www.sqlmesh.com).
- [x] Model the silver layer according to [HOOK](https://hookcookbook.substack.com/).
- [x] Model the gold layer as a [Unified Star Schema](https://www.amazon.com/Unified-Star-Schema-Resilient-Warehouse/dp/163462887X).
- [x] Add GitHub Actions for CI/CD and daily ELT.
- [ ] Create reports in [Metabase](https://www.metabase.com/).
- [ ] Add [Adventure Works](https://demodata.grapecity.com/#AdventureWorks) to the mix, to train on a scenario with multiple sources.

## Architecture
```mermaid
graph LR
    %% Source Systems
    source_1[(Northwind)]
    %%source_2[(Adventure Works)]
    
    %% Processing Layer - Using ([text]) for servers
    extract([dlt])
    transform([SQLMesh])
    
    %% Data Warehouse - MotherDuck
    subgraph warehouse[MotherDuck]
        bronze[(Bronze)]
        silver[(Silver)]
        gold[(Gold)]
    end
    
    %% BI
    bi[Metabase]
    
    %% Connections
    source_1 --> extract
    %%source_2 --> extract
    extract --> bronze
    bronze --> silver
    silver --> gold
    gold --> bi
    transform -.- bronze
    transform -.- silver
    transform -.- gold
```

## ERDs
### bronze.*
#### bronze.raw__northwind__*
```mermaid
erDiagram
    raw__northwind__customers ||--o{ raw__northwind__orders : customer_id
    raw__northwind__orders ||--o{ raw__northwind__order_details : order_id
    raw__northwind__products ||--o{ raw__northwind__order_details : product_id
    raw__northwind__categories ||--|| raw__northwind__category_details : category_id
    raw__northwind__categories ||--o{ raw__northwind__products : category_id
    raw__northwind__suppliers ||--o{ raw__northwind__products : supplier_id
    raw__northwind__employees ||--o{ raw__northwind__orders : employee_id
    raw__northwind__shippers ||--o{ raw__northwind__orders : shipper_id
    raw__northwind__territories }|--|| raw__northwind__region : region_id
    raw__northwind__employee_territories }o--|| raw__northwind__employees : employee_id
    raw__northwind__employee_territories }o--|| raw__northwind__territories : territory_id
```

### silver.*
#### silver.bag__northwind__*
```mermaid
erDiagram
    bag__northwind__customers ||--o{ bag__northwind__orders : hook__customer__id
    bag__northwind__orders ||--o{ bag__northwind__order_details : hook__order__id
    bag__northwind__products ||--o{ bag__northwind__order_details : hook__product__id
    bag__northwind__categories ||--|| bag__northwind__category_details : hook__category__id
    bag__northwind__categories ||--o{ bag__northwind__products : hook__category__id
    bag__northwind__suppliers ||--o{ bag__northwind__products : hook__supplier__id
    bag__northwind__employees ||--o{ bag__northwind__orders : hook__employee__id
    bag__northwind__shippers ||--o{ bag__northwind__orders : hook__shipper__id
    bag__northwind__territories }|--|| bag__northwind__region : hook__region__id
    bag__northwind__employee_territories }o--|| bag__northwind__employees : hook__employee__id
    bag__northwind__employee_territories }o--|| bag__northwind__territories : hook__territory__id
```

### gold.*
```mermaid
flowchart LR
    uss__northwind__categories --> uss__bridge
    uss__northwind__category_details --> uss__bridge
    uss__northwind__customers --> uss__bridge
    uss__northwind__employee_territories --> uss__bridge
    uss__northwind__employees --> uss__bridge
    uss__northwind__order_details --> uss__bridge
    uss__northwind__orders --> uss__bridge
    uss__northwind__products --> uss__bridge
    uss__northwind__regions --> uss__bridge
    uss__northwind__shippers --> uss__bridge
    uss__northwind__suppliers --> uss__bridge
    uss__northwind__territories --> uss__bridge
```
