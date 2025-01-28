# Obsidian Insights
<p style="text-align: center; margin: 0;">
    <a href="https://www.dlthub.com"> <img src="https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?w=2000&auto=format" alt="dltHub logo" height="30px"></a>
    <a href="https://www.sqlmesh.com"><img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" alt="SQLMesh logo" height="30px"></a>
    <a href="https://www.motherduck.com"><img src="https://gist.githubusercontent.com/mattiasthalen/7919bc48c6e0d706bbec96f452f8ea69/raw/f76c2dde8ba0870e0ae52b7eb7f700a40cfda047/motherduck.svg" alt="MotherDuck logo" height="30px"></a>
</p>

### Goals:
- [x] Extract & load [Northwind](https://demodata.grapecity.com/#NorthWind) via REST API to [MotherDuck](https://www.motherduck.com) using [dlt](https://www.dlthub.com).
- [x] Transform using [SQLMesh](https://www.sqlmesh.com).
- [x] Model the silver layer according to [The Hook Cookbook](https://hookcookbook.substack.com/).
- [x] Model the gold layer as a [Unified Star Schema](https://www.amazon.com/Unified-Star-Schema-Resilient-Warehouse/dp/163462887X).
- [x] Add GitHub Actions for CI/CD and daily ELT.
- [ ] Add Apache Iceberg as an alternative to MotherDuck.

## How To Run
1. Clone the repo.
2. Run `pip install uv`, followed by `uv sync`.
4. Create an account on [MotherDuck](https://www.motherduck.com).
5. Create a database called `obsidian_insights`.
6. [Create a token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token).
7. Save the token (`motherduck_token=your_token`) in an `.env` file, placed at the repo root.
8. Run `init_warehouse.sh`.
9. Then run `elt.sh` whenever you want to refresh the warehouse.

## Architecture
```mermaid
graph LR
    %% Source Systems
    source_1[(Northwind)]
    %%source_2[(Adventure Works)]
    
    %% Processing Layer - Using ([text]) for servers
    extract(dlt)
    transform(SQLMesh)
    
    %% Data Warehouse - MotherDuck
    subgraph warehouse[MotherDuck]
        bronze[("BRONZE: Raw → Snapshot")]
        silver[("SILVER: Hook Model")]
        gold[("GOLD: Unified Star Schema")]
    end
    
    %% BI
    bi((BI/ML etc.))
    
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
```mermaid
flowchart TD

        _hook__reference__id__category(["_hook__reference__id__category"])
        _hook__reference__id__region(["_hook__reference__id__region"])
        _hook__reference__id__territory(["_hook__reference__id__territory"])
        _hook__customer__id(["_hook__customer__id"])
        _hook__employee__id(["_hook__employee__id"])
        _hook__order__id(["_hook__order__id"])
        _hook__product__id(["_hook__product__id"])
        _hook__shipper__id(["_hook__shipper__id"])
        _hook__supplier__id(["_hook__supplier__id"])
 
        bag__northwind__categories[("bag__northwind__categories")]
        bag__northwind__category_details[("bag__northwind__category_details")]
        bag__northwind__customers[("bag__northwind__customers")]
        bag__northwind__employees[("bag__northwind__employees")]
        bag__northwind__employee_territories[("bag__northwind__employee_territories")]
        bag__northwind__orders[("bag__northwind__orders")]
        bag__northwind__order_details[("bag__northwind__order_details")]
        bag__northwind__products[("bag__northwind__products")]
        bag__northwind__regions[("bag__northwind__regions")]
        bag__northwind__shippers[("bag__northwind__shippers")]
        bag__northwind__suppliers[("bag__northwind__suppliers")]
        bag__northwind__territories[("bag__northwind__territories")]

    _hook__reference__id__category  o--o bag__northwind__categories
    bag__northwind__category_details  o--o _hook__reference__id__category
    _hook__reference__id__category  o--o bag__northwind__products
    _hook__customer__id  o--o bag__northwind__customers
    bag__northwind__orders  o--o _hook__customer__id
    _hook__employee__id  o--o bag__northwind__employees
    bag__northwind__orders  o--o _hook__employee__id
    _hook__employee__id  o--o bag__northwind__employee_territories
    _hook__order__id  o--o bag__northwind__orders
    bag__northwind__order_details  o--o _hook__order__id
    _hook__product__id  o--o bag__northwind__products
    bag__northwind__order_details  o--o _hook__product__id
    _hook__reference__id__region  o--o bag__northwind__regions
    bag__northwind__territories  o--o _hook__reference__id__region
    _hook__shipper__id  o--o bag__northwind__shippers
    bag__northwind__orders  o--o _hook__shipper__id
    _hook__supplier__id  o--o bag__northwind__suppliers
    bag__northwind__products  o--o _hook__supplier__id
    _hook__reference__id__territory  o--o bag__northwind__territories
    bag__northwind__employee_territories  o--o _hook__reference__id__territory
```

### gold.*
```mermaid
erDiagram
    uss__customers ||--|{ uss__bridge: ""
    uss__employees ||--|{ uss__bridge: ""
    uss__order_details ||--|{ uss__bridge: ""
    uss__orders ||--|{ uss__bridge: ""

    uss__bridge }|--|| uss__products : ""
    uss__bridge }|--|| uss__product_categories: ""
    uss__bridge }|--|| uss__sales_territories: ""
    uss__bridge }|--|| uss__shippers: ""
    uss__bridge }|--|| uss__suppliers: ""
```
