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
flowchart LR
    raw__northwind__orders("raw__northwind__orders")
    raw__northwind__order_details("raw__northwind__order_details")
    raw__northwind__products("raw__northwind__products")
    
    raw__northwind__category_details(["raw__northwind__category_details"])
    raw__northwind__customers(["raw__northwind__customers"])
    raw__northwind__employee_territories(["raw__northwind__employee_territories"])
    raw__northwind__employees(["raw__northwind__employees"])
    raw__northwind__region(["raw__northwind__region"])
    raw__northwind__shippers(["raw__northwind__shippers"])
    raw__northwind__suppliers(["raw__northwind__suppliers"])
    raw__northwind__territories(["raw__northwind__territories"])

    raw__northwind__order_details --> raw__northwind__orders
    raw__northwind__order_details --> raw__northwind__products
    raw__northwind__products --> raw__northwind__category_details
    raw__northwind__products --> raw__northwind__suppliers
    raw__northwind__orders --> raw__northwind__employees
    raw__northwind__orders --> raw__northwind__customers
    raw__northwind__orders --> raw__northwind__shippers
    raw__northwind__territories --> raw__northwind__region
    raw__northwind__employee_territories --> raw__northwind__territories
    raw__northwind__employee_territories --> raw__northwind__employees
```

### silver.*
```mermaid
flowchart LR
        _hook__reference__category(["_hook__reference__category"])
        _hook__reference__region(["_hook__reference__region"])
        _hook__reference__territory(["_hook__reference__territory"])
        _hook__customer(["_hook__customer"])
        _hook__employee(["_hook__employee"])
        _hook__order(["_hook__order"])
        _hook__product(["_hook__product"])
        _hook__shipper(["_hook__shipper"])
        _hook__supplier(["_hook__supplier"])
 
        bag__northwind__categories[("bag__northwind__categories")]
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

    bag__northwind__order_details --> _hook__order --> bag__northwind__orders
    bag__northwind__order_details --> _hook__product --> bag__northwind__products
    bag__northwind__products --> _hook__reference__category --> bag__northwind__categories
    bag__northwind__products --> _hook__supplier --> bag__northwind__suppliers
    bag__northwind__orders --> _hook__employee --> bag__northwind__employees
    bag__northwind__orders --> _hook__customer --> bag__northwind__customers
    bag__northwind__orders --> _hook__shipper --> bag__northwind__shippers
    bag__northwind__territories --> _hook__reference__region --> bag__northwind__regions
    bag__northwind__employee_territories --> _hook__reference__territory --> bag__northwind__territories
    bag__northwind__employee_territories --> _hook__employee
```

### gold.*
```mermaid
flowchart TD
    uss__bridge[("uss__bridge")]

    uss__peripheral__categories(["uss__peripheral__categories"])
    uss__peripheral__customers(["uss__peripheral__customers"])
    uss__peripheral__employees(["uss__peripheral__employees"])
    uss__peripheral__order_details(["uss__peripheral__order_details"])
    uss__peripheral__orders(["uss__peripheral__orders"])
    uss__peripheral__products(["uss__peripheral__products"])
    uss__peripheral__regions(["uss__peripheral__regions"])
    uss__peripheral__shippers(["uss__peripheral__shippers"])
    uss__peripheral__suppliers(["uss__peripheral__suppliers"])
    uss__peripheral__territories(["uss__peripheral__territories"])

    uss__peripheral__categories o--o uss__bridge
    uss__peripheral__customers o--o uss__bridge
    uss__peripheral__employees o--o uss__bridge
    uss__peripheral__order_details o--o uss__bridge
    uss__peripheral__orders o--o uss__bridge
    
    uss__bridge o--o uss__peripheral__products
    uss__bridge o--o uss__peripheral__regions
    uss__bridge o--o uss__peripheral__shippers
    uss__bridge o--o uss__peripheral__suppliers
    uss__bridge o--o uss__peripheral__territories
```
