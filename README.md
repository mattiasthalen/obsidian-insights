# Obsidian Insights
<p style="text-align: center; margin: 0;">
    <a href="https://www.dlthub.com"> <img src="https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?w=2000&auto=format" alt="dltHub logo" height="30px"></a>
    <a href="https://www.sqlmesh.com"><img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" alt="SQLMesh logo" height="30px"></a>
    <a href="https://www.motherduck.com"><img src="https://gist.githubusercontent.com/mattiasthalen/7919bc48c6e0d706bbec96f452f8ea69/raw/f76c2dde8ba0870e0ae52b7eb7f700a40cfda047/motherduck.svg" alt="MotherDuck logo" height="30px"></a>
</p>

### Goals:
- [x] Extract & load [Northwind](https://demodata.grapecity.com/#NorthWind) via REST API to [MotherDuck](https://www.motherduck.com) using [dlt](https://www.dlthub.com).
- [x] Transform using [SQLMesh](https://www.sqlmesh.com).
- [x] Model the silver layer according to [HOOK](https://hookcookbook.substack.com/).
- [x] Model the gold layer as a [Unified Star Schema](https://www.amazon.com/Unified-Star-Schema-Resilient-Warehouse/dp/163462887X).
- [x] Add GitHub Actions for CI/CD and daily ELT.
- [ ] Add Apache Iceberg as an alternative to MotherDuck.
- [ ] Add [Adventure Works](https://demodata.grapecity.com/#AdventureWorks) to the mix, to train on a scenario with multiple sources.

## How To Run
1. Clone the repo.
2. Create an account on [MotherDuck](https://www.motherduck.com).
3. Create a database called `obsidian_insights`.
4. [Create a token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token).
5. Save the token (`motherduck_token=your_token`) in an `.env` file, placed at the repo root.
6. Run `init_warehouse.sh`.
7. Then run `elt.sh` whenever you want to refresh the warehouse.

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
flowchart LR

    subgraph category
        hook__category__id(["hook__category__id"])
    end
    
    subgraph customer
        hook__customer__id(["hook__customer__id"])
    end
    
    subgraph employee
        hook__employee__id(["hook__employee__id"])
    end

    subgraph order
        hook__order__id(["hook__order__id"])
    end
    
    subgraph product
        hook__product__id(["hook__product__id"])
    end
    
    subgraph region
        hook__region__id(["hook__region__id"])
    end

    subgraph shipper
        hook__shipper__id(["hook__shipper__id"])
    end
    
    subgraph supplier
        hook__supplier__id(["hook__supplier__id"])
    end
    
    subgraph territory
        hook__territory__id(["hook__territory__id"])
    end

    subgraph bags
        bag__northwind__categories[("bag__northwind__categories")]
        bag__northwind__category_details[("bag__northwind__category_details")]
        bag__northwind__customers[("bag__northwind__customers")]
        bag__northwind__employees[("bag__northwind__employees")]
        bag__northwind__orders[("bag__northwind__orders")]
        bag__northwind__order_details[("bag__northwind__order_details")]
        bag__northwind__products[("bag__northwind__products")]
        bag__northwind__regions[("bag__northwind__regions")]
        bag__northwind__shippers[("bag__northwind__shippers")]
        bag__northwind__suppliers[("bag__northwind__suppliers")]
        bag__northwind__territories[("bag__northwind__territories")]
        bag__northwind__employee_territories[("bag__northwind__employee_territories")]
    end

    hook__category__id --> bag__northwind__categories
    hook__category__id --> bag__northwind__category_details
    hook__category__id --> bag__northwind__products

    hook__customer__id -->  bag__northwind__customers
    hook__customer__id -->  bag__northwind__orders

    hook__employee__id -->  bag__northwind__employees
    hook__employee__id -->  bag__northwind__orders
    hook__employee__id -->  bag__northwind__employee_territories

    hook__order__id --> bag__northwind__orders
    hook__order__id --> bag__northwind__order_details

    hook__product__id --> bag__northwind__products
    hook__product__id --> bag__northwind__order_details

    hook__region__id -->  bag__northwind__regions
    hook__region__id --> bag__northwind__territories

    hook__shipper__id --> bag__northwind__shippers
    hook__shipper__id --> bag__northwind__orders

    hook__supplier__id --> bag__northwind__suppliers
    hook__supplier__id --> bag__northwind__products

    hook__territory__id --> bag__northwind__territories
    hook__territory__id -->  bag__northwind__employee_territories
```

### gold.*
```mermaid
flowchart LR
    uss__bridge[("uss__bridge")]
    uss__northwind__categories(["uss__northwind__categories"])
    uss__northwind__category_details(["uss__northwind__category_details"])
    uss__northwind__customers(["uss__northwind__customers"])
    uss__northwind__employee_territories(["uss__northwind__employee_territories"])
    uss__northwind__employees(["uss__northwind__employees"])
    uss__northwind__order_details(["uss__northwind__order_details"])
    uss__northwind__orders(["uss__northwind__orders"])
    uss__northwind__products(["uss__northwind__products"])
    uss__northwind__regions(["uss__northwind__regions"])
    uss__northwind__shippers(["uss__northwind__shippers"])
    uss__northwind__suppliers(["uss__northwind__suppliers"])
    uss__northwind__territories(["uss__northwind__territories"])

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
