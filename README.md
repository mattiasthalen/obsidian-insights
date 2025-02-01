# Obsidian Insights
<p style="text-align: center; margin: 0;">
    <a href="https://www.dlthub.com"> <img src="https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?w=2000&auto=format" alt="dltHub logo" height="30px"></a>
    <a href="https://www.sqlmesh.com"><img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" alt="SQLMesh logo" height="30px"></a>
    <a href="https://www.duckdb.org"><img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" alt="DuckDB logo" height="30px"></a>
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
1. Run `pip install uv`, followed by `uv sync`.
1. Decide if you want to use DuckDB (local) or MotherDuck (remote) as gateway.
    * DuckDB (default)
      1. Add the gateway var in .env: `gateway=duckdb`. (fallback is this)
      1. Add the duckdb_path var in .env: `duckdb_path=your_db.duckdb`. (default is `./obsidian_insights.duckdb`)
    * MotherDuck
      1. Add the gateway var in .env: `gateway=motherduck`. (fallback is `duckdb`)
      1. Create an account on [MotherDuck](https://www.motherduck.com).
        1. Create a database called `obsidian_insights`.
        1. [Create a token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token).
        1. Add the motherduck_token var in .env: `motherduck_token=your_token`.
1. Run `init_warehouse.sh` and follow the prompts.
1. Then run `elt.sh` whenever you want to refresh the warehouse.

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

## Formulas
> **!NOTE**
>
>I'm using this definition of what a measure is:
> 
>>*A measure is a raw quantifiable value representing a specific aspect of performance, status, or characteristics that must include a <ins>**temporal anchor**</ins> specifying the exact point or period in time to which it refers.*
> 
>I.e., a measure <ins>**must**</ins> be associated with a date.
>
>E.g., the amount on an invoice is associated with three dates; incoive date, due date, and payment date.
>That means there will be three measures: amount invoiced, amount due, amount payed.

### Key Performance Indicators
#### Primary
|**Name**|**Temporal Anchor**|**Formula**|
|-|-|-|
|% Order Fill Rate|Order & Ship Date|# Orders Shipped / # Orders Placed|
|% On Time Delivery|Order Due Date|# Orders Shipped On Time / # Orders Due|
|Average Order Processing Time|Shipped Date|Total Order Processing Time / # Orders Shipped|

#### Secondary
|**Name**|**Temporal Anchor**|**Formula**|
|-|-|-|
|Average Age of Open Orders|-|-|
|% Orders Within Capability|-|-|
|Processing Time StDev|-|-|

### Metrics
|**Name**|**Temporal Anchor**|**Formula**|
|-|-|-|
|# Orders Shipped|-|-|
|# Orders Placed|-|-|
|# Orders Shipped On Time|-|-|
|# Orders Due|-|-|
|Total Order Processing Time|-|-|

### Measures
|**Name**|**Temporal Anchor**|**Formula**|
|-|-|-|

### Measures In The Unified Star Schema
Instead of building a regular bridge, we will turn it into an event based bridge.
This will allow us to stack measures in the same graph and on a common date dimension.

This is the normal bridge:
|**Stage**|**_key__orders**|**_key__customers**|
|-|-|-|
|Orders|A|X|
|Orders|B|X|
|Customers|-|X|

We then add the measurements, along with their corresponding date.
- I.e., `# Orders Shipped` would set the date to `shipped_date`.

|**Stage**|**_key__orders**|**_key__customers**|**_key__calendar**|**# Orders Placed**|**# Orders Required**|**# Orders Shipped**|
|-|-|-|-|-|-|-|
|Orders|A|X|2025-01-01|1|-|-|
|Orders|A|X|2025-01-02|-|1|-|
|Orders|A|X|2025-01-02|-|-|1|
|Orders|B|X|2025-01-01|1|-|-|
|Orders|B|X|2025-01-01|-|1|-|
|Orders|B|X|2025-01-01|-|-|1|
|Customers|-|X|-|-|-|-|

What happened is that every row got duplicated, with one line per measurement.
We can do better than this, we can group it by date.

|**Stage**|**_key__orders**|**_key__customers**|**_key__calendar**|**# Orders Placed**|**# Orders Required**|**# Orders Shipped**|
|-|-|-|-|-|-|-|
|Orders|A|X|2025-01-01|1|-|-|
|Orders|A|X|2025-01-02|-|1|1|
|Orders|B|X|2025-01-01|1|1|1|
|Customers|-|X|-|-|-|-|

So, how many orders were placed, required, and shipped per day, for customer X?
|**Customer**|**Date**|**# Orders Placed**|**# Orders Required**|**# Orders Shipped**|
|-|-|-|-|-|
|x|2025-01-01|2|1|1|
|x|2025-01-02|0|1|1|

## Lineage / DAG
```mermaid
flowchart LR
    subgraph obsidian_insights.bronze["obsidian_insights.bronze"]
        direction LR
        raw__northwind__categories(["raw__northwind__categories"])
        raw__northwind__category_details(["raw__northwind__category_details"])
        raw__northwind__customers(["raw__northwind__customers"])
        raw__northwind__employee_territories(["raw__northwind__employee_territories"])
        raw__northwind__employees(["raw__northwind__employees"])
        raw__northwind__order_details(["raw__northwind__order_details"])
        raw__northwind__orders(["raw__northwind__orders"])
        raw__northwind__products(["raw__northwind__products"])
        raw__northwind__regions(["raw__northwind__regions"])
        raw__northwind__shippers(["raw__northwind__shippers"])
        raw__northwind__suppliers(["raw__northwind__suppliers"])
        raw__northwind__territories(["raw__northwind__territories"])
        snp__northwind__categories(["snp__northwind__categories"])
        snp__northwind__category_details(["snp__northwind__category_details"])
        snp__northwind__customers(["snp__northwind__customers"])
        snp__northwind__employee_territories(["snp__northwind__employee_territories"])
        snp__northwind__employees(["snp__northwind__employees"])
        snp__northwind__order_details(["snp__northwind__order_details"])
        snp__northwind__orders(["snp__northwind__orders"])
        snp__northwind__products(["snp__northwind__products"])
        snp__northwind__regions(["snp__northwind__regions"])
        snp__northwind__shippers(["snp__northwind__shippers"])
        snp__northwind__suppliers(["snp__northwind__suppliers"])
        snp__northwind__territories(["snp__northwind__territories"])
    end

    subgraph obsidian_insights.silver["obsidian_insights.silver"]
        direction LR
        bag__northwind__categories(["bag__northwind__categories"])
        bag__northwind__customers(["bag__northwind__customers"])
        bag__northwind__employee_territories(["bag__northwind__employee_territories"])
        bag__northwind__employees(["bag__northwind__employees"])
        bag__northwind__order_details(["bag__northwind__order_details"])
        bag__northwind__orders(["bag__northwind__orders"])
        bag__northwind__products(["bag__northwind__products"])
        bag__northwind__regions(["bag__northwind__regions"])
        bag__northwind__shippers(["bag__northwind__shippers"])
        bag__northwind__suppliers(["bag__northwind__suppliers"])
        bag__northwind__territories(["bag__northwind__territories"])
        int__measures__order_details(["int__measures__order_details"])
        int__measures__orders(["int__measures__orders"])
        int__uss_bridge(["int__uss_bridge"])
        int__uss_bridge__categories(["int__uss_bridge__categories"])
        int__uss_bridge__customers(["int__uss_bridge__customers"])
        int__uss_bridge__employee_territories(["int__uss_bridge__employee_territories"])
        int__uss_bridge__employees(["int__uss_bridge__employees"])
        int__uss_bridge__order_details(["int__uss_bridge__order_details"])
        int__uss_bridge__orders(["int__uss_bridge__orders"])
        int__uss_bridge__products(["int__uss_bridge__products"])
        int__uss_bridge__regions(["int__uss_bridge__regions"])
        int__uss_bridge__shippers(["int__uss_bridge__shippers"])
        int__uss_bridge__suppliers(["int__uss_bridge__suppliers"])
        int__uss_bridge__territories(["int__uss_bridge__territories"])
    end

    subgraph obsidian_insights.gold["obsidian_insights.gold"]
        direction LR
        _bridge(["_bridge"])
        calendar(["calendar"])
        categories(["categories"])
        customers(["customers"])
        employees(["employees"])
        order_details(["order_details"])
        orders(["orders"])
        products(["products"])
        regions(["regions"])
        shippers(["shippers"])
        suppliers(["suppliers"])
        territories(["territories"])
    end

    %% obsidian_insights.bronze -> obsidian_insights.bronze
    raw__northwind__categories --> snp__northwind__categories
    raw__northwind__category_details --> snp__northwind__category_details
    raw__northwind__customers --> snp__northwind__customers
    raw__northwind__employee_territories --> snp__northwind__employee_territories
    raw__northwind__employees --> snp__northwind__employees
    raw__northwind__order_details --> snp__northwind__order_details
    raw__northwind__orders --> snp__northwind__orders
    raw__northwind__products --> snp__northwind__products
    raw__northwind__regions --> snp__northwind__regions
    raw__northwind__shippers --> snp__northwind__shippers
    raw__northwind__suppliers --> snp__northwind__suppliers
    raw__northwind__territories --> snp__northwind__territories

    %% obsidian_insights.bronze -> obsidian_insights.silver
    snp__northwind__category_details --> bag__northwind__categories
    snp__northwind__customers --> bag__northwind__customers
    snp__northwind__employee_territories --> bag__northwind__employee_territories
    snp__northwind__employees --> bag__northwind__employees
    snp__northwind__order_details --> bag__northwind__order_details
    snp__northwind__orders --> bag__northwind__orders
    snp__northwind__products --> bag__northwind__products
    snp__northwind__regions --> bag__northwind__regions
    snp__northwind__shippers --> bag__northwind__shippers
    snp__northwind__suppliers --> bag__northwind__suppliers
    snp__northwind__territories --> bag__northwind__territories

    %% obsidian_insights.silver -> obsidian_insights.silver
    bag__northwind__categories --> int__uss_bridge__categories
    bag__northwind__categories --> int__uss_bridge__order_details
    bag__northwind__customers --> int__uss_bridge__customers
    bag__northwind__customers --> int__uss_bridge__order_details
    bag__northwind__customers --> int__uss_bridge__orders
    bag__northwind__employee_territories --> int__uss_bridge__employee_territories
    bag__northwind__employees --> int__uss_bridge__employee_territories
    bag__northwind__employees --> int__uss_bridge__employees
    bag__northwind__employees --> int__uss_bridge__order_details
    bag__northwind__employees --> int__uss_bridge__orders
    bag__northwind__order_details --> int__measures__order_details
    bag__northwind__order_details --> int__uss_bridge__order_details
    bag__northwind__orders --> int__measures__order_details
    bag__northwind__orders --> int__measures__orders
    bag__northwind__orders --> int__uss_bridge__order_details
    bag__northwind__orders --> int__uss_bridge__orders
    bag__northwind__products --> int__uss_bridge__order_details
    bag__northwind__products --> int__uss_bridge__products
    bag__northwind__regions --> int__uss_bridge__employee_territories
    bag__northwind__regions --> int__uss_bridge__regions
    bag__northwind__regions --> int__uss_bridge__territories
    bag__northwind__shippers --> int__uss_bridge__order_details
    bag__northwind__shippers --> int__uss_bridge__orders
    bag__northwind__shippers --> int__uss_bridge__shippers
    bag__northwind__suppliers --> int__uss_bridge__order_details
    bag__northwind__suppliers --> int__uss_bridge__products
    bag__northwind__suppliers --> int__uss_bridge__suppliers
    bag__northwind__territories --> int__uss_bridge__employee_territories
    bag__northwind__territories --> int__uss_bridge__territories
    int__measures__order_details --> int__uss_bridge__order_details
    int__measures__orders --> int__uss_bridge__orders
    int__uss_bridge__categories --> int__uss_bridge
    int__uss_bridge__customers --> int__uss_bridge
    int__uss_bridge__employee_territories --> int__uss_bridge
    int__uss_bridge__employees --> int__uss_bridge
    int__uss_bridge__order_details --> int__uss_bridge
    int__uss_bridge__orders --> int__uss_bridge
    int__uss_bridge__products --> int__uss_bridge
    int__uss_bridge__regions --> int__uss_bridge
    int__uss_bridge__shippers --> int__uss_bridge
    int__uss_bridge__suppliers --> int__uss_bridge
    int__uss_bridge__territories --> int__uss_bridge

    %% obsidian_insights.silver -> obsidian_insights.gold
    bag__northwind__categories --> categories
    bag__northwind__customers --> customers
    bag__northwind__employees --> employees
    bag__northwind__order_details --> order_details
    bag__northwind__orders --> orders
    bag__northwind__products --> products
    bag__northwind__regions --> regions
    bag__northwind__shippers --> shippers
    bag__northwind__suppliers --> suppliers
    bag__northwind__territories --> territories
    int__uss_bridge --> _bridge
    int__uss_bridge --> calendar
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
flowchart LR
    _bridge("_bridge")

    categories(["categories"])
    customers(["customers"])
    employees(["employees"])
    order_details(["order_details"])
    orders(["orders"])
    products(["products"])
    regions(["regions"])
    shippers(["shippers"])
    suppliers(["suppliers"])
    territories(["territories"])
    calendar(["calendar"])

    _bridge --> categories
    _bridge --> customers
    _bridge --> employees
    _bridge --> order_details
    _bridge --> orders
    _bridge --> products
    _bridge --> regions
    _bridge --> shippers
    _bridge --> suppliers
    _bridge --> territories
    _bridge --> calendar
```
