# Singer Target for Iceberg Tables

This is a [Singer](https://singer.io) target that loads data from Singer streams into [Iceberg tables](https://iceberg.apache.org/). This reposotory provides mulitple singer targets, one for each iceberg catalog. These are:

- [SQL catalog target](/target-iceberg-sql/README.md)

## Features

- Creates Iceberg tables automatically if they don't exist
- Incrementally loads versions into Iceberg tables
- Converts Singer stream schemas into Iceberg table schemas
- Validates records against the Singer stream schema
- Generates metadata about syncs for data governance 

## Usage

The target ingests singer messages into the Icberg tables and stores the state in the `singer-bookmark` property.

### Sync mode

To run:

```bash
target-iceberg-sql --config config.json
```

## Configuration

Example:

```json
{
    "streams": {
      "inventory-orders": { 
        "identifier": "bronze.inventory.orders",
        "replication-method": "LOG_BASED"
      },
      "inventory-customers": { 
        "identifier": "bronze.inventory.customers",
        "replication-method": "LOG_BASED"
      },
      "inventory-products": { 
        "identifier": "bronze.inventory.products",
        "replication-method": "LOG_BASED"
      }
    },
    "bucket": "s3://example-postgres",
    "catalogName": "bronze",
    "catalogUrl": "postgres://postgres:postgres@postgres:5432",
    "awsRegion": "us-east-1",
    "awsAccessKeyId": "AKIAIOSFODNN7EXAMPLE",
    "awsSecretAccessKey": "$AWS_SECRET_ACCESS_KEY",
    "awsEndpoint": "http://localstack:4566",
    "awsAllowHttp": "true"
}
```

The configuration consists of 3 parts:
- General parameters
- Catalog parameters
- Object store parameters

### General parameters

The general parameters apply to each catalog and object store.

| Parameter | Description | 
|-|-|  
| `streams` | A map of streams to replicate. Each stream is a map with the fields: `identifier`, `replication-method`(optional) |
| `bucket` (optional) | Object store bucket where the iceberg tables should be stored (optional) |



### Catalog parameters

Only one set of catalog parameters should be used in the configuration. Choose which catalog you want to use.

#### SQL catalog

| Parameter | Description |
|-|-|  
| `catalogName` | The name of the catalog |
| `catalogUrl` | The connection url of the catalog |


### Object store parameters

Only one set of object store parameters should be used in the configuration. Choose which object store you want to use.

#### AWS S3

| Parameter | Description |
|-|-|  
| `awsRegion` | The region of the bucket |
| `awsAccessKeyId` | The access key id |
| `awsSecretAccessKey` | The secret access key |
| `awsEndpoint` (optional) | The endpoint of the object store |
| `awsAllowHttp` (optional) | Allow http connections to the object store |



## Docker containers

- [dashbook/target-iceberg:sql](https://hub.docker.com/r/dashbook/target-iceberg)

## Contributing

Feel free to open issues for any feedback or ideas! PRs are welcome.

