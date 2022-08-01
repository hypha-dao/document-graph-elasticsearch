# Document Graph Elastic Search Stream Processor

Connects to the dfuse firehose to get a stream of table deltas from a contract that implements document graph, and updates the elastic search schema and data accordingly, in order for the elastic search database to reflect the data contained in the documents and edges contract tables.

To run only the document graph elastic search stream process with the helper script:

`./start-local-env.sh`

Or:

`go run . ./config.yml`

Look at the **config.yml** file for an example configuration file, some important configuration parameters are:

- firehose-endpoint: The dfuse firehose endpoint to connecto
- elastic-endpoint: The elastic search endpoint
- elastic-ca: The elastic search TLS certificate
- cursor-index-prefix: The prefix to use for this instance cursor it should be unique for the database instance
- contracts: Defines the properties of the contracts to listen to
  - index-prefix: The index prefix to use when storing data from this contract, should be unique for the database instance
  - edge-black-list: Enables the specification of edges that should not be stored, the "*" wild card may be specified for the properties to indicate that all of them should be ignored

The elastic search username and password are provided through the following environment variables:
- ES_USER
- ES_PASSWORD