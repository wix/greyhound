# Greyhound vs. Kafka Streams

| Criteria      | Greyhound | Kafka Stream |
| ------------- | ------------- | ------------- |
| Main Purpose  | Event driven inter-service communication  | Stream processing  |
| DSL  | Produce + Consumer fluent APIs  | FlatMap, Filter, GroupBy, etc...  |
| Features | * Consumer Retries, | * Stream processing
|          | * Parallel message processing, | * SQL-like tables with joins
|          | * observability (metrics) | * Sliding Window
| Footprint | Lightweight - creates retry topics | Heavyweight creates many topics, and DB instances
