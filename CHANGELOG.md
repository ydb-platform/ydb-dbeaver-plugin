# Changelog

## 0.1.0

Initial release.

* YDB connection with all authentication methods: Anonymous, Static, Token, Service Account, Metadata
* Hierarchical object navigator: tables with folder hierarchy, topics, views, external data sources, external tables, streaming queries, resource pools, resource pool classifiers, system views (.sys)
* YQL SQL dialect with 150+ keywords, data types and built-in functions
* Autocomplete via YDB API
* EXPLAIN and EXPLAIN ANALYZE visualization: text plan, diagram, SVG statistics
* Session manager with idle session filter
* Cluster dashboard: CPU, storage, memory, network, running queries, node status (auto-refresh every 5 seconds)
* Access rights editor (ACL): grant, revoke, set owner, view permissions
* Topic message viewer (PersQueue)
* Streaming query management: view source, view issues, view plan, start, stop, alter
* Federated queries via external data sources (S3, databases)
* Specialized value editors for JSON, JSONDOCUMENT, YSON
* Context menu commands for creating tables, topics, views, resource pools, external data sources, external tables, transfers, streaming queries
* Clickable object links in properties panel (tables, topics, views, transfers)
* YDB JDBC Driver 2.3.10 bundled
