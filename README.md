# clickhouse-inserter-from-kafka

Consume JSON data from a topic and insert it into a clickhouse table, supporting nested data structure  

    ./clickhouse_inserter_from_kafka.py ${KAFKA_CLUSTER}:9092 minhthucdao2 minhthucdao2_latest http://${CLICKHOUSE_CLUSTER}:8123 coccoc_search.serp

