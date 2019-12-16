#!/usr/bin/env python3
from kafka import KafkaConsumer
import click
from requests import Request, Session

@click.command()
@click.argument('kafka_broker')
@click.argument('topic')
@click.argument('group')
@click.argument('clickhouse_server')
@click.argument('table')

def consume_and_insert(kafka_broker, topic, group, clickhouse_server, table):
    url = clickhouse_server + "/?query=INSERT%20INTO%20" + table + "%20FORMAT%20JSONEachRow" 
    data = b''
    s = Session()
    consumer = KafkaConsumer(topic, bootstrap_servers = kafka_broker, enable_auto_commit = False, group_id = group)
    for message in consumer:
        data += message.value
        if len(data) > 1000000000:
            resp = s.post(url, data=data)
            if resp.status_code == 200:
                consumer.commit()
            else:
                print("fail to insert into clickhouse, quit") 
                return
    return


if __name__ == "__main__":
    consume_and_insert()

