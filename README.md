# Indexing-Elasticsearch-Kuromoji

## Argpasing

| Args | Example | Description |
| --- | --- | --- |
| path_analyzer | ./analyzer.json |  |
| elasticsearch_url | http://localhost:9200 |  |
| elasticsearch_tokenizer | kuromoji_tokenizer |  |
| elasticsearch_index | article |  |
| path_export | ./export |  |

## Setup

```bash

poetry install

```

### Installation

```bash

poetry run python es_indexing/main.py --elasticsearch_url http://localhost:9200

```

### Run

## Reference

<https://elasticsearch-py.readthedocs.io/en/v8.7.0/#>
<https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html>
