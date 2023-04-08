import json
import os
import copy
from typing import TypedDict
import argparse
from logconf import mylogger
logger = mylogger(__name__)
import utils

from elasticsearch import Elasticsearch, helpers


class WikiDict(TypedDict):
    id: int
    revid: int
    url: str
    title: str
    text: str


class EsDocuent(TypedDict):
    article_id: int
    revid: int
    url: str
    title: str
    text: str
    content_type: str


def wikidict2esarticle(d: WikiDict) -> EsDocuent:
    return {
        "article_id": d["id"],
        "revid": d["revid"],
        "url": d["url"],
        "title": d["title"],
        "text": d["text"],
        "content_type": "text",
    }


def gen_bulk_data(documents: list[WikiDict]):
    for d in documents:
        yield {"_op_type": "create", "_index": "ja", "_source": wikidict2esarticle(d)}


def file_to_document_list(
    args: argparse.Namespace,
    file_path: str
) -> list[WikiDict]:
    result: list[WikiDict] = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            if line == "\n":
                continue
            j: WikiDict = json.loads(line)
            result += [
                update_text_in_dict(j, text) for text in split_text_of_n_words(
                    args.elasticsearch_url,
                    args.elasticsearch_index,
                    args.elasticsearch_tokenizer,
                    j["text"]
                )
            ]
    return result


def split_text_of_n_words(
    url_elasticsearch: str,
    index: str,
    tokenizer_name: str,
    text: str,
    n: int=300
) -> list[str]:
    
    # n: int = 400
    # logger.info(f'text{text}')
    # url_elasticsearch = 'http://localhost:9200'
    # index = 'ja'
    # tokenizer_name = 'kuromoji_tokenizer'

    tokens = utils.tokenizer(
        text,
        url_elasticsearch,
        index,
        tokenizer_name
    )

    if len(tokens) <= n:
        return [text]
    else:
        result: list[str] = []
        for i in range(0, len(tokens), n):
            new_text: str = "".join(tokens[i : i + n])
            result.append(new_text)
        return result


def update_text_in_dict(d: WikiDict, text: str) -> WikiDict:
    d2: WikiDict = copy.deepcopy(d)
    d2["text"] = text
    return d2


def main(args: argparse.Namespace):
    
    # https://elasticsearch-py.readthedocs.io/en/v8.7.0/

    with open(args.path_analyzer, "r") as f:
        mapping: json = json.load(f)
        es: Elasticsearch = Elasticsearch("http://localhost:9200")
        es.indices.create(
            index=args.elasticsearch_index,
            body=mapping,
            ignore=400  # ignore 400 already code
        )

    documents: list[dict] = []
    for dirs, subdirs, files in os.walk(f"{args.path_export}/"):
        logger.info(f'{dirs}, {subdirs}, {files}')

        for file in files:
            file_path = f"{dirs}/{file}"
            logger.info("Processing...{file_path}")

            docs: list[documents] = file_to_document_list(file_path=file_path)
            documents += docs
    
    helpers.bulk(es, gen_bulk_data(documents))


if __name__ == "__main__":
    

    parser = argparse.ArgumentParser()
    parser.add_argument('--path_analyzer', '-PG', type=str, default='./analyzer.json', help='')
    parser.add_argument('--elasticsearch_url', '-EU', type=str, default='http://localhost:9200', help='')
    parser.add_argument('--elasticsearch_index', '-EI', type=str, default='jp', help='')
    parser.add_argument('--elasticsearch_tokenizer', '-ET', type=str, default='kuromoji_tokenizer', help='')
    parser.add_argument('--path_export', '-PE', type=str, default='./output', help='')
    args = parser.parse_args()

    main(args)