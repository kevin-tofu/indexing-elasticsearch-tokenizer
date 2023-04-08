import requests

def tokenizer(
    text: list[str],
    url:str,
    index:str,
    tokenizer_es: str,
):
    headers = {
        'Content-Type': 'application/json'
    }

    json_data = {
        'tokenizer': {
            'type': tokenizer_es
        },
        'text': text
    }

    res = requests.get(
        f"{url}/{index}/_analyze",
        headers=headers,
        json=json_data
    )

    assert res.status_code == 200
    ret = [r['token'] for r in res.json()['tokens']]
    return ret


if __name__ == '__main__':

    url = 'http://localhost:9200'
    index = 'ja'
    tokenizer_name = 'kuromoji_tokenizer'

    tokenizer(
        '今日は晴れのち曇りです',
        url,
        index,
        tokenizer_name
    )