import requests
import csv
import sys
import os
from io import StringIO

PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY', )


def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""

def get_fields_info(fields, language='English'):
    """Returns information about data fields"""
    url = "https://api.perplexity.ai/chat/completions"
    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}"}
    payload = {
        "model": "sonar",
        "messages": [
            {"role": "system", "content": "Be precise and concise, provide data output only CSV or JSON, accrording to request"},
            {"role": "user", "content": (
                f"Please describe in {language} these fields delimited by comma: {fields}"
                "Please output as single csv table only with following fields: name and description"
            )},
        ],
        "response_format": {
                "type": "text",
        },
    }
    response = requests.post(url, headers=headers, json=payload).json()
    text = response["choices"][0]["message"]["content"]
    a_text = find_between(text, "```csv", "```").strip()
    if len(a_text) == 0:
        a_text = find_between(text, "```", "```").strip()
    f = StringIO()
    f.write(a_text)
    f.seek(0)
    table = {}
    dr = csv.reader(f, delimiter=',')    
    n = 0
    for r in dr:
        n += 1
        if n == 1: continue
        table[r[0]] = r[1]
    return table



def get_description(data, language='English'):
    url = "https://api.perplexity.ai/chat/completions"
    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}"}
    payload = {
        "model": "sonar",
        "messages": [
            {"role": "system", "content": "Be precise and concise, provide data output only CSV or JSON, accrording to request"},
            {"role": "user", "content": (
                f"""
I have the following CSV data: 
{data}
Please provide short description about this data in English.""")},
        ],
        "response_format": {
                "type": "text",
        },
    }
    response = requests.post(url, headers=headers, json=payload).json()
    return response.text




if __name__ == "__main__":
    print(get_fields_info(sys.argv[1], sys.argv[2]))