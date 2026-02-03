import requests, json, re

def get_dataset_list(target, reaction):
    url = 'https://nds.iaea.org/exfor/x4list?Target=' + target + '&Reaction=' + reaction + '&Quantity=SIG&json'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}

    r = requests.get(url, headers=headers)

    # decode properly
    text = r.content.decode("ISO-8859-1", errors="replace")

    # 1. Remove control characters
    text = re.sub(r"[\x00-\x1F\x7F]", "", text)

    # 2. Fix missing numeric fields like "enMin":,
    text = re.sub(r'"([A-Za-z0-9_]+)":\s*,', r'"\1": null,', text)

    # 3. Fix trailing commas
    text = text.replace(",]", "]").replace(",}", "}")

    # Parse cleaned JSON
    data_list = json.loads(text)['x4Datasets']

    return data_list

def get_data(dataset_id):
    url = 'https://nds.iaea.org/exfor/x4get?DatasetID=' + dataset_id + '&op=x4jsfy'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}

    r = requests.get(url, headers=headers)

    # decode properly
    text = r.content.decode("ISO-8859-1", errors="replace")

    # 1. Remove control characters
    text = re.sub(r"[\x00-\x1F\x7F]", "", text)

    # 2. Fix missing numeric fields like "enMin":,
    text = re.sub(r'"([A-Za-z0-9_]+)":\s*,', r'"\1": null,', text)

    # 3. Fix trailing commas
    text = text.replace(",]", "]").replace(",}", "}")

    # Parse cleaned JSON
    data = json.loads(text)

    print("Loaded JSON successfully!")

    return data