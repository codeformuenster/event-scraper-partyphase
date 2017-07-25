import json, hashlib
from pathlib import Path
import elasticsearch, elasticsearch.helpers


# FIXME lookup parkleit

lookup = json.loads(Path("./partyphase_locations.json").read_text())

    # missing:

    # Schwarzes Schaf
    # Konzertsaal Friedenskapelle
    # Arena Berlin
    # Boulevard Münster
    # CubaCultur
    # Alter Pulverturm
    # Whisky Dungeon Münster
    # BLACK BOX im cuba, Achtermannstraße 12, 48143 Münster
    # NOBIS KRUG Sepia 5.12
    # Münster – Unikino
    # Pool Hall
    # Café Nobis
    # Elephant Lounge
    # Bahnhof Wolbeck


es = elasticsearch.Elasticsearch(['https://elasticsearch.kube.codeformuenster.org'])

def process_event_stream(file):
    for line in file.readlines():

        event = json.loads(line)
        if event['location']['name'] in lookup:
            event['location'] = lookup[event['location']['name']]

        # FIXME filter
        for k, v in list(event.items()):
            if not v:
                del event[k]

        # print(json.dumps(event, ensure_ascii=False)) # FIXME log debug
        md5 = hashlib.md5(json.dumps(event, sort_keys=True).encode('utf8')).hexdigest()
        yield md5, event


with open("/data/events.jsonl", "r") as file:

    actions = ({
        '_op_type': 'create',
        '_index': 'party-test-hash-lat-lon2',
        '_type': 'event',
        '_id': md5,
        "_source": event,
        } for md5, event in process_event_stream(file))

    result = elasticsearch.helpers.bulk(es, actions, raise_on_error=False, stats_only=True)
    # FIXME expand_action_callback
    # print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"successful: {result[0]}, failed: {result[1]}")
