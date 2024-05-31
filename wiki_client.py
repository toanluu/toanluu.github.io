import json
import urllib.parse

import requests


class WikiConcept:
    PUBLIC_COMPANY = 'Q891723'
    COUNTRY = 'Q6256'


class WikiProperty:
    INSTANCE_OF = 'P31'
    COUNTRY = 'P17'


class Wikidata:
    def __init__(self):
        pass

    def sparql(self, payload):
        url = 'https://query.wikidata.org/sparql?query=%s' % urllib.parse.quote(payload)
        return requests.get(url=url, headers={'Accept': 'application/sparql-results+json'}).json()

    def get_intance_of_concept(self, concept):
        query = """
        SELECT ?item
        WHERE
        {
          ?item wdt:%s wd:%s.
        }
        """ % (WikiProperty.INSTANCE_OF, concept)
        res = wiki.sparql(query)

    def get_intances(self, property, concept):
        query = """
        SELECT ?item
        WHERE
        {
          ?item wdt:%s wd:%s.
        }
        """ % (property, concept)
        res = wiki.sparql(query)
        items = []
        for r in res['results']['bindings']:
            item = r['item']
            items.append({
                'uri': item['value'],
                'id': item['value'][len('http://www.wikidata.org/entity/'):]
            })
        return items


def check_sparql():
    wiki = Wikidata()
    example = """
        #Cats
        SELECT ?item ?itemLabel
        WHERE
        {
          ?item wdt:P31 wd:Q146. # Must be a cat
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". } # Helps get the label in your language, if not, then en language
        }
    """
    print(json.dumps(wiki.sparql(example), indent=2))


if __name__ == "__main__":
    wiki = Wikidata()

    # print(json.dumps(wiki.get_intance_of_concept(WikiConcept.PUBLIC_COMPANY), indent=2))
    print(json.dumps(wiki.get_intances(WikiProperty.INSTANCE_OF, WikiConcept.COUNTRY), indent=2))
    # check_sparql()
