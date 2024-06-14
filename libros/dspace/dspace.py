import aiohttp
import asyncio
from rdflib import Graph, Namespace
from rdflib.namespace import DC, DCTERMS
import requests

DSPACE = Namespace('http://digital-repositories.org/ontologies/dspace/0.1.0#')
BIBO = Namespace('http://purl.org/ontology/bibo/')


class DspaceWork:
    def __init__(self, url: str):
        self.url = url
        self.handle = url.split('/')[-1]
        self.uuid = self.__get_uuid()
        self.rdf = self.__request_data().content.decode("utf-8").replace("^^xsd:dateTime", "")
        self.graph = Graph().parse(data=self.rdf, format="turtle")
        self.images = self.grab_canvases_asynchronously()
        self.metadata = self.__get_metadata()
        self.metadata_over_rest = self.__get_metadata_over_rest()
        self.labels = self.__get_labels()
        self.homepage = self.__get_homepage()

    def __request_data(self):
        headers = {
            "Accept": "text/turtle, application/turtle, application/x-turtle, application/json, text/json, text/n3,"
                      "text/dspace+n3, application/dspace+n3, application/dspace+xml, application/n-triples"
        }
        return requests.get(self.url, headers=headers)

    def get_canvases(self):
        return [
            str(o).replace(
                "https://oaktrust.library.tamu.edu/bitstream/",
                "https://api.library.tamu.edu/iiif-service/dspace/canvas/"
            )
            for s, p, o in self.graph if p == DSPACE.hasBitstream and '.jpf' in str(o)
        ]

    @staticmethod
    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.json()

    async def fetch_all(self, urls):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch(session, url) for url in urls]
            responses = await asyncio.gather(*tasks)
            return responses

    async def process_canvases(self):
        urls = self.get_canvases()
        responses = await self.fetch_all(urls)
        result = [response["images"][0]["@id"] for response in responses]
        return result

    def grab_canvases_asynchronously(self):
        return asyncio.run(self.process_canvases())

    def __get_contributors(self):
        return [
            str(o) for s, p, o in self.graph if p == DC.contributor
        ]

    def __get_languages(self):
        return [
            str(o) for s, p, o in self.graph if p == DC.language
        ]

    def __get_publishers(self):
        return [
            str(o) for s, p, o in self.graph if p == DC.publisher
        ]

    def __get_alternatives(self):
        return [
            str(o) for s, p, o in self.graph if p == DCTERMS.alternative
        ]

    def __get_created_dates(self):
        return [
            str(o) for s, p, o in self.graph if p == DCTERMS.created
        ]

    def __get_labels(self):
        return [
            str(o) for s, p, o in self.graph if p == DCTERMS.title
        ]

    def __get_homepage(self):
        return [
            str(o) for s, p, o in self.graph if p == BIBO.uri
        ]

    def __get_metadata(self):
        return {
            "Alternative Titles": self.__get_alternatives(),
            "Contributors": self.__get_contributors(),
            "Created date": self.__get_created_dates(),
            "Language": self.__get_languages(),
            "Publisher": self.__get_publishers(),
        }

    def __get_uuid(self):
        r = requests.get(f"https://oaktrust.library.tamu.edu/rest/handle/1969.1/{self.handle}").json()
        return r['uuid']

    def __get_metadata_over_rest(self):
        data = {
            "Subjects": {
                "en": [],
                "es": [],
            },
            "Description": {
                "en": [],
                "es": [],
            }
        }
        r = requests.get(
            f"https://oaktrust.library.tamu.edu/rest/items/{self.uuid}/metadata"
        ).json()
        for entry in r:
            if entry['key'] == 'dc.subject.other' or entry['key'] == 'dc.subject':
                data['Subjects'][entry['language']].append(entry['value'])
            elif entry['key'] == 'dc.description':
                data['Description'][entry['language']].append(entry['value'])
        return data


if __name__ == "__main__":
    x = DspaceWork("https://oaktrust.library.tamu.edu/rdf/handle/1969.1/92214")
    print(x.metadata_over_rest)