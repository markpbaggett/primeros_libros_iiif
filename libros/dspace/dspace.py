import aiohttp
import asyncio
from rdflib import Graph, Namespace
from rdflib.namespace import DC, DCTERMS
import requests
from aiohttp import ClientTimeout

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
        self.rendering = self.get_rendering()

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

    def get_rendering(self):
        return [
            str(o).replace(
                "https://oaktrust.library.tamu.edu/bitstream/",
                "https://api.library.tamu.edu/iiif-service/dspace/canvas/"
            )
            for s, p, o in self.graph if p == DSPACE.hasBitstream and '.pdf' in str(o)
        ]

    async def fetch(self, session, url, index, retries=3, backoff=5):
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return index, await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Error fetching {url}: {e} (attempt {attempt + 1} of {retries})")
                if attempt == retries - 1:
                    return index, None
                await asyncio.sleep(backoff)

    async def fetch_all(self, urls):
        timeout = ClientTimeout(total=60)  # Set a total timeout of 60 seconds
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [self.fetch(session, url, index) for index, url in enumerate(urls)]
            responses = await asyncio.gather(*tasks)
            responses.sort(key=lambda x: x[0])
            return [response for index, response in responses if response]

    async def process_canvases(self):
        urls = self.get_canvases()
        urls = sorted(urls)
        responses = await self.fetch_all(urls)
        result = [response["images"][0]["@id"] for response in responses if response]
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
        try:
            r = requests.get(f"https://oaktrust.library.tamu.edu/rest/handle/1969.1/{self.handle}").json()
            return r['uuid']
        except requests.RequestException as e:
            print(f"Error fetching UUID for {self.handle}: {e}")
            return None

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
        try:
            r = requests.get(
                f"https://oaktrust.library.tamu.edu/rest/items/{self.uuid}/metadata"
            ).json()
            for entry in r:
                if entry['key'] == 'dc.subject.other' or entry['key'] == 'dc.subject':
                    data['Subjects'][entry['language']].append(entry['value'])
                elif entry['key'] == 'dc.description':
                    data['Description'][entry['language']].append(entry['value'])
        except requests.RequestException as e:
            print(f"Error fetching metadata for {self.uuid}: {e}")
        return data


if __name__ == "__main__":
    x = DspaceWork("https://oaktrust.library.tamu.edu/rdf/handle/1969.1/92820")
    print(x.images)
