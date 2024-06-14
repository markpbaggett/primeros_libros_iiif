import requests
from iiif_prezi3 import Manifest, config, KeyValueString
from libros import DspaceWork
import json
from time import sleep


class IIIFManifest:
    def __init__(self, uri):
        self.uri = uri
        self.config = config.configs['helpers.auto_fields.AutoLang'].auto_lang = "en"
        self.dspace_data = DspaceWork(uri)
        self.manifest = self.__build_manifest()

    def __build_manifest(self):
        manifest = Manifest(
            id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json",
            label=self.dspace_data.labels,
            metadata=self.__build_metadata(),
            rights="http://rightsstatements.org/vocab/NoC-US/1.0/",
            homepage=self.__build_home_page()
        )
        i = 1
        for canvas in self.dspace_data.images:
            try:
                manifest.make_canvas_from_iiif(
                    url=canvas,
                    label=f"Page {i}",
                    id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}",
                    anno_id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}/annotation/1",
                    anno_page_id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}/canvas/{i}/annotation/1/page/1",
                )
            except requests.exceptions.HTTPError:
                sleep(30)
                manifest.make_canvas_from_iiif(
                    url=canvas,
                    label=f"Page {i}",
                    id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}",
                    anno_id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}/annotation/1",
                    anno_page_id=f"https://raw.githubusercontent.com/markpbaggett/static_iiif/main/manifests/tamu/{self.dspace_data.uuid}.json/canvas/{i}/canvas/{i}/annotation/1/page/1",
                )
            i += 1
        x = manifest.json(indent=2)
        manifest_as_json = json.loads(x)
        return manifest_as_json

    def write(self):
        with open(f'data/{self.dspace_data.uuid}.json', 'w') as outfile:
            outfile.write(
                json.dumps(
                    self.manifest, indent=2)
            )

    def __build_metadata(self):
        metadata = []
        for k, v in self.dspace_data.metadata_over_rest.items():
            metadata.append(
                KeyValueString(
                    label=k,
                    value=v
                )
            )
        for k, v in self.dspace_data.metadata.items():
            metadata.append(
                KeyValueString(
                    label=k,
                    value={"en": v}
                )
            )
        return metadata

    def __build_home_page(self):
        return [
            {
                "id": self.dspace_data.homepage[0],
                "type": "Text",
                "label": { "en": [ "View Item in DSPACE" ] },
                "format": "text/html"
            }
        ]


if __name__ == "__main__":
    x = IIIFManifest("https://oaktrust.library.tamu.edu/rdf/handle/1969.1/92214")
    x.write()
