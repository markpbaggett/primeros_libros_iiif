from libros.iiif import IIIFManifest
import tqdm

with open('handles.txt', 'r') as f:
    handles = f.read().split('\n')
    for handle in tqdm.tqdm(handles):
        x = IIIFManifest(f"https://oaktrust.library.tamu.edu/rdf/handle/1969.1/{handle}")
        x.write()
