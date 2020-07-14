from yattag import Doc, indent
from .components import text

class RFrame():
    def __init__(self):
        self.doc = {}

    def to_page(self):
        doc, tag, text = Doc().tagtext()
        doc.asis('<!DOCTYPE html>')
        with tag('html'):
            with tag('body'):
                doc.asis(self.doc["header"])
        self.doc["html"] = indent(doc.getvalue())
        return self

    def add_title(self, title: str):
        doc = text.title(title)
        self.doc["header"] = doc.getvalue()
        return self