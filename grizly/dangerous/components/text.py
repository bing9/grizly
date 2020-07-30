from yattag import Doc

def title(title):
    doc, tag, text = Doc().tagtext()

    with tag('h1'):
        doc.text(title)
    return doc