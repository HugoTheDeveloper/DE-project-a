import os


def get_content_and_extension(path):
    path = os.path.join(path)
    _, extension = os.path.splitext(path)
    # add format limits
    content = open(path).read()
    return content, extension
