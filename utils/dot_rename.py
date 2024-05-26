import os


def dot_rename(path: str) -> None:
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))
