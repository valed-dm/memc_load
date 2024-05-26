import gzip
from typing import Generator


def line_generator(file_path: str) -> Generator[str, None, None]:
    with gzip.open(file_path, 'rt', encoding='utf-8') as fd:
        for line in fd:
            yield line.strip()
