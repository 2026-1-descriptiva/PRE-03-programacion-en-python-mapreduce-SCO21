"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby


# small helper to flatten iterables (replacement for toolz.concat)
def concat(seqs):
    for seq in seqs:
        for item in seq:
            yield item



def copy_raw_files_to_input_folder(n):

    """Generate n copies of the raw files in the input folder"""
    # ensure input directory is clean
    input_dir = os.path.join("files", "input")
    if os.path.exists(input_dir):
        # remove any existing files
        for f in glob.glob(os.path.join(input_dir, "*")):
            try:
                os.remove(f)
            except Exception:
                pass
    else:
        os.makedirs(input_dir, exist_ok=True)

    raw_paths = glob.glob(os.path.join("files", "raw", "*"))
    for i in range(n):
        for src in raw_paths:
            basename = os.path.basename(src)
            dest = os.path.join(input_dir, f"{i}_{basename}")
            with open(src, "rb") as fsrc, open(dest, "wb") as fdst:
                fdst.write(fsrc.read())

def load_input(input_directory):
    """Funcion load_input"""
    # generator yielding every line from every file in the input directory
    pattern = os.path.join(input_directory, "*")
    for path in glob.glob(pattern):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                yield line


def preprocess_line(x):
    """Preprocess the line x"""
    # normalize case and strip punctuation
    import re
    x = x.lower()
    # replace any non-letter with space
    x = re.sub(r"[^a-zA-Z]+", " ", x)
    return x.strip()


def map_line(x):
    """Take a raw line and return (word,1) pairs"""
    x = preprocess_line(x)
    for word in x.split():
        yield (word, 1)

def mapper(sequence):
    """Mapper"""
    # flatten the sequence of iterables produced by map_line
    return concat(map(map_line, sequence))


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    # sort by key and group values
    sorted_seq = sorted(sequence, key=lambda x: x[0])
    result = []
    for key, group in groupby(sorted_seq, key=lambda x: x[0]):
        # group is an iterator of (key, value) pairs
        values = [v for (_k, v) in group]
        result.append((key, values))
    return result


def compute_sum_by_group(group):
    # group is a tuple (key, [values])
    key, values = group
    return (key, sum(values))

def reducer(sequence):
    """Reducer"""
    # sequence is list of (key, [values])
    return [compute_sum_by_group(group) for group in sequence]


def create_directory(directory):
    """Create Output Directory"""
    os.makedirs(directory, exist_ok=True)


def save_output(output_directory, sequence):
    """Save Output"""
    path = os.path.join(output_directory, "part-00000")
    with open(path, "w", encoding="utf-8") as f:
        # sort keys for deterministic output
        for key, value in sorted(sequence, key=lambda x: x[0]):
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create Marker"""
    path = os.path.join(output_directory, "_SUCCESS")
    with open(path, "w", encoding="utf-8"):
        pass


def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
