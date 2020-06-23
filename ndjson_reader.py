#!/usr/bin/env python3


import sys
import multiprocessing as mp
import ujson as json


def process_chunk(q):

    while True:
        chunk = q.get()

        if chunk is None or len(chunk) == 0:
            return

        objs = chunk.split(b'\n')

        for obj in objs:
            j = json.loads(obj)
            # Do something with each JSON


# Number of workers to spawn
THREADS = 8

# CHUNK_SIZE should be a size approximately 100x larger than the average json object size
CHUNK_SIZE = 4096 * 128

q = mp.Manager().Queue(THREADS * 4)
workers = []

for i in range(THREADS):
    w = mp.Process(target=process_chunk, args=(q,))
    workers.append(w)
    w.start()

buf = b''

while True:

    data = sys.stdin.buffer.read(CHUNK_SIZE)
    pos = data.rfind(b'\n')
    chunk = buf + data[0:pos]
    buf = data[pos+1:]
    q.put(chunk)

    if not data:
        break

for x in range(THREADS):
    q.put(None)

for w in workers:
    w.join()
