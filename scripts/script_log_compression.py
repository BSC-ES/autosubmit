import zipfile
import matplotlib.pyplot as plt
from autosubmit.log.log import Log
import time
import glob
import os
import zstandard as zstd
import numpy as np

t = 3 # number of times to repeat measurement

file_path = "scripts/original_files/"
for filepath in glob.glob(os.path.join(file_path, "*.xz")):
    os.remove(filepath)
filenames = [os.path.join(file_path, f) for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))] 
original_size = [os.stat(f).st_size for f in filenames] # in bytes

sorted_indices = sorted(range(len(original_size)), key=lambda i: original_size[i])
original_size = [original_size[i] for i in sorted_indices]
filenames = [filenames[i] for i in sorted_indices]

time_xz = np.zeros(len(filenames))
min_xz = np.full(len(filenames), 1000)
max_xz = np.zeros(len(filenames))
compressed_size_xz = np.empty(len(filenames))

time_zstd = np.zeros(len(filenames))
min_zstd = np.full(len(filenames), 1000)
max_zstd = np.zeros(len(filenames))
compressed_size_zstd = np.empty(len(filenames))

time_zipfile = np.zeros(len(filenames))
min_zipfile = np.full(len(filenames), 1000)
max_zipfile = np.zeros(len(filenames))
compressed_size_zipfile = np.empty(len(filenames))

for i, f in enumerate(filenames):
    for _ in range(t):
        for filepath in glob.glob(os.path.join(file_path, "*.xz")):
            os.remove(filepath)

        with open(f, "rb") as file:
            data = file.read()
        start_time = time.time()
        # zstd algoritm
        cctx = zstd.ZstdCompressor()
        compressed = cctx.compress(data)
        end_time = time.time()

        elapsed = end_time-start_time
        time_zstd[i] += elapsed
        if elapsed > max_zstd[i]: 
             max_zstd[i] = elapsed
        if elapsed < min_zstd[i]: 
             min_zstd[i] = elapsed

        start_time = time.time()
        # xz algorithm
        Log.compress_logfile(f)
        end_time = time.time()

        elapsed = end_time-start_time
        time_xz[i] += elapsed 
        if elapsed > max_xz[i]: 
             max_xz[i] = elapsed
        if elapsed < min_xz[i]: 
             min_xz[i] = elapsed

        start_time = time.time()
        # stdlib Python
        zipped = zipfile.ZipFile(f+".zip", "w", zipfile.ZIP_DEFLATED)
        zipped.write(f)
        zipped.close()
        end_time = time.time()

        elapsed = end_time-start_time
        time_zipfile[i] += elapsed
        if elapsed > max_zipfile[i]: 
             max_zipfile[i] = elapsed
        if elapsed < min_zipfile[i]: 
             min_zipfile[i] = elapsed


    time_xz[i] /= t
    compressed_size_xz[i] = os.stat(f+".xz").st_size

    time_zstd[i] /= t
    compressed_size_zstd[i] = len(compressed)

    time_zipfile[i] /= t
    compressed_size_zipfile[i] = os.stat(f+".zip").st_size


plt.errorbar(original_size,
        time_xz,
        yerr=[min_xz, max_xz],
        fmt="r--o",
        label='xz',
        ecolor = "gray")

plt.errorbar(original_size,
        time_zstd,
        yerr=[min_zstd, max_zstd],
        fmt="b--+",
        label='zstd',
        ecolor = "gray")

plt.errorbar(original_size,
        time_zipfile,
        yerr=[min_zipfile, max_zipfile],
        fmt="g--*",
        label='zipfile',
        ecolor = "gray")

plt.xlabel("Original file size [bytes]")
plt.ylabel("Elapsed time to compress [s]")
plt.yscale('log')
plt.title("Compression algorithms time comparison")
plt.legend()
plt.savefig('scripts/compression_algorithms_time_results.png')
plt.close()

plt.scatter(original_size,
        compressed_size_xz,
        c = "r",
        marker= "o", 
        label = "xz")

plt.scatter(original_size,
        compressed_size_zstd,
        c="b",
        marker="+",
        label="zstd")

plt.scatter(original_size,
        compressed_size_zipfile,
        c="g",
        marker="*",
        label="zipfile")

plt.xlabel("Original file size [bytes]")
plt.ylabel("Compressed file size [bytes]")
plt.title("Compression algorithms size comparison")
plt.legend()
plt.xlim(0, 8e6)
plt.ylim(0, 1e6)
# plt.gca().set_aspect('equal', adjustable='box')
plt.savefig('scripts/compression_algorithms_size_results.png')
plt.close()
