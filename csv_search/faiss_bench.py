import numpy as np
from tqdm import tqdm
import faiss
import time

# Parameters
num_vectors = 2_000_000  # 2M vectors
dim = 3072  # 3072 bits
num_queries = 1000
k = 10
batch_size = 100

# IVF params
nlist = 1000  # Number of clusters/cells
nprobe = 10   # Number of cells to search

# HNSW params
M = 32        # Number of connections per layer
efConstruction = 40  # Size of dynamic candidate list for construction
efSearch = 16      # Size of dynamic candidate list for search

# Create data
vectors = np.random.randint(0, 2, size=(num_vectors, dim), dtype=np.uint8)
queries = np.random.randint(0, 2, size=(num_queries, dim), dtype=np.uint8)

# Pack bits to bytes
bytes_per_vec = (dim + 7) // 8
packed_vectors = np.packbits(vectors, axis=1)[:, :bytes_per_vec]
packed_queries = np.packbits(queries, axis=1)[:, :bytes_per_vec]

def benchmark_index(index, name):
    # Add vectors
    start_time = time.time()
    index.train(packed_vectors)
    index.add(packed_vectors)
    build_time = time.time() - start_time
    
    # Warmup
    _, _ = index.search(packed_queries[:1], k)
    
    # Single query benchmark
    start_time = time.time()
    for i in tqdm(range(num_queries)):
        _, _ = index.search(packed_queries[i:i+1], k)
    single_time = time.time() - start_time
    single_qps = num_queries / single_time
    
    # Batch query benchmark
    num_batches = num_queries // batch_size
    start_time = time.time()
    for i in tqdm(range(num_batches)):
        batch_start = i * batch_size
        batch_end = batch_start + batch_size
        _, _ = index.search(packed_queries[batch_start:batch_end], k)
    batch_time = time.time() - start_time
    batch_qps = (num_batches * batch_size) / batch_time
    
    print(f"\n{name} Results:")
    print(f"Build time: {build_time:.2f}s")
    print(f"Single query QPS: {single_qps:.2f}")
    print(f"Batched query QPS (batch_size={batch_size}): {batch_qps:.2f}")
    
    # Check sample results
    distances, indices = index.search(packed_queries[0:1], k)
    return distances[0], indices[0]

# Benchmark Flat index
## flat_index = faiss.IndexBinaryFlat(dim)
## flat_d, flat_i = benchmark_index(flat_index, "Flat Index")

# Benchmark IVF index
ivf_index = faiss.IndexBinaryIVF(faiss.IndexBinaryFlat(dim), dim, nlist)
ivf_index.nprobe = nprobe
ivf_d, ivf_i = benchmark_index(ivf_index, "IVF Index")

# Benchmark HNSW index
hnsw_index = faiss.IndexBinaryHNSW(dim)
hnsw_index.hnsw.efConstruction = efConstruction
hnsw_index.hnsw.efSearch = efSearch
hnsw_d, hnsw_i = benchmark_index(hnsw_index, "HNSW Index")

# Print sample results comparison
print("\nSample Query Results Comparison:")
print(f"Flat:  D={flat_d}, I={flat_i}")
print(f"IVF:   D={ivf_d}, I={ivf_i}")
print(f"HNSW:  D={hnsw_d}, I={hnsw_i}")

# Memory usage (approximate)
print("\nApproximate Memory Usage:")
print(f"Flat:  {flat_index.sa_code_size() / (1024*1024):.1f} MB")
print(f"IVF:   {ivf_index.sa_code_size() / (1024*1024):.1f} MB")
print(f"HNSW:  {hnsw_index.sa_code_size() / (1024*1024):.1f} MB")
