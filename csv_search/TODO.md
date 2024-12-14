Idea: This is already something. LSH or something.
      Represent docs by 1024 element (or something) bit vectors. Create N bit vectors where elements are inserted at hash(token) % 1024 for N different hashing functions.
      Concat bit vectors, do hamming distance similarity. Maybe cluster after.
