from setuptools import setup, Extension
from Cython.Build import cythonize
import os


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.join(CURRENT_DIR, "../lib")

extensions = [
    Extension(
        "radix",
        ["test.pyx"],
        include_dirs=[CURRENT_DIR],
        library_dirs=[LIB_DIR],
        libraries=["radix_trie"],
        runtime_library_dirs=[LIB_DIR],  # Makes the library search relative to the .so file
    )
]

setup(
    name="radix",
    ext_modules=cythonize(extensions),
    zip_safe=False,
)
