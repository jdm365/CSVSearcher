from setuptools import setup, Extension
from Cython.Build import cythonize
import os


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.join(CURRENT_DIR, "../zig-out/lib")

extensions = [
    Extension(
        "radixtrie.test",
        [os.path.join(CURRENT_DIR, "radixtrie/test.pyx")],
        include_dirs=[os.path.join(CURRENT_DIR, "radixtrie")],
        library_dirs=[LIB_DIR],
        libraries=["radix_trie"],
        runtime_library_dirs=[LIB_DIR],
    )
]

setup(
    name="radixtrie",
    ext_modules=cythonize(extensions),
    zip_safe=False,
    include_package_data=False,
)
