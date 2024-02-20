from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np

MODULE_NAME = "bm25"

COMPILER_FLAGS = [
    "-std=c++17",
    "-stdlib=libc++",
    "-O3",
    "-Wall",
    "-Wextra",
    "-march=native",
    "-ffast-math",
    "-fPIC",
]

SANITIZER_FLAGS = [
    "-fsanitize=address",
    "-fsanitize=leak",
    "-fsanitize=undefined",
    "-fsanitize=thread",
]


extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx", "bm25/bm25_utils.cpp"],
        extra_compile_args=COMPILER_FLAGS,
        language="c++",
        include_dirs=[np.get_include(), "bm25"],
        extra_link_args=["-fopenmp", "-lc++", "-lc++abi", "-lleveldb", "-L/usr/local/lib", "-lsnappy"],
    ),
]

setup(
    name=MODULE_NAME,
    ext_modules=cythonize(extensions),
)
