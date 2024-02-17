from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np
import os

MODULE_NAME = "bm25"

os.environ["CC"]  = "clang"
os.environ["CXX"] = "clang++"

COMPILER_FLAGS = [
    "-std=c++11",
    "-O3",
    "-Wall",
    "-Wextra",
    "-march=native",
    "-ffast-math",
    "-fopenmp",
    "-lstdc++",
]


extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx", "bm25/bm25_utils.cpp"],
        extra_compile_args=COMPILER_FLAGS,
        language="c++",
        include_dirs=[np.get_include(), "bm25"],
        extra_link_args=["-fopenmp", "-lstdc++"],
        ##link boost libraries
    ),
]

setup(
    name="text_processing",
    ext_modules=cythonize(extensions),
)
