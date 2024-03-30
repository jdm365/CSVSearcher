from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np
import os

MODULE_NAME = "bm25"

COMPILER_FLAGS = [
    "-std=c++17",
    "-stdlib=libc++",
    "-O3",
    "-Wall",
    "-Wextra",
    "-march=native",
    "-ffast-math",
]

OS = os.uname().sysname

LINK_ARGS = [
    "-lc++",
    "-lc++abi",
    "-L/usr/local/lib",
]

if OS == "Darwin":
    COMPILER_FLAGS += [
        "-fopenmp",
    ]
    LINK_ARGS += [
        "-fopenmp"
    ]

extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx", "bm25/engine.cpp"],
        extra_compile_args=COMPILER_FLAGS,
        language="c++",
        include_dirs=[np.get_include(), "bm25"],
        extra_link_args=LINK_ARGS,
    ),
]

setup(
    name=MODULE_NAME,
    ext_modules=cythonize(extensions),
)
