from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension
import os

## MODULE_NAME = "bm25"
MODULE_NAME = "rapid_bm25"

## Optionally choose compiler ##
'''
COMPILER = "clang++"
## COMPILER = "g++"
os.environ["CXX"] = COMPILER
'''

COMPILER = os.environ["CXX"]

COMPILER_FLAGS = [
    "-std=c++17",
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

if COMPILER == "clang++":
    COMPILER_FLAGS += [
            "-stdlib=libc++"
    ]

extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx", "bm25/engine.cpp", "bm25/vbyte_encoding.cpp"],
        extra_compile_args=COMPILER_FLAGS,
        language="c++",
        include_dirs=["bm25"],
        extra_link_args=LINK_ARGS,
    ),
]

setup(
    name=MODULE_NAME,
    version="0.1.0",
    author="Jake Mehlman",
    ext_modules=cythonize(extensions),
    include_package_data=True,
    package_data={
        "bm25": ["*.txt"]
        },
)
