from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension
import os

MODULE_NAME = "bloom25"

## Optionally choose compiler ##
COMPILER_FLAGS = [
    "-std=c++17",
    "-O3",
    "-Wall",
    "-Wextra",
    "-march=native",
    "-ffast-math",
    "-g",
]

'''
COMPILER = "clang++"
## COMPILER = "g++"
os.environ["CXX"] = COMPILER

COMPILER = os.environ["CXX"]

if COMPILER == "clang++":
    COMPILER_FLAGS += [
            "-stdlib=libc++"
    ]
elif COMPILER == "g++":
    COMPILER_FLAGS += [
            "-I/usr/local/include"
    ]
'''


OS = os.uname().sysname

LINK_ARGS = [
    "-lc++",
    "-lc++abi",
    "-L/usr/local/lib",
]


extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx", "bm25/engine.cpp", "bm25/vbyte_encoding.cpp", "bm25/serialize.cpp", "bm25/bloom.cpp"],
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
