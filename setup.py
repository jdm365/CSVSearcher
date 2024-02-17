from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np

MODULE_NAME = "bm25"

COMPILER_FLAGS = [
    "-std=c++11",
    "-O3",
    "-Wall",
    "-Wextra",
    "-march=native",
    "-ffast-math",
    "-fopenmp",
]

extensions = [
    Extension(
        MODULE_NAME,
        sources=["bm25/bm25.pyx"],
        extra_compile_args=COMPILER_FLAGS,
        language="c++",
        include_dirs=[np.get_include()],
        extra_link_args=["-fopenmp"],
        ##link boost libraries
    ),
]

setup(
    name="text_processing",
    ext_modules=cythonize(extensions),
)
