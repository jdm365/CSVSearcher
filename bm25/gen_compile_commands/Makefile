CXX = clang++
CXXFLAGS = -std=c++17 -stdlib=libc++ -fopenmp -Iinclude -Llib -lstdc++
SRCS = main.cpp ../bm25_utils.cpp
INCLUDES = ../ -I/usr/include
TARGET = bm25
LIBS = -lstdc++ -llmdb

## This generates compile_commands.json file for the lsp.
## Cython compilation causing problems.

all:
	bear -- $(CXX) $(CXXFLAGS) -o $(TARGET) $(SRCS) -I$(INCLUDES) $(LIBS)
	mv compile_commands.json ../../
	rm -f $(TARGET)
