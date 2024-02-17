CXX = clang++
CXXFLAGS = -std=c++11 -stdlib=libc++ -fopenmp -Iinclude -Llib -lstdc++
SRCS = main.cpp ../bm25_utils.cpp
INCLUDES = ../ 
TARGET = bm25

## This generates compile_commands.json file for the lsp.
## Cython compilation causing problems.

all:
	bear -- $(CXX) $(CXXFLAGS) -o $(TARGET) $(SRCS) -I$(INCLUDES)
	mv compile_commands.json ../../
	rm -f $(TARGET)
