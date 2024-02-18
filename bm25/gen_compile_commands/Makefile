## CXX = clang++ -v
CXX = g++
CXXFLAGS = -std=c++17 -fopenmp
SRCS = main.cpp ../bm25_utils.cpp
INCLUDES = -I../ -I/usr/include -I/usr/local/include
TARGET = bm25
LIBS = -lstdc++ -lleveldb -lc++ -pthread -L/usr/lib/x86_64-linux-gnu
EXTRA_CLANG_LINKER_FLAGS = -stdlib=libc++

ifeq ($(CXX), clang++)
	CXXFLAGS += $(EXTRA_CLANG_LINKER_FLAGS)
endif

SANITIZER_FLAGS = -fsanitize=address -fsanitize=undefined -fsanitize=leak

## This generates compile_commands.json file for the lsp.
## Cython compilation causing problems.

all:
	bear -- $(CXX) $(CXXFLAGS) $(INCLUDES) -o $(TARGET) $(SRCS) $(LIBS)
	cp compile_commands.json ../../
	rm -f $(TARGET)

run:
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRCS) -I$(INCLUDES) $(LIBS)
	./$(TARGET)

sanitize:
	$(CXX) $(CXXFLAGS) $(SANITIZER_FLAGS) -o $(TARGET) $(SRCS) -I$(INCLUDES) $(LIBS)
	./$(TARGET)

memprof:
	##gprof
	$(CXX) $(CXXFLAGS) -pg -o $(TARGET) $(SRCS) -I$(INCLUDES) $(LIBS)
	./$(TARGET)
	gprof $(TARGET) gmon.out > analysis.txt
	nvim analysis.txt
