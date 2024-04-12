CXX = clang++
CXXFLAGS = -std=c++17 -fopenmp
CXXFLAGS += -O3 -march=native -ffast-math -funroll-loops
SRCS = main.cpp ../engine.cpp ../vbyte_encoding.cpp
INCLUDES = -I../ -I/usr/include -I/usr/local/include
TARGET = bm25
LIBS = -lstdc++ -lc++ -pthread -L/usr/lib/x86_64-linux-gnu
EXTRA_CLANG_LINKER_FLAGS = -stdlib=libc++


ifeq ($(CXX), clang++)
	CXXFLAGS += $(EXTRA_CLANG_LINKER_FLAGS)
endif

SANITIZER_FLAGS = -fsanitize=address -fsanitize=undefined -fsanitize=leak

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
