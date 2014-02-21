CFLAGS=-O2 -Werror -Wextra -g -std=c++0x 
LIBS=-lnuma -lpthread -lprotobuf -lrt -lcityhash -ltcmalloc_minimal

INCLUDE=include
SRC=src
HEADERS=$(wildcard $(INCLUDE)/*.hh $(INCLUDE)/*.h)

SOURCES=$(wildcard $(SRC)/*.cc $(SRC)/*.c)

OBJECTS=$(patsubst $(SRC)/%.cc,build/%.o,$(SOURCES))

TARGET=build/lazy_db

all: $(TARGET)

dev: CFLAGS = -g -Werror -Wextra -std=c++0x
dev: all

$(TARGET): build $(OBJECTS)
	g++ $(CFLAGS) -I$(INCLUDE) -o $@ $(OBJECTS) $(LIBS)

$(OBJECTS): $(HEADERS) 

build/%.o: src/%.cc
	g++ $(CFLAGS) -I$(INCLUDE) -c -o $@ $<

build:
	mkdir -p build

.PHONY: clean
clean:
	rm -rf build $(OBJECTS)
