CFLAGS=-O3 -Wall -Wextra -g
LIBS=-lnuma -lpthread -lprotobuf

HEADERS=$(wildcard src/*.h)
PROTO=$(wildcard proto/*.proto)

PROTOSRC=$(patsubst proto/%.proto,src/%.pb.cc,$(PROTO))

SOURCES=$(wildcard src/*.c src/*.cc)

OBJECTS=$(patsubst src/%.cc,build/%.o,$(SOURCES))

TARGET=build/lazy_db

all: $(TARGET)

dev: CFLAGS = -g -Wall -Isrc -Wall -Wextra
dev: all

$(TARGET): build $(OBJECTS)
	@g++ $(CFLAGS) -o $@ $(OBJECTS) $(LIBS)

$(OBJECTS): $(HEADERS) 


build/%.o: src/%.cc
	@g++ $(CFLAGS) -c -o $@ $<

build:
	@mkdir -p build

.PHONY: clean
clean:
	@rm -rf build $(OBJECTS)
