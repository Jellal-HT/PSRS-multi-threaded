CC=gcc
TARGET=main.o
CFLAGS=-Wall -O
LFLAGS=-lpthread
SOURCES=main.c

all:
	$(CC) $(CFLAGS) $(SOURCES) $(LFLAGS) -o $(TARGET)
compile:
	$(CC) $(CFLAGS) $(SOURCES) $(LFLAGS) -o $(TARGET)
run: 
	./$(TARGET) $(args)
clean:
	rm $(TARGET) 
