
CC	= cc
W	= -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized
THREADS = -pthread 
CFLAGS	= $(THREADS) $(W)
LDLIBS	= 
OBJS	= main.o kdb.o sqlite3.o

all: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o kdb

clean:
	rm -rf $(PROGS) main.o kdb.o *~ *.db* kdb

cleana:
	rm -rf $(PROGS) main.o sqlite3.o kdb.o *~ *.db* kdb

sqlite3.o: sqlite3.c sqlite3.h
	$(CC) $(CFLAGS) -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_JSON1 -DSQLITE_OMIT_LOAD_EXTENSION \
	-c sqlite3.c -o $@

%:	%.o
	$(CC) $(CFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@