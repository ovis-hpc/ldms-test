CFLAGS = -ggdb3
LDLIBS = -lcrypto

all: test libtada.so libtada.a

test: test.c libtada.a

libtada.so: tada.c tada.h
	$(CC) $(CFLAGS) -o $@ $< -fPIC -shared $(LDLIBS)

tada.o: tada.c

libtada.a: tada.o
	$(AR) rcs $@ $<

clean:
	rm -f test libtada.so libtada.a tada.o
