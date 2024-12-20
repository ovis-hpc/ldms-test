
DESTDIR = .

CFLAGS = -ggdb3 -O0 -I/opt/ovis/include -L/opt/ovis/lib -lldms -lserdes -lavro

all: $(DESTDIR)/pub $(DESTDIR)/sub

$(DESTDIR)/pub: pub.c
	gcc $< -o $@ $(CFLAGS)

$(DESTDIR)/sub: sub.c
	gcc $< -o $@ $(CFLAGS)

clean:
	rm -f $(DESTDIR)/pub $(DESTDIR)/sub
