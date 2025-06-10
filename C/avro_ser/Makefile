
DESTDIR = .

CFLAGS = -ggdb3 -O0 -I/opt/ovis/include -L/opt/ovis/lib -lldms \
	 -lldms_msg_avro_ser -lserdes -lavro \
	 -D_REF_TRACK_

all: $(DESTDIR)/pub $(DESTDIR)/sub

$(DESTDIR)/pub: pub.c
	gcc $< -o $@ $(CFLAGS)

$(DESTDIR)/sub: sub.c
	gcc $< -o $@ $(CFLAGS)

clean:
	rm -f $(DESTDIR)/pub $(DESTDIR)/sub
