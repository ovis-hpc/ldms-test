BUILDDIR := ${PWD}
OVIS_PREFIX := /opt/ovis

TARGETS = $(BUILDDIR)/libtada.so $(BUILDDIR)/libtest_stream_sampler.so

all: $(TARGETS)

clean:
	rm -f $(TARGETS)

$(BUILDDIR)/libtada.so: tada.c tada.h
	mkdir -p $(BUILDDIR)
	gcc -o $@ -fPIC -shared $^ -ldl -pthread -lcrypto

$(BUILDDIR)/libtest_stream_sampler.so: test_stream_sampler.c
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -ljson_util -ltada
	ln -fs $@ $@.0
