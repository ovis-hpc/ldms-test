BUILDDIR := ${PWD}
OVIS_PREFIX := /opt/ovis

TARGETS = $(BUILDDIR)/libtada.so $(BUILDDIR)/libtest_stream_sampler.so

LDMSD_VERSION = $(shell /usr/bin/strings ${OVIS_PREFIX}/sbin/ldmsd | \
	      grep OVIS_LDMS_VERSION | \
	      sed 's/.*"\(.*\)"/\1/' )
LDMSD_MAJOR_VERSION = $(shell echo ${LDMSD_VERSION} | cut -d . -f 1)
$(info LDMSD_VERSRION ${LDMSD_VERSION})


ifeq ($(LDMSD_MAJOR_VERSION), 5)
TEST_STREAM_SAMPLER_SRC = test_stream_sampler5.c
else
TEST_STREAM_SAMPLER_SRC = test_stream_sampler.c
endif

all: $(TARGETS)

clean:
	rm -f $(TARGETS)

$(BUILDDIR)/libtada.so: tada.c tada.h
	mkdir -p $(BUILDDIR)
	gcc -o $@ -fPIC -shared $^ -ldl -pthread -lcrypto

$(BUILDDIR)/libtest_stream_sampler.so: $(TEST_STREAM_SAMPLER_SRC)
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -ljson_util -ltada
	ln -fs $@ $@.0
