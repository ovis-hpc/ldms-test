BUILDDIR := ${PWD}
OVIS_PREFIX := /opt/ovis

TARGETS = $(BUILDDIR)/libtada.so $(BUILDDIR)/libtest_stream_sampler.so \
	  $(BUILDDIR)/test_stream_publish

LDMSD_VERSION = $(shell /usr/bin/strings ${OVIS_PREFIX}/sbin/ldmsd | \
	      grep OVIS_LDMS_VERSION | \
	      sed 's/.*"\(.*\)"/\1/' )
LDMSD_MAJOR_VERSION = $(shell echo ${LDMSD_VERSION} | cut -d . -f 1)
LDMSD_MINOR_VERSION = $(shell echo ${LDMSD_VERSION} | cut -d . -f 2)
$(info LDMSD_VERSRION ${LDMSD_VERSION})

VMIN = $(shell echo -e "${LDMSD_VERSION}\n4.100.0" | sort -V | head -n1)

$(info VMIN ${VMIN})

ifeq ($(VMIN), 4.100.0)
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
ifeq ($(VMIN), 4.100.0)
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -ljson_util -ltada
	ln -fs $@ $@.0
else
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -lovis_json -ltada
	ln -fs $@ $@.0
endif

$(BUILDDIR)/test_stream_publish: test_stream_publish.c
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ \
		$^ -L$(OVIS_PREFIX)/lib -L$(OVIS_PREFIX)/lib64 \
		-lldms -lldmsd_stream -lovis_util -pthread
