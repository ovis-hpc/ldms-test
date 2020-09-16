BUILDDIR := ${PWD}
OVIS_PREFIX := /opt/ovis

GCC_COMMON_FLAGS = -ggdb3 -O0

TARGETS = $(BUILDDIR)/libutil.so \
	  $(BUILDDIR)/libtada.so \
	  $(BUILDDIR)/libtest_stream_sampler.so \
	  $(BUILDDIR)/test_stream_publish

LDMSD_VERSION = $(shell /usr/bin/strings ${OVIS_PREFIX}/sbin/ldmsd | \
	      grep OVIS_LDMS_VERSION | \
	      sed 's/.*"\(.*\)"/\1/' )
LDMSD_MAJOR_VERSION = $(shell echo ${LDMSD_VERSION} | cut -d . -f 1)
LDMSD_MINOR_VERSION = $(shell echo ${LDMSD_VERSION} | cut -d . -f 2)
$(info LDMSD_VERSRION ${LDMSD_VERSION})

ifeq ($(LDMSD_MINOR_VERSION), 3)
TEST_STREAM_SAMPLER_SRC = test_stream_sampler.c
else
TEST_STREAM_SAMPLER_SRC = test_stream_sampler5.c
endif

all: $(TARGETS)

clean:
	rm -f $(TARGETS)

$(BUILDDIR)/libutil.so: util.c util.h
	mkdir -p $(BUILDDIR)
	gcc -o $@ -fPIC -shared $^ ${GCC_COMMON_FLAGS}

$(BUILDDIR)/libtada.so: tada.c tada.h
	mkdir -p $(BUILDDIR)
	gcc -o $@ -fPIC -shared $^ -ldl -pthread -lcrypto ${GCC_COMMON_FLAGS}

$(BUILDDIR)/libtest_stream_sampler.so: $(TEST_STREAM_SAMPLER_SRC)
ifeq ($(LDMSD_MINOR_VERSION), 3)
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -lovis_json -ltada \
	    ${GCC_COMMON_FLAGS}
	ln -fs $@ $@.0
else
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ -fPIC -shared \
	    $^ -L$(OVIS_PREFIX)/lib -L$(BUILDDIR) -L$(OVIS_PREFIX)/lib64 \
	    -lldmsd_stream -lovis_util -lcoll -pthread -ljson_util -ltada \
	    ${GCC_COMMON_FLAGS}
	ln -fs $@ $@.0
endif

$(BUILDDIR)/test_stream_publish: test_stream_publish.c
	mkdir -p $(BUILDDIR)
	gcc -I $(OVIS_PREFIX)/include -o $@ \
		$^ -L$(OVIS_PREFIX)/lib -L$(OVIS_PREFIX)/lib64 \
		-lldms -lldmsd_stream -lovis_util -pthread ${GCC_COMMON_FLAGS}
