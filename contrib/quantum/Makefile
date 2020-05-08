# contrib/quantum/Makefile
UNAME_S := $(shell uname -s)
PG_CPPFLAGS := -march=native -fPIC -I/usr/local/include


MODULE_big = quantum
OBJS=utils_simd.o array.o  hnswfunc.o hnutil.o hninsert.o hnscan.o hnvacuum.o util.o hnpage.o $(WIN32RES)

EXTENSION = quantum
DATA = quantum--1.3.sql
PGFILEDESC = "quantum - multidimensional analyse extension"

HEADERS = hnsw.h

REGRESS = quantum partial_index


#CXX = c++ -std=c++11 -I/usr/local/include -I./hopscotch-map-1.4.0/src


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/quantum
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

SHLIB_LINK += $(filter -lm, $(LIBS))
SHLIB_LINK += -lstdc++

override CXXFLAGS += -std=c++11 -O3

%.bc : %.cc
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<

# util.o: override CXXFLAGS += -std=c++11

wal-check: temp-install
	$(prove_check)


maintainer-clean:
	rm -f quantum.so

test: test.cc
	g++ -std=c++11 -o test test.cc util.o -lgtest
	chmod +x test


