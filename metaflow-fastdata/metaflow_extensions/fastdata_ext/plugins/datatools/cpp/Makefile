CC=gcc
CXX=g++
CFLAGS=-I$(SRC) -Ivendor/arrow/cpp/src -Ivendor/avro/lang/c/src/ -Wall -Wextra -O3 -fPIC
CXXFLAGS=-I$(SRC) -Ivendor/arrow/cpp/src -Ivendor_build/arrow/build/src/ -Ivendor/avro/lang/c/src/ -std=c++17 -Wall -O3 -fPIC
ARROWLIBS=-L$(ARROWBUILD)/release -lparquet -larrow_dataset -larrow_acero -larrow -larrow_bundled_dependencies
AVROLIBS=-L$(AVROBUILD)/release/lib -lavro -ljansson
SYSTEMLIBS=-lz -llzma
ifeq ($(shell uname),Darwin)
LIBS=$(AVROLIBS) $(ARROWLIBS) $(SYSTEMLIBS) -lpthread
else
LIBS=-Wl,-Bstatic $(AVROLIBS) $(ARROWLIBS)  $(SYSTEMLIBS) -Wl,-Bdynamic -lpthread
endif
SRC=./src
TESTSRC=./tests
BUILD=./build
BUILDTESTS=$(BUILD)/tests
TESTTARGETS=$(addprefix $(BUILDTESTS)/, $(TESTS))
ARROWBUILD=./vendor_build/arrow/build
AVROBUILD=./vendor_build/avro/build
HEADERS=$(SRC)/c_types.h $(SRC)/util.h $(SRC)/clog.h $(SRC)/log.h $(SRC)/indicator.h
SRCS=c_api dataframe parquet_patches parquet select_rows util struct_utils list_to_tensor sparse_to_dense column
OBJ=$(addprefix $(BUILD)/, $(addsuffix .cpp.o, $(SRCS))) $(BUILD)/iceberg.c.o
EXTOBJ=$(ARROWBUILD)/release/libarrow.a $(AVROBUILD)/release/lib/libavro.a
TESTS=test_log test_strings
LOG_LEVEL=0

.PHONY: directories test_load

# Link the main metaflow-data object, default for make
$(BUILD)/metaflow-data.so: directories $(OBJ) $(EXTOBJ)
ifeq ($(shell uname),Darwin)
	$(CXX) -shared -o $(BUILD)/metaflow-data.so $(OBJ) $(LIBS)
else
	$(CXX) -shared -o $(BUILD)/metaflow-data.so $(OBJ) $(LIBS) -Wl,--version-script=metaflow-data.version
endif

directories:
	mkdir -p $(BUILD)
	mkdir -p $(BUILDTESTS)
	mkdir -p $(ARROWBUILD)
	mkdir -p $(AVROBUILD)

# Build C objects
$(BUILD)/%.c.o: $(SRC)/%.c $(SRC)/%.h
	$(CC) -o $(BUILD)/$*.c.o -c $(SRC)/$*.c $(CFLAGS) -D LOG_LEVEL=$(LOG_LEVEL)

# Build C++ objects for arrowops
$(BUILD)/%.cpp.o: $(SRC)/%.cpp $(HEADERS) $(SRC)/c_api_funcs.h $(ARROWBUILD)/release/libarrow.a
	$(CXX) -o $(BUILD)/$*.cpp.o -c $(SRC)/$*.cpp $(CXXFLAGS) -D LOG_LEVEL=$(LOG_LEVEL)

# Build libarrow
$(ARROWBUILD)/release/libarrow.a:
	bash ./vendor_build/arrow/build_arrow.sh

# Build libavro
$(AVROBUILD)/release/lib/libavro.a:
	bash ./vendor_build/avro/build_avro.sh

# Build and link tests
$(BUILDTESTS)/test_%: $(TESTSRC)/test_%.cpp $(OBJ) $(EXTOBJ)
	$(CXX) -o $(BUILDTESTS)/test_$*.o -c $(TESTSRC)/test_$*.cpp $(CFLAGS) -D LOG_LEVEL=$(LOG_LEVEL)
	$(CXX) -o $(BUILDTESTS)/test_$* $(BUILDTESTS)/test_$*.o $(OBJ) $(LIBS)

# Build the tests
tests: $(TESTTARGETS)

# Run the tests
run_tests: $(TESTTARGETS)
	true $(addprefix && ,$(TESTTARGETS))

# Check that the .so can be loaded with cffi
test_load: TEST_LOAD_VENV_DIR:=$(shell mktemp -d)
test_load: $(BUILD)/metaflow-data.so
	python -m venv ${TEST_LOAD_VENV_DIR}/venv
	${TEST_LOAD_VENV_DIR}/venv/bin/pip install cffi
	${TEST_LOAD_VENV_DIR}/venv/bin/python test_load.py
	rm -rf ${TEST_LOAD_VENV_DIR}


# Create a release name which includes the git commit and .so hash
release: directories $(BUILD)/metaflow-data.so test_load
	mkdir -p $(BUILD)/release
	rm -f $(BUILD)/release/*.so
	cp $(BUILD)/metaflow-data.so $(BUILD)/release/metaflow-data-$(shell (sha256sum -b $(BUILD)/metaflow-data.so 2>/dev/null || shasum -a 256 $(BUILD)/metaflow-data.so) | cut -c -8).so

# Copy the release so to the metaflow-data-bin repo
copy:
	rm -f ../bin/*.so
	mkdir -p ../bin
	cp $(BUILD)/release/*.so ../bin/

# Clean our top level objects
clean:
	rm -f $(BUILD)/*.*
	rm -f $(BUILD)/release/*.*
	rm -f $(BUILD)/tests/*

# Clean our top levels and external build objects
clean-all:
	rm -f $(BUILD)/*.*
	rm -f $(BUILD)/release/*.*
	rm -f $(BUILD)/tests/*
	rm -rf $(ARROWBUILD)/*
	rm -rf $(AVROBUILD)/*
