ifneq ($(BUILD),$(notdir $(CURDIR)))
.PHONY: $(BUILD) clean

export OUTDIR	:=	$(CURDIR)/../src/main/resources
BUILDDIR		:=	../build/native/$(BUILD_TYPE)/$(BUILD)

clean:
	@if [ -d ../build/native/ ]; then rm -rf ../build/native/; fi
	@rm -rf $(foreach arch,$(ARCHS),$(OUTDIR)/$(arch))

$(BUILD):
	@[ -d $(BUILDDIR) ] || mkdir -p $(BUILDDIR)
	@$(MAKE) --no-print-directory -C $(BUILDDIR) -f $(CURDIR)/Makefile BUILD=$(BUILD) build

else
.PHONY: build

include $(TOOLCHAINS)/$(BUILD)

SOURCES			:=	$(PROJDIR)/../src/main/native/common $(PROJDIR)/../src/main/native/$(BUILD)
SOURCES			:=	$(abspath $(SOURCES))

CFILES			:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.c))
CPPFILES		:=	$(foreach dir,$(SOURCES),$(wildcard $(dir)/*.cpp))
OFILES			:=	$(addsuffix .o,$(subst /,__,$(CFILES)) $(subst /,__,$(CPPFILES)))

INCLUDE			:=	$(addprefix -I,$(INCLUDES) $(CURDIR))

build: libldb-jni.$(EXT)
	@echo "Copying libldb-jni.$(EXT) to $(OUTDIR)/$(BUILD)/..."
	@[ -d $(OUTDIR)/$(BUILD) ] || mkdir -p $(OUTDIR)/$(BUILD)
	@cp libldb-jni.$(EXT) $(OUTDIR)/$(BUILD)/libldb-jni.$(EXT)

libldb-jni.$(EXT): $(CFILES) $(CPPFILES) $(OFILES) $(CURDIR)/lib-leveldb/libleveldb.a
	@echo "Linking $@..."
	@$(LD) $(LDFLAGS) $(INCLUDE) -o $@ $(OFILES) $(CURDIR)/lib-leveldb/libleveldb.a
	@echo "Stripping $@..."
	@$(STRIP) $@

%.c.o: $(CFILES) $(CURDIR)/lib-leveldb/libleveldb.a
	@echo "Building $(subst .o,,$(subst __,/,$@))..."
	@$(CC) $(CFLAGS) $(INCLUDE) -c $(subst .o,,$(subst __,/,$@)) -o $@

%.cpp.o: $(CPPFILES) $(CURDIR)/lib-leveldb/libleveldb.a
	@echo "Building $(subst .o,,$(subst __,/,$@))..."
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $(subst .o,,$(subst __,/,$@)) -o $@

$(CURDIR)/lib-leveldb/libleveldb.a: $(TOPDIR)/leveldb-mcpe-1.22.tar.gz
	@[ ! -d lib-leveldb/ ] || rm -rf lib-leveldb/
	@tar zxf $(TOPDIR)/leveldb-mcpe-1.22.tar.gz
	@mv leveldb-mcpe-1.22/ lib-leveldb/
	@cd lib-leveldb/ && cmake -DCMAKE_C_COMPILER="$(CC)" -DCMAKE_CXX_COMPILER="$(CXX)" -DLEVELDB_BUILD_TESTS=OFF -DLEVELDB_BUILD_BENCHMARKS=OFF -DLEVELDB_INSTALL=OFF . && cd ..
	@$(MAKE) -C $(CURDIR)/lib-leveldb/ -f $(CURDIR)/lib-leveldb/Makefile

endif