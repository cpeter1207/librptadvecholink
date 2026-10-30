.DEFAULT_GOAL := all

CARGO ?= cargo
CC ?= cc
READELF ?= readelf
PYTHON ?= python3
DOXYGEN ?= doxygen
CPPCHECK ?= cppcheck
CARGO_TARGET_DIR ?= target
PACKAGE := librptadvecholink
CRATE := rptadv_echolink
VERSION ?= $(shell dpkg-parsechangelog -S Version | sed 's/-[0-9]*$$//' | tr '~' '-')
PACKAGE_VERSION ?= $(VERSION)
SOVERSION := 1
PREFIX ?= /usr
LIBDIR ?= $(PREFIX)/lib
DESTDIR ?=
LIBRARY_BASENAME := lib$(CRATE)
TARGET_LIBRARY := $(CARGO_TARGET_DIR)/release/$(LIBRARY_BASENAME).so
LIBRARY_VERSIONED := build/$(LIBRARY_BASENAME).so.$(SOVERSION).$(PACKAGE_VERSION)
LIBRARY_SONAME := build/$(LIBRARY_BASENAME).so.$(SOVERSION)
LIBRARY_LINK := build/$(LIBRARY_BASENAME).so
HEADER := include/rptadv_echolink.h
RUST_SOURCES := $(shell find src tests -name '*.rs' -type f)
PC_TEMPLATE := rptadv_echolink.pc.in
PC_FILE := build/rptadv_echolink.pc
SMOKE_SOURCE := tests/test_abi.c
SMOKE_BINARY := build/abi-header-smoke
DEBIAN_VERSION = $(shell dpkg-parsechangelog -S Version)
DEBIAN_ARCH = $(shell dpkg-architecture -qDEB_HOST_ARCH)
DEBIAN_MULTIARCH = $(shell dpkg-architecture -qDEB_HOST_MULTIARCH)
DEBIAN_RUNTIME_DEB := build/librptadvecholink1_$(DEBIAN_VERSION)_$(DEBIAN_ARCH).deb
DEBIAN_DEV_DEB := build/librptadvecholink-dev_$(DEBIAN_VERSION)_$(DEBIAN_ARCH).deb
STAGE := build/stage

SONAME_RUSTFLAGS = $(RUSTFLAGS) -C link-arg=-Wl,-soname,$(LIBRARY_BASENAME).so.$(SOVERSION)

.PHONY: all quality lint static-analysis docs rustdoc test coverage platform-verify install install-check package-check debian-package-check release-packages dist clean FORCE

all: $(LIBRARY_VERSIONED) $(LIBRARY_SONAME) $(LIBRARY_LINK)

build:
	mkdir -p $@

$(TARGET_LIBRARY): Cargo.toml Cargo.lock build.rs $(HEADER) $(RUST_SOURCES)
	RUSTFLAGS="$(SONAME_RUSTFLAGS)" CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build --release --locked

$(LIBRARY_VERSIONED): $(TARGET_LIBRARY) | build
	cp $(TARGET_LIBRARY) $@
	$(READELF) -d $@ | grep -F '$(LIBRARY_BASENAME).so.$(SOVERSION)'
	! $(READELF) -d $@ | grep -E 'libasterisk|res_usbradio|libasound|libportaudio'

$(LIBRARY_SONAME): $(LIBRARY_VERSIONED)
	ln -sf $(notdir $<) $@

$(LIBRARY_LINK): $(LIBRARY_SONAME)
	ln -sf $(notdir $<) $@

$(PC_FILE): $(PC_TEMPLATE) FORCE | build
	sed -e 's|@PREFIX@|$(PREFIX)|' -e 's|@LIBDIR@|$(LIBDIR)|' \
		-e 's|@VERSION@|$(PACKAGE_VERSION)|' -e 's|@ABI_VERSION@|$(SOVERSION)|' $< > $@

test: all
	$(PYTHON) tests/test_debian_manifest.py
	$(CARGO) test --all-targets --locked
	$(MAKE) $(SMOKE_BINARY)
	LD_LIBRARY_PATH="$(CURDIR)/build:$${LD_LIBRARY_PATH}" ./$(SMOKE_BINARY)

coverage:
	mkdir -p build/coverage
	$(CARGO) +nightly-2025-02-20 llvm-cov --all-targets --branch --json \
		--output-path build/coverage/rust.json
	$(PYTHON) tests/check_coverage.py build/coverage/rust.json

platform-verify: test install-check package-check

quality: lint static-analysis docs

lint:
	$(CARGO) fmt --all -- --check
	ruff format --check tests
	ruff check tests

static-analysis:
	$(CARGO) clippy --all-targets -- -D warnings
	$(CPPCHECK) --force --check-level=exhaustive --enable=warning,style,performance,portability --error-exitcode=1 \
		--std=c11 -Iinclude $(wildcard tests/*.c)

docs: rustdoc | build
	$(DOXYGEN) Doxyfile || { cat build/doxygen-warnings.log; exit 1; }
	test ! -s build/doxygen-warnings.log

rustdoc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --no-deps --locked

$(SMOKE_BINARY): $(SMOKE_SOURCE) $(HEADER) $(LIBRARY_LINK) | build
	$(CC) -std=c11 -Wall -Wextra -Werror -Iinclude $< -Lbuild \
		-l$(CRATE) -Wl,-rpath,'$$ORIGIN' -o $@

install: all $(PC_FILE)
	install -d $(DESTDIR)$(LIBDIR) $(DESTDIR)$(PREFIX)/include/rptadv_echolink \
		$(DESTDIR)$(LIBDIR)/pkgconfig
	install -m 0755 $(LIBRARY_VERSIONED) $(DESTDIR)$(LIBDIR)/
	ln -sf $(notdir $(LIBRARY_VERSIONED)) $(DESTDIR)$(LIBDIR)/$(notdir $(LIBRARY_SONAME))
	ln -sf $(notdir $(LIBRARY_SONAME)) $(DESTDIR)$(LIBDIR)/$(notdir $(LIBRARY_LINK))
	install -m 0644 $(HEADER) $(DESTDIR)$(PREFIX)/include/rptadv_echolink/
	install -m 0644 $(PC_FILE) $(DESTDIR)$(LIBDIR)/pkgconfig/

install-check: all
	rm -rf $(STAGE)
	$(MAKE) DESTDIR=$(CURDIR)/$(STAGE) PREFIX=/usr LIBDIR=/usr/lib/$(DEBIAN_MULTIARCH) install
	test -f $(STAGE)/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_VERSIONED))
	test -L $(STAGE)/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_SONAME))
	test -L $(STAGE)/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_LINK))
	test -f $(STAGE)/usr/include/rptadv_echolink/$(notdir $(HEADER))
	test ! -e $(STAGE)/usr/lib/$(DEBIAN_MULTIARCH)/$(LIBRARY_BASENAME).a
	$(READELF) -d $(STAGE)/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_VERSIONED)) | grep -F '$(LIBRARY_BASENAME).so.$(SOVERSION)'
	$(CC) -std=c11 -Wall -Wextra -Werror -I$(STAGE)/usr/include/rptadv_echolink $(SMOKE_SOURCE) \
		-L$(STAGE)/usr/lib/$(DEBIAN_MULTIARCH) -l$(CRATE) \
		-Wl,-rpath,$(CURDIR)/$(STAGE)/usr/lib/$(DEBIAN_MULTIARCH) -o $(STAGE)/abi-header-smoke
	$(STAGE)/abi-header-smoke

package-check:
	rm -rf build/package-source
	mkdir -p build/package-source
	tar --exclude='./.git' --exclude='./.work' --exclude='./.work-target' --exclude='./build' --exclude='./target' --exclude='./.superpowers' --exclude='./.quality-tools' -cf - . | \
		tar -xf - -C build/package-source
	chmod 0755 build/package-source/debian/rules
	cd build/package-source && dpkg-buildpackage -us -uc -b
	test -f $(DEBIAN_RUNTIME_DEB)
	test -f $(DEBIAN_DEV_DEB)
	rm -rf build/package-stage
	mkdir -p build/package-stage
	dpkg-deb --extract $(DEBIAN_RUNTIME_DEB) build/package-stage
	dpkg-deb --extract $(DEBIAN_DEV_DEB) build/package-stage
	test -f build/package-stage/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_VERSIONED))
	test -L build/package-stage/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_SONAME))
	test -L build/package-stage/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_LINK))
	test -f build/package-stage/usr/include/rptadv_echolink/$(notdir $(HEADER))
	$(READELF) -d build/package-stage/usr/lib/$(DEBIAN_MULTIARCH)/$(notdir $(LIBRARY_VERSIONED)) | grep -F '$(LIBRARY_BASENAME).so.$(SOVERSION)'
	$(CC) -std=c11 -Wall -Wextra -Werror -Ibuild/package-stage/usr/include/rptadv_echolink $(SMOKE_SOURCE) \
		-Lbuild/package-stage/usr/lib/$(DEBIAN_MULTIARCH) -l$(CRATE) \
		-Wl,-rpath,$(CURDIR)/build/package-stage/usr/lib/$(DEBIAN_MULTIARCH) -o build/package-stage/abi-header-smoke
	build/package-stage/abi-header-smoke
	rm -rf build/package-source

debian-package-check: package-check

release-packages: package-check dist
	rm -rf build/debian-source
	mkdir -p build/debian-source
	cp $(DEBIAN_RUNTIME_DEB) $(DEBIAN_DEV_DEB) build/debian-source/

dist:
	mkdir -p build
	git -c safe.directory=$(CURDIR) archive --format=tar --prefix=$(PACKAGE)-$(VERSION)/ HEAD | gzip -n > build/$(PACKAGE)-$(VERSION).tar.gz

clean:
	rm -rf build target

FORCE:
