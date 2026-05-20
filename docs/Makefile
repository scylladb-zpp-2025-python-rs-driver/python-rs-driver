# Global variables
UV              = uv
SPHINXOPTS      = -j auto
SPHINXBUILD     = $(UV) run sphinx-build
SPHINXAUTOBUILD = $(UV) run sphinx-autobuild
BUILDDIR        = _build
SOURCEDIR       = source
PORT            ?= 5500
HOST            ?= 127.0.0.1

ALLSPHINXOPTS = -d $(BUILDDIR)/doctrees $(SPHINXOPTS) $(SOURCEDIR)

.PHONY: all
all: html

# Setup commands
.PHONY: setup
setup:
	$(UV) sync

.PHONY: update
update:
	$(UV) lock --upgrade

# Clean commands
.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)

# Standard HTML builder.
.PHONY: html
html: setup
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# Directory HTML builder.
.PHONY: dirhtml
dirhtml: setup
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

# Live preview.
# Set PORT to choose the preview port (default: 5500), e.g. `make preview PORT=8000`.
# Set HOST to choose the preview host (default: 127.0.0.1), e.g. `make preview HOST=0.0.0.0`.
.PHONY: preview
preview: setup
	$(SPHINXAUTOBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html --port $(PORT) --host $(HOST)
