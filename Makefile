GO = go
PLAKAR = ../plakar/plakar
VERSION = v0.0.1
EXT=

all: build

build:
	${GO} build -v -o scalewayInstanceImporter${EXT} ./plugin/importer
	#${GO} build -v -o scalewayInstanceExporter${EXT} ./plugin/exporter

create:
	${PLAKAR} pkg create manifest.yaml

uninstall:
	${PLAKAR} pkg ls | grep scaleway-Instance-v | xargs ${PLAKAR} pkg uninstall

install:
	${PLAKAR} pkg install scaleway-Instance-${VERSION}.ptar

clean:
	rm -f scalewayInstanceImporter scalewayInstanceExporter scaleway-Instance-*.ptar
