GO = go

all:
	${GO} build -v -o imapImporter ./plugin/importer
	${GO} build -v -o imapExporter ./plugin/exporter

clean:
	rm -f imapImporter imapExporter imap-v*.ptar
