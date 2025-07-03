package exporter

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
	_ "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
)

type ImapExporter struct {
	ctx      context.Context
	address  string
	tlsMode  string
	username string
	password string
}

func NewImapExporter(ctx context.Context, opts *exporter.Options, name string, config map[string]string) (exporter.Exporter, error) {
	location := config["location"]
	location, _ = strings.CutPrefix(location, "imap://")

	username, ok := config["username"]
	if !ok {
		return nil, fmt.Errorf("Missing username")
	}

	password, ok := config["password"]
	if !ok {
		return nil, fmt.Errorf("Missing password")
	}

	tlsMode, ok := config["tls"]
	if !ok {
		tlsMode = "starttls"
	}

	return &ImapExporter{
		ctx:      ctx,
		address:  location,
		tlsMode:  tlsMode,
		username: username,
		password: password,
	}, nil
}

func (imp *ImapExporter) connect() (*imapclient.Client, error) {
	dialer := imapclient.DialTLS
	switch imp.tlsMode {
	case "no-tls":
		dialer = imapclient.DialInsecure
	case "starttls":
		dialer = imapclient.DialStartTLS
	case "tls":
		dialer = imapclient.DialTLS
	default:
		return nil, fmt.Errorf("Invalid tls mode %q", imp.tlsMode)
	}

	client, err := dialer(imp.address, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to dial IMAP server: %w", err)
	}

	err = client.Login(imp.username, imp.password).Wait()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("Failed to login %w", err)
	}

	return client, nil
}

func (exp *ImapExporter) Root() string {
	return "/"
}

func (exp *ImapExporter) CreateDirectory(pathname string) error {
	return nil
}

func (exp *ImapExporter) StoreFile(pathname string, fp io.Reader, size int64) error {
	return nil
}

func (exp *ImapExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (exp *ImapExporter) Close() error {
	return nil
}
