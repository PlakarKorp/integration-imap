package exporter

import (
	"context"
	"io"
	"strings"

	"github.com/PlakarKorp/integration-imap/common"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
)

type ImapExporter struct {
	ctx       context.Context
	connector common.ImapConnector
	session   *common.ImapSession
}

func NewImapExporter(ctx context.Context, opts *exporter.Options, name string, config map[string]string) (exporter.Exporter, error) {
	exp := &ImapExporter{
		ctx: ctx,
	}

	err := exp.connector.InitFromConfig(config)
	if err != nil {
		return nil, err
	}

	return exp, nil
}

func (exp *ImapExporter) Root() string {
	return "/"
}

func (exp *ImapExporter) CreateDirectory(pathname string) error {
	session, err := exp.getSession()
	if err != nil {
		return err
	}

	mailbox, _ := strings.CutPrefix(pathname, "/")
	return session.Create(mailbox, true)
}

func (exp *ImapExporter) StoreFile(pathname string, fp io.Reader, size int64) error {
	session, err := exp.getSession()
	if err != nil {
		return err
	}

	pathname, _ = strings.CutPrefix(pathname, "/")
	// XXX
	path := strings.SplitN(pathname, "/", 2)
	mailbox := path[0]

	return session.Append(mailbox, fp, size)
}

func (exp *ImapExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (exp *ImapExporter) Close() error {
	if exp.session != nil {
		return exp.session.Logout()
	}
	return nil
}

func (exp *ImapExporter) getSession() (*common.ImapSession, error) {
	if exp.session == nil {
		session, err := exp.connector.Connect()
		if err != nil {
			return nil, err
		}
		exp.session = session
	}
	return exp.session, nil
}
