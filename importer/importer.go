package importer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/integration-imap/common"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/emersion/go-imap/v2"
)

type ImapImporter struct {
	connector common.ImapConnector
}

func NewImapImporter(ctx context.Context, opts *importer.Options, name string, config map[string]string) (importer.Importer, error) {
	imp := &ImapImporter{}
	err := imp.connector.InitFromConfig(config)
	if err != nil {
		return nil, err
	}

	return imp, nil
}

func (imp *ImapImporter) Origin(ctx context.Context) (string, error) {
	return imp.connector.Address, nil
}

func (imp *ImapImporter) Type(ctx context.Context) (string, error) {
	return "imap", nil
}

func (imp *ImapImporter) Root(ctx context.Context) (string, error) {
	return "/", nil
}

func (imp *ImapImporter) Scan(ctx context.Context) (<-chan *importer.ScanResult, error) {
	result := make(chan *importer.ScanResult, 10)
	go func() {
		defer close(result)
		session, err := imp.connector.Connect()
		if err != nil {
			result <- importer.NewScanError("/", err)
			return
		}

		mailboxes, err := session.List()
		if err != nil {
			result <- importer.NewScanError("/", err)
		}
		for _, mbox := range mailboxes {
			result <- imp.makeMailboxRecord(mbox)
			imp.scanMailbox(session, mbox.Mailbox, result)
		}

		err = session.Logout()
		if err != nil {
			result <- importer.NewScanError("/", err)
			return
		}

		result <- imp.makeRootRecord()
	}()
	return result, nil
}

func (imp *ImapImporter) Close(ctx context.Context) error {
	return nil
}

func (imp *ImapImporter) scanMailbox(session *common.ImapSession, mailbox string, out chan *importer.ScanResult) error {
	err := session.Select(mailbox, true)
	if err != nil {
		return fmt.Errorf("SELECT command failed: %w", err)
	}

	searchData, err := session.Client.UIDSearch(
		&imap.SearchCriteria{},
		&imap.SearchOptions{
			ReturnMin: true,
			ReturnMax: true,
			ReturnAll: true,
		},
	).Wait()
	if err != nil {
		return fmt.Errorf("UIDSELECT command failed: %w", err)
	}

	for _, uid := range searchData.AllUIDs() {

		path := fmt.Sprintf("/%s/%v", mailbox, uid)

		seq := imap.SeqSetNum(uint32(uid))
		opts := &imap.FetchOptions{
			BodySection: []*imap.FetchItemBodySection{
				&imap.FetchItemBodySection{
					Peek: true,
				},
			},
		}
		messages, err := session.Client.Fetch(seq, opts).Collect()
		if err != nil {
			out <- importer.NewScanError(path, err)
			continue
		}
		if len(messages) != 1 {
			out <- importer.NewScanError(path, fmt.Errorf("Unexpected number of messages %v", len(messages)))
			continue
		}
		msg := messages[0]
		if len(msg.BodySection) != 1 {
			out <- importer.NewScanError(path, fmt.Errorf("Unexpected number of sections %v", len(msg.BodySection)))
			continue
		}
		section := msg.BodySection[0]
		out <- imp.makeUIDRecord(mailbox, uid, section.Bytes)
	}

	return nil
}

func (imp *ImapImporter) makeRootRecord() *importer.ScanResult {
	fi := objects.NewFileInfo(
		"/",
		0,
		0700|os.ModeDir,
		time.Unix(0, 0),
		0,
		0,
		0,
		0,
		0,
	)
	return importer.NewScanRecord("/", "", fi, nil, nil)
}

func (imp *ImapImporter) makeMailboxRecord(m *imap.ListData) *importer.ScanResult {
	fi := objects.NewFileInfo(
		m.Mailbox,
		0,
		0700|os.ModeDir,
		time.Unix(0, 0),
		0,
		0,
		0,
		0,
		0,
	)
	return importer.NewScanRecord(fmt.Sprintf("/%s", m.Mailbox), "", fi, nil, nil)
}

func (imp *ImapImporter) makeUIDRecord(mailbox string, uid imap.UID, data []byte) *importer.ScanResult {
	fi := objects.NewFileInfo(
		fmt.Sprint(uid),
		0,
		0600,
		time.Unix(0, 0),
		0,
		0,
		0,
		0,
		0,
	)

	f := func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return importer.NewScanRecord(fmt.Sprintf("/%s/%v", mailbox, uid), "", fi, nil, f)
}
