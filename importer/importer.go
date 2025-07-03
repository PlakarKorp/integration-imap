package importer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
)

type ImapImporter struct {
	ctx      context.Context
	address  string
	tlsMode  string
	username string
	password string
}

func NewImapImporter(ctx context.Context, opts *importer.Options, name string, config map[string]string) (importer.Importer, error) {
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

	return &ImapImporter{
		ctx:      ctx,
		address:  location,
		tlsMode:  tlsMode,
		username: username,
		password: password,
	}, nil
}

func (imp *ImapImporter) connect() (*imapclient.Client, error) {
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

func (imp *ImapImporter) Origin() string {
	return imp.address
}

func (imp *ImapImporter) Type() string {
	return "imap"
}

func (imp *ImapImporter) Root() string {
	return "/"
}

func (imp *ImapImporter) Scan() (<-chan *importer.ScanResult, error) {
	result := make(chan *importer.ScanResult, 10)
	go func() {
		defer close(result)
		client, err := imp.connect()
		if err != nil {
			result <- importer.NewScanError("/", err)
			return
		}

		mailboxes, err := imp.listMailboxes(client)
		if err != nil {
			result <- importer.NewScanError("/", err)
		}
		for _, mbox := range mailboxes {
			result <- imp.makeMailboxRecord(mbox)
			imp.scanMailbox(client, mbox.Mailbox, result)
		}

		err = client.Logout().Wait()
		if err != nil {
			result <- importer.NewScanError("/", err)
			return
		}

		result <- imp.makeRootRecord()
	}()
	return result, nil
}

func (imp *ImapImporter) Close() error {
	return nil
}

func (imp *ImapImporter) listMailboxes(client *imapclient.Client) ([]*imap.ListData, error) {
	var res []*imap.ListData

	listCmd := client.List("", "%", &imap.ListOptions{
		ReturnStatus: &imap.StatusOptions{
			NumMessages: true,
			NumUnseen:   true,
		},
	})
	for {
		mbox := listCmd.Next()
		if mbox == nil {
			break
		}
		res = append(res, mbox)
	}
	if err := listCmd.Close(); err != nil {
		return nil, fmt.Errorf("LIST command failed: %v", err)
	}

	return res, nil
}

func (imp *ImapImporter) scanMailbox(client *imapclient.Client, mailbox string, out chan *importer.ScanResult) error {
	_, err := client.Select(mailbox, &imap.SelectOptions{
		ReadOnly: true,
	}).Wait()
	if err != nil {
		return fmt.Errorf("SELECT command failed: %w", err)
	}

	searchData, err := client.UIDSearch(
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
		messages, err := client.Fetch(seq, opts).Collect()
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
