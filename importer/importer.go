package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	block "github.com/scaleway/scaleway-sdk-go/api/block/v1"
	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

func init() {
	importer.Register("scaleway+instance", 0, NewImporter)
}

type Importer struct {
	accessKey string
	secretKey string
	projectID string
	serverID  string
	bucket    string
	region    string
	zone      scw.Zone

	server *instance.Server

	client      *scw.Client
	blockAPI    *block.API
	instanceAPI *instance.API
}

func NewImporter(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (importer.Importer, error) {
	imp := &Importer{
		accessKey: config["access_key"],
		secretKey: config["secret_key"],
		projectID: config["project_id"],
		serverID:  config["server_id"],
		bucket:    config["bucket"],
	}

	switch config["zone"] {
	case "", "fr-par-1":
		imp.zone = scw.ZoneFrPar1
	case "fr-par-2":
		imp.zone = scw.ZoneFrPar2
	case "nl-ams-1":
		imp.zone = scw.ZoneNlAms1
	case "nl-ams-2":
		imp.zone = scw.ZoneNlAms2
	case "pl-waw-1":
		imp.zone = scw.ZonePlWaw1
	case "pl-waw-2":
		imp.zone = scw.ZonePlWaw2
	default:
		return nil, fmt.Errorf("unsupported zone %q", config["zone"])
	}
	region, _ := imp.zone.Region()
	imp.region = region.String()

	client, err := scw.NewClient(
		scw.WithAuth(imp.accessKey, imp.secretKey),
	)
	if err != nil {
		return nil, err
	}

	imp.client = client
	imp.instanceAPI = instance.NewAPI(client)
	imp.blockAPI = block.NewAPI(client)

	if resp, err := imp.instanceAPI.GetServer(&instance.GetServerRequest{
		Zone:     imp.zone,
		ServerID: imp.serverID,
	}); err != nil {
		return nil, err
	} else {
		imp.server = resp.Server
	}
	return imp, nil
}

func (imp *Importer) Root() string { return "/" }
func (imp *Importer) Origin() string {
	return fmt.Sprintf("scaleway+instance://%s", imp.serverID)
}
func (imp *Importer) Type() string          { return "scaleway+instance" }
func (imp *Importer) Flags() location.Flags { return 0 }

func (imp *Importer) Ping(ctx context.Context) error {
	_, err := imp.instanceAPI.GetServer(&instance.GetServerRequest{
		Zone:     imp.zone,
		ServerID: imp.serverID,
	})
	return err
}

func (imp *Importer) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	defer close(records)

	if err := imp.Ping(ctx); err != nil {
		return err
	}

	records <- connectors.NewRecord("/", "", objects.FileInfo{
		Lname:    "/",
		Lmode:    os.ModeDir | 0o700,
		Lsize:    0,
		LmodTime: time.Unix(0, 0),
	}, nil, nil)

	records <- connectors.NewRecord("/METADATA", "", objects.FileInfo{
		Lname:    "METADATA.json",
		Lmode:    0o700,
		Lsize:    -1,
		LmodTime: time.Unix(0, 0),
	}, nil, func() (io.ReadCloser, error) {
		if ret, err := json.Marshal(imp.server); err != nil {
			return nil, err
		} else {
			return io.NopCloser(bytes.NewReader(ret)), nil
		}
	})

	for _, volume := range imp.server.Volumes {
		name := fmt.Sprintf("%s-%s", time.Now().Format("20060102150405"), volume.ID)
		filename := name + ".qcow2"

		reader := func() (io.ReadCloser, error) {
			snap, err := imp.blockAPI.CreateSnapshot(&block.CreateSnapshotRequest{
				Zone:      imp.zone,
				ProjectID: imp.projectID,
				VolumeID:  volume.ID,
				Name:      name,
			})
			if err != nil {
				return nil, fmt.Errorf("create snapshot: %w", err)
			}

			_, err = imp.blockAPI.WaitForSnapshot(&block.WaitForSnapshotRequest{
				Zone:       imp.zone,
				SnapshotID: snap.ID,
				Timeout:    scw.TimeDurationPtr(30 * time.Minute),
			})
			if err != nil {
				return nil, fmt.Errorf("wait snapshot: %w", err)
			}

			_, err = imp.blockAPI.ExportSnapshotToObjectStorage(&block.ExportSnapshotToObjectStorageRequest{
				Zone:       imp.zone,
				SnapshotID: snap.ID,
				Bucket:     imp.bucket,
				Key:        filename,
			})
			if err != nil {
				return nil, fmt.Errorf("export snapshot: %w", err)
			}

			//
			for {
				snap, err := imp.blockAPI.GetSnapshot(&block.GetSnapshotRequest{
					Zone:       imp.zone,
					SnapshotID: snap.ID,
				})
				if err != nil {
					return nil, err
				}

				if snap.Status == block.SnapshotStatusAvailable {
					break
				}

				if snap.Status == block.SnapshotStatusError {
					return nil, fmt.Errorf("snapshot export failed")
				}

				time.Sleep(1 * time.Second)
			}
			//

			return imp.waitAndOpenObject(ctx, imp.bucket, filename, 60, 10*time.Second)
		}

		records <- connectors.NewRecord("/"+filename, "", objects.FileInfo{
			Lname:    filename,
			Lmode:    0o600,
			Lsize:    -1, // unknown until object exists
			LmodTime: time.Now(),
		}, nil, reader)

	}

	return nil
}

func (imp *Importer) waitAndOpenObject(ctx context.Context, bucket, key string, attempts int, delay time.Duration) (io.ReadCloser, error) {
	s3client, err := imp.newS3Client(ctx)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		_, err := s3client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			out, err := s3client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return nil, err
			}
			return out.Body, nil
		}

		lastErr = err

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("object %s/%s not ready: %w", bucket, key, lastErr)
}

func (imp *Importer) newS3Client(ctx context.Context) (*s3.Client, error) {
	endpoint := fmt.Sprintf("https://s3.%s.scw.cloud", imp.region)

	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(imp.region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(imp.accessKey, imp.secretKey, ""),
		),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}

func (imp *Importer) Close(ctx context.Context) error {
	return nil
}
