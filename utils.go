package main

import (
	"github.com/nu7hatch/gouuid"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/context"
	metadata "google.golang.org/grpc/metadata"
)

// TODO(labkode) set collation for table and column to utf8. The default is swedish
type record struct {
	Path  string
	ETag  string `redis:"etag"`
	MTime uint32 `redis:"mtime"`
}

func (r *record) String() string {
	return fmt.Sprintf("path=%s etag=%s mtime=%d",
		r.Path, r.ETag, r.MTime)
}

func newGRPCTraceContext(ctx context.Context, trace string) context.Context {
	md := metadata.Pairs("trace", trace)
	ctx = metadata.NewContext(ctx, md)
	return ctx
}

func getGRPCTraceID(ctx context.Context) (string, error) {

	md, ok := metadata.FromContext(ctx)
	if !ok {
		rawUUID, err := uuid.NewV4()
		if err != nil {
			return "", err
		}
		return rawUUID.String(), nil
	}

	tokens := md["trace"]
	if len(tokens) == 0 {
		rawUUID, err := uuid.NewV4()
		if err != nil {
			return "", err
		}
		return rawUUID.String(), nil
	}

	if tokens[0] != "" {
		return tokens[0], nil
	}

	rawUUID, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return rawUUID.String(), nil
}
