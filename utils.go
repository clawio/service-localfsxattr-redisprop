package main

import (
	"fmt"

	"code.google.com/p/go-uuid/uuid"
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

func getGRPCTraceID(ctx context.Context) string {

	md, ok := metadata.FromContext(ctx)
	if !ok {
		return uuid.New()
	}

	tokens := md["trace"]
	if len(tokens) == 0 {
		return uuid.New()
	}

	if tokens[0] != "" {
		return tokens[0]
	}

	return uuid.New()
}
