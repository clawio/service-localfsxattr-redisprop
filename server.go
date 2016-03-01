package main

import (
	"github.com/nu7hatch/gouuid"
	"github.com/clawio/service-auth/lib"
	pb "github.com/clawio/service-localfsxattr-redisprop/proto/propagator"
	"github.com/garyburd/redigo/redis"
	rus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"path"
	"strings"
	"time"
)

var (
	unauthenticatedError = grpc.Errorf(codes.Unauthenticated, "identity not found")
	permissionDenied     = grpc.Errorf(codes.PermissionDenied, "access denied")
)

// debugLogger satisfies Gorm's logger interface
// so that we can log SQL queries at Logrus' debug level
type debugLogger struct{}

func (*debugLogger) Print(msg ...interface{}) {
	rus.Debug(msg)
}

type newServerParams struct {
	dsn            string
	sharedSecret   string
	maxRedisIdle   int
	maxRedisActive int
	pool           *redis.Pool
}

func newServer(p *newServerParams) (*server, error) {
	pool := &redis.Pool{
		Wait:        true,
		MaxIdle:     p.maxRedisIdle,
		MaxActive:   p.maxRedisActive,
		IdleTimeout: 0,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", p.dsn)
			if err != nil {
				return nil, err
			}
			/*
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			*/
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	s := &server{}
	s.pool = pool
	s.p = p
	return s, nil
}

type server struct {
	p    *newServerParams
	pool *redis.Pool
}

func (s *server) Get(ctx context.Context, req *pb.GetReq) (*pb.Record, error) {

	traceID, err := getGRPCTraceID(ctx)
	if err != nil {
		rus.Error(err)
		return &pb.Record{}, err
	}
	log := rus.WithField("trace", traceID).WithField("svc", serviceID)
	ctx = newGRPCTraceContext(ctx, traceID)

	log.Info("request started")

	// Time request
	reqStart := time.Now()

	defer func() {
		// Compute request duration
		reqDur := time.Since(reqStart)

		// Log access info
		log.WithFields(rus.Fields{
			"method":   "get",
			"type":     "grpcaccess",
			"duration": reqDur.Seconds(),
		}).Info("request finished")

	}()

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Record{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	p := path.Clean(req.Path)

	log.Infof("path is %s", p)

	var rec *record

	rec, err = s.getByPath(p)
	if err != nil {
		log.Error(err)
		return &pb.Record{}, err
	}
	//When Etag is not set or mtime is not set means that the record is not in Redis
	if rec.ETag == "" || rec.MTime == 0 {
		if !req.ForceCreation {
			return &pb.Record{}, err
		}
		in := &pb.PutReq{}
		in.AccessToken = req.AccessToken
		in.Path = req.Path
		_, e := s.Put(ctx, in)
		if e != nil {
			return &pb.Record{}, err
		}

		rec, err = s.getByPath(p)
		if err != nil {
			return &pb.Record{}, nil
		}
	}

	r := &pb.Record{}
	r.Path = rec.Path
	r.Etag = rec.ETag
	r.Modified = rec.MTime
	return r, nil
}

func (s *server) Mv(ctx context.Context, req *pb.MvReq) (*pb.Void, error) {

	traceID, err := getGRPCTraceID(ctx)
	if err != nil {
		rus.Error(err)
		return &pb.Void{}, err
	}
	log := rus.WithField("trace", traceID).WithField("svc", serviceID)
	ctx = newGRPCTraceContext(ctx, traceID)

	log.Info("request started")

	// Time request
	reqStart := time.Now()

	defer func() {
		// Compute request duration
		reqDur := time.Since(reqStart)

		// Log access info
		log.WithFields(rus.Fields{
			"method":   "mv",
			"type":     "grpcaccess",
			"duration": reqDur.Seconds(),
		}).Info("request finished")

	}()

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	src := path.Clean(req.Src)
	dst := path.Clean(req.Dst)

	log.Infof("src path is %s", src)
	log.Infof("dst path is %s", dst)

	recs, err := s.getRecordsWithPathPrefix(src)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, nil
	}

	con := s.pool.Get()
	// Close the connection as the pool does not work with connections that has
	// been used for transactions
	defer con.Close()
	// When watching the key we are sure that if the key gets modified the transaction will fail
	// is the alternative to mtime < newmtime
	// if update was succesfull returns 1 and if not returns 0 (no keys updated)
	for _, rec := range recs {
		con.Send("WATCH", rec.Path)
	}
	con.Send("MULTI")

	for _, rec := range recs {
		newPath := path.Join(dst, path.Clean(strings.TrimPrefix(rec.Path, src)))
		log.Infof("src path %s will be renamed to %s", rec.Path, newPath)
		con.Send("HMSET", newPath, "etag", rec.ETag, "mtime", rec.MTime)
		con.Send("DEL", rec.Path)
	}
	_, err = con.Do("EXEC")
	if err != nil {
		return &pb.Void{}, err
	}

	log.Infof("renamed %d entries", len(recs))
	
	rawUUID, err := uuid.NewV4()
	if err != nil {
		return &pb.Void{}, err
	}
	etag := rawUUID.String()
	mtime := uint32(time.Now().Unix())
	err = s.propagateChanges(ctx, dst, etag, mtime, "")
	if err != nil {
		log.Error(err)
	}

	log.Infof("propagated changes till %s", "")

	return &pb.Void{}, nil
}

func (s *server) getRecordsWithPathPrefix(p string) ([]record, error) {
	var (
		cursor int64
		items  []string
		recs   []record
	)

	con := s.pool.Get()
	defer con.Close()
	results := make([]string, 0)

	for {
		values, err := redis.Values(con.Do("SCAN", cursor, "MATCH", p+"/*"))
		if err != nil {
			return recs, err
		}

		values, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return recs, err
		}

		results = append(results, items...)

		if cursor == 0 {
			break
		}
	}

	// the regex do not match the current path so we add it manually
	items = append(items, p)

	// TODO(labkode) results can contain duplicated items, be sure to clean before return
	for _, k := range items {
		con.Send("HGETALL", k)
	}
	err := con.Flush()
	if err != nil {
		return recs, err
	}
	for _, k := range items {
		value, err := redis.Values(con.Receive())
		if err != nil {
			return recs, err
		}
		r := record{}
		if err := redis.ScanStruct(value, &r); err != nil {
			return recs, err
		}
		r.Path = k
		if r.ETag != "" && r.MTime != 0 {
			recs = append(recs, r)
		}
	}

	return recs, nil
}
func (s *server) Rm(ctx context.Context, req *pb.RmReq) (*pb.Void, error) {

	traceID, err := getGRPCTraceID(ctx)
	if err != nil {
		rus.Error(err)
		return &pb.Void{}, err
	}
	if err != nil {
		rus.Error(err)
		return &pb.Void{}, err
	}
	log := rus.WithField("trace", traceID).WithField("svc", serviceID)
	ctx = newGRPCTraceContext(ctx, traceID)

	log.Info("request started")

	// Time request
	reqStart := time.Now()

	defer func() {
		// Compute request duration
		reqDur := time.Since(reqStart)

		// Log access info
		log.WithFields(rus.Fields{
			"method":   "rm",
			"type":     "grpcaccess",
			"duration": reqDur.Seconds(),
		}).Info("request finished")

	}()

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	p := path.Clean(req.Path)

	log.Infof("path is %s", p)

	con := s.pool.Get()
	defer con.Close()
	_, err = con.Do("DEL", p)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, err
	}

	ts := time.Now().Unix()
	rawUUID, err := uuid.NewV4()
	if err != nil {
		return &pb.Void{}, err
	}
	err = s.propagateChanges(ctx, p, rawUUID.String(), uint32(ts), "")
	if err != nil {
		log.Error(err)
	}

	log.Infof("propagated changes till %s", "")
	return &pb.Void{}, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutReq) (*pb.Void, error) {

	traceID, err := getGRPCTraceID(ctx)
	if err != nil {
		rus.Error(err)
		return &pb.Void{}, err
	}
	if err != nil {
		rus.Error(err)
		return &pb.Void{}, err
	}
	log := rus.WithField("trace", traceID).WithField("svc", serviceID)
	ctx = newGRPCTraceContext(ctx, traceID)

	log.Info("request started")

	// Time request
	reqStart := time.Now()

	defer func() {
		// Compute request duration
		reqDur := time.Since(reqStart)

		// Log access info
		log.WithFields(rus.Fields{
			"method":   "put",
			"type":     "grpcaccess",
			"duration": reqDur.Seconds(),
		}).Info("request finished")

	}()

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	p := path.Clean(req.Path)

	log.Infof("path is %s", p)

	rawUUID, err := uuid.NewV4()
	if err != nil {
		return &pb.Void{}, err
	}
	var etag = rawUUID.String()
	var mtime = uint32(time.Now().Unix())

	log.Infof("new record will have path=%s etag=%s mtime=%d", p, etag, mtime)

	err = s.insert(p, etag, mtime)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, err
	}

	log.Infof("new record saved to db")

	err = s.propagateChanges(ctx, p, etag, mtime, "")
	if err != nil {
		log.Error(err)
	}

	log.Infof("propagated changes till ancestor %s", "")

	return &pb.Void{}, nil
}

// Redis returns nil when the key is not found
func (s *server) getByPath(path string) (*record, error) {

	con := s.pool.Get()
	defer con.Close()
	v, err := redis.Values(con.Do("HGETALL", path))
	if err != nil {
		return nil, err
	}
	r := &record{}
	if err := redis.ScanStruct(v, r); err != nil {
		return nil, err
	}
	r.Path = path
	return r, nil
}

func (s *server) insert(p, etag string, mtime uint32) error {
	con := s.pool.Get()
	// Close the connection as the pool does not work with connections that has
	// been used for transactions
	defer con.Close()
	// When watching the key we are sure that if the key gets modified the transaction will fail
	// is the alternative to mtime < newmtime
	// if update was succesfull returns 1 and if not returns 0 (no keys updated)
	con.Send("WATCH", p)
	con.Send("MULTI")

	r := &record{}
	r.ETag = etag
	r.MTime = mtime

	con.Send("HMSET", p, "etag", r.ETag, "mtime", r.MTime)
	_, err := con.Do("EXEC")
	if err != nil {
		return err
	}
	return nil

}
func (s *server) update(p, etag string, mtime uint32) int64 {
	err := s.insert(p, etag, mtime)
	if err != nil {
		return 0
	}
	return 1
}

// propagateChanges propagates mtime and etag until the user home directory
// This propagation is needed for the client discovering changes
// Ex: given the succesfull upload of the file /local/users/d/demo/photos/1.png
// the etag and mtime will be propagated to:
//    - /local/users/d/demo/photos
//    - /local/users/d/demo
func (s *server) propagateChanges(ctx context.Context, p, etag string, mtime uint32, stopPath string) error {

	traceID, err := getGRPCTraceID(ctx)
	if err != nil {
		rus.Error(err)
		return err
	}
	log := rus.WithField("trace", traceID).WithField("svc", serviceID)
	ctx = newGRPCTraceContext(ctx, traceID)

	// TODO(labkode) assert the list ordered from most deeper to less so we can shortcircuit
	// after first miss
	paths := getPathsTillHome(ctx, p)
	for _, p := range paths {
		numRows := s.update(p, etag, mtime)
		if numRows == 0 {
			log.Warnf("parent path %s has been updated in the meanwhile so we do not override with old info. Propagation stopped", p)
			// Following the CAS tree approach it does not make sense to update\
			// parents if child has been updated wit new info
			break
		}
		log.Infof("parent path %s has being updated", p)
	}

	return nil
}

func getPathsTillHome(ctx context.Context, p string) []string {

	paths := []string{}
	tokens := strings.Split(p, "/")

	if len(tokens) < 5 {
		// if not under home dir we do not propagate
		return paths
	}

	homeTokens := tokens[0:5]
	restTokens := tokens[5:]

	home := path.Clean("/" + path.Join(homeTokens...))

	previous := home
	paths = append(paths, previous)

	for _, token := range restTokens {
		previous = path.Join(previous, path.Clean(token))
		paths = append(paths, previous)
	}

	if len(paths) >= 1 {
		paths = paths[:len(paths)-1] // remove inserted/updated path from paths to update
	}

	//reverse it to have deeper paths first to shortcircuit
	for i := len(paths)/2 - 1; i >= 0; i-- {
		opp := len(paths) - 1 - i
		paths[i], paths[opp] = paths[opp], paths[i]

	}
	return paths
}
