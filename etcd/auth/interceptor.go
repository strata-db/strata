package auth

import (
	"context"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// methodPerm maps gRPC full method paths to the required permission type.
// Methods absent from this map require no key-level permission check (e.g.
// Auth service methods, health checks).
var methodPerm = map[string]PermType{
	"/etcdserverpb.KV/Range":       READ,
	"/etcdserverpb.KV/Put":         WRITE,
	"/etcdserverpb.KV/DeleteRange": WRITE,
	"/etcdserverpb.KV/Txn":         WRITE,
	"/etcdserverpb.Watch/Watch":    READ,
}

// openMethods are always allowed regardless of auth state (auth bootstrapping
// and health endpoints).
var openMethods = map[string]bool{
	"/etcdserverpb.Auth/Authenticate": true,
	"/etcdserverpb.Auth/AuthEnable":   true,
	"/etcdserverpb.Auth/AuthDisable":  true,
	"/etcdserverpb.Auth/AuthStatus":   true,
	"/grpc.health.v1.Health/Check":    true,
	"/grpc.health.v1.Health/Watch":    true,
}

// Interceptors returns gRPC server options that enforce token validation and
// RBAC. If store is nil, the interceptors are no-ops (auth disabled at compile
// time).
func Interceptors(store *Store, tokens *TokenStore) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	return unaryInterceptor(store, tokens), streamInterceptor(store, tokens)
}

func unaryInterceptor(store *Store, tokens *TokenStore) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkAuth(ctx, store, tokens, info.FullMethod, req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamInterceptor(store *Store, tokens *TokenStore) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkAuth(ss.Context(), store, tokens, info.FullMethod, nil); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// checkAuth validates the token (when auth is enabled) and enforces RBAC for
// the method + key(s) in req.
func checkAuth(ctx context.Context, store *Store, tokens *TokenStore, method string, req any) error {
	// Auth service + health endpoints are always open.
	if openMethods[method] {
		return nil
	}

	if !store.IsEnabled() {
		// Block access to auth-reserved keys even when auth is off, to prevent
		// clients from pre-loading \x00auth/ keys that would be mistaken for auth
		// data when auth is later enabled.
		return checkAuthKeyAccess(method, req)
	}

	// Extract token from gRPC metadata.
	username, err := tokenFromCtx(ctx, tokens)
	if err != nil {
		return err
	}

	// Block access to the reserved auth namespace from the KV service.
	if err := checkAuthKeyAccess(method, req); err != nil {
		return err
	}

	// Check key-level RBAC permission.
	pt, needsKeyCheck := methodPerm[method]
	if !needsKeyCheck {
		return nil
	}
	keys := extractKeys(method, req)
	for _, key := range keys {
		if err := store.CheckPermission(username, key, pt); err != nil {
			return status.Errorf(codes.PermissionDenied, "permission denied on key %q", key)
		}
	}
	return nil
}

// tokenFromCtx extracts the bearer token from gRPC metadata and looks up the
// username.
func tokenFromCtx(ctx context.Context, tokens *TokenStore) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("token")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing token")
	}
	username, ok := tokens.Lookup(vals[0])
	if !ok {
		return "", status.Error(codes.Unauthenticated, "invalid or expired token")
	}
	return username, nil
}

// checkAuthKeyAccess returns PermissionDenied if req contains a key in the
// reserved \x00auth/ namespace and the method is a KV operation.
func checkAuthKeyAccess(method string, req any) error {
	if req == nil {
		return nil
	}
	if !strings.HasPrefix(method, "/etcdserverpb.KV/") {
		return nil
	}
	for _, key := range extractKeys(method, req) {
		if IsAuthPrefix(key) {
			return status.Error(codes.PermissionDenied, "access to internal auth namespace is not allowed")
		}
	}
	return nil
}

// extractKeys returns the key(s) involved in the request for RBAC checking.
// For range/prefix operations the start key is used; for Txn the keys from
// all compare, success, and failure ops are returned.
func extractKeys(method string, req any) []string {
	if req == nil {
		return nil
	}
	switch method {
	case "/etcdserverpb.KV/Range":
		if r, ok := req.(*etcdserverpb.RangeRequest); ok {
			return []string{string(r.Key)}
		}
	case "/etcdserverpb.KV/Put":
		if r, ok := req.(*etcdserverpb.PutRequest); ok {
			return []string{string(r.Key)}
		}
	case "/etcdserverpb.KV/DeleteRange":
		if r, ok := req.(*etcdserverpb.DeleteRangeRequest); ok {
			return []string{string(r.Key)}
		}
	case "/etcdserverpb.KV/Txn":
		if r, ok := req.(*etcdserverpb.TxnRequest); ok {
			return txnKeys(r)
		}
	case "/etcdserverpb.Watch/Watch":
		if r, ok := req.(*etcdserverpb.WatchRequest); ok {
			if cr := r.GetCreateRequest(); cr != nil {
				return []string{string(cr.Key)}
			}
		}
	}
	return nil
}

func txnKeys(r *etcdserverpb.TxnRequest) []string {
	seen := map[string]bool{}
	var keys []string
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for _, c := range r.Compare {
		add(string(c.Key))
	}
	for _, op := range r.Success {
		for _, k := range opKeys(op) {
			add(k)
		}
	}
	for _, op := range r.Failure {
		for _, k := range opKeys(op) {
			add(k)
		}
	}
	return keys
}

func opKeys(op *etcdserverpb.RequestOp) []string {
	switch v := op.Request.(type) {
	case *etcdserverpb.RequestOp_RequestRange:
		if v.RequestRange != nil {
			return []string{string(v.RequestRange.Key)}
		}
	case *etcdserverpb.RequestOp_RequestPut:
		if v.RequestPut != nil {
			return []string{string(v.RequestPut.Key)}
		}
	case *etcdserverpb.RequestOp_RequestDeleteRange:
		if v.RequestDeleteRange != nil {
			return []string{string(v.RequestDeleteRange.Key)}
		}
	}
	return nil
}
