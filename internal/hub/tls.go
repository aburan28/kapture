package hub

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
)

// Secret keys the hub TLS configuration reads, matching the layout of
// kubernetes.io/tls secrets plus the conventional CA bundle key used by
// cert-manager.
const (
	TLSCertKey = corev1.TLSCertKey       // tls.crt
	TLSKeyKey  = corev1.TLSPrivateKeyKey // tls.key
	TLSCAKey   = "ca.crt"
)

// buildServerTLSConfig parses a TLS secret's data into a server tls.Config.
// When requireClientCert is true (mTLS), the secret must also carry ca.crt
// and spokes must present certificates signed by it.
func buildServerTLSConfig(secretData map[string][]byte, requireClientCert bool) (*tls.Config, error) {
	certPEM, ok := secretData[TLSCertKey]
	if !ok {
		return nil, fmt.Errorf("TLS secret missing %s", TLSCertKey)
	}
	keyPEM, ok := secretData[TLSKeyKey]
	if !ok {
		return nil, fmt.Errorf("TLS secret missing %s", TLSKeyKey)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse server certificate: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		// This config is returned from GetConfigForClient during the
		// handshake, so ALPN must be declared here: gRPC requires h2.
		NextProtos: []string{"h2"},
	}

	if requireClientCert {
		caPEM, ok := secretData[TLSCAKey]
		if !ok {
			return nil, fmt.Errorf("mTLS requires %s in the TLS secret", TLSCAKey)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no certificates parsed from %s", TLSCAKey)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// BuildServerCredentials builds static gRPC transport credentials from a
// TLS secret's data. Rotation-aware callers should use DynamicServerTLS
// instead.
func BuildServerCredentials(secretData map[string][]byte, requireClientCert bool) (credentials.TransportCredentials, error) {
	cfg, err := buildServerTLSConfig(secretData, requireClientCert)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

// DynamicServerTLS serves TLS with hot-rotatable certificates: every
// handshake resolves the current config via GetConfigForClient, so
// updating the secret rotates the server certificate and client CA
// without restarting the gRPC listener or disturbing connected spokes.
type DynamicServerTLS struct {
	current atomic.Pointer[tls.Config]
}

// NewDynamicServerTLS builds the initial configuration from a TLS
// secret's data.
func NewDynamicServerTLS(secretData map[string][]byte, requireClientCert bool) (*DynamicServerTLS, error) {
	d := &DynamicServerTLS{}
	if err := d.Update(secretData, requireClientCert); err != nil {
		return nil, err
	}
	return d, nil
}

// Update parses new secret data and atomically swaps it in for future
// handshakes. Existing connections are unaffected.
func (d *DynamicServerTLS) Update(secretData map[string][]byte, requireClientCert bool) error {
	cfg, err := buildServerTLSConfig(secretData, requireClientCert)
	if err != nil {
		return err
	}
	d.current.Store(cfg)
	return nil
}

// Credentials returns gRPC transport credentials that follow the current
// configuration.
func (d *DynamicServerTLS) Credentials() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return d.current.Load(), nil
		},
	})
}
