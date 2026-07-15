package hub

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

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

// BuildServerCredentials builds gRPC transport credentials for the hub
// server from a TLS secret's data. When requireClientCert is true (mTLS),
// the secret must also carry ca.crt and spokes must present certificates
// signed by it.
func BuildServerCredentials(secretData map[string][]byte, requireClientCert bool) (credentials.TransportCredentials, error) {
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

	return credentials.NewTLS(cfg), nil
}
