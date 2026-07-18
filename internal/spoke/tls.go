package spoke

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// HubTLSFiles points at the PEM files (typically a mounted TLS secret)
// used to authenticate the spoke-to-hub gRPC connection.
type HubTLSFiles struct {
	// CertFile/KeyFile are the spoke's client certificate for mTLS.
	// Optional when the hub does not require client certificates.
	CertFile string
	KeyFile  string
	// CAFile is the CA bundle that signed the hub's server certificate.
	CAFile string
	// ServerName overrides the expected server certificate name when the
	// dial address differs from the certificate SAN (e.g. connecting via
	// IP or a port-forward).
	ServerName string
}

// Configured reports whether any TLS file is set.
func (f HubTLSFiles) Configured() bool {
	return f.CertFile != "" || f.KeyFile != "" || f.CAFile != ""
}

// BuildHubTLSConfig builds the client TLS configuration for the hub
// connection.
func BuildHubTLSConfig(files HubTLSFiles) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: files.ServerName,
	}

	if files.CAFile != "" {
		caPEM, err := os.ReadFile(files.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read hub CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no certificates parsed from %s", files.CAFile)
		}
		cfg.RootCAs = pool
	}

	if files.CertFile != "" || files.KeyFile != "" {
		if files.CertFile == "" || files.KeyFile == "" {
			return nil, fmt.Errorf("hub TLS client certificate requires both cert and key files")
		}
		// Fail fast on unloadable certificates at startup...
		if _, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile); err != nil {
			return nil, fmt.Errorf("load hub client certificate: %w", err)
		}
		// ...but re-read the files on every handshake so rotated secret
		// mounts (kubelet updates them in place) take effect on the next
		// reconnect without restarting the spoke.
		certFile, keyFile := files.CertFile, files.KeyFile
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("reload hub client certificate: %w", err)
			}
			return &cert, nil
		}
	}

	return cfg, nil
}
