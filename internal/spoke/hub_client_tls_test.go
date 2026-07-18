package spoke

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/kapture-io/kapture/internal/hub"
)

// testPKI generates a CA plus server and client leaf certificates for
// localhost, written as PEM files.
type testPKI struct {
	dir        string
	caPEM      string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	pki := &testPKI{dir: dir}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kapture-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pki.caPEM = pki.writePEM(t, "ca.crt", "CERTIFICATE", caDER)

	issue := func(cn string, usage x509.ExtKeyUsage, certFile, keyFile string) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pki.writePEM(t, certFile, "CERTIFICATE", der),
			pki.writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	}

	pki.serverCert, pki.serverKey = issue("kapture-hub", x509.ExtKeyUsageServerAuth, "server.crt", "server.key")
	pki.clientCert, pki.clientKey = issue("kapture-spoke", x509.ExtKeyUsageClientAuth, "client.crt", "client.key")
	return pki
}

func (p *testPKI) writePEM(t *testing.T, name, blockType string, der []byte) string {
	t.Helper()
	path := filepath.Join(p.dir, name)
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (p *testPKI) read(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestHubSpokeMTLS_EndToEnd runs a real TLS hub server and registers a
// spoke over mTLS, then verifies clients without certificates are
// rejected.
func TestHubSpokeMTLS_EndToEnd(t *testing.T) {
	pki := newTestPKI(t)

	secretData := map[string][]byte{
		hub.TLSCertKey: pki.read(t, pki.serverCert),
		hub.TLSKeyKey:  pki.read(t, pki.serverKey),
		hub.TLSCAKey:   pki.read(t, pki.caPEM),
	}
	creds, err := hub.BuildServerCredentials(secretData, true)
	if err != nil {
		t.Fatalf("BuildServerCredentials: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // free the port; the server re-listens on it

	server := hub.NewServerWithTLS(addr, creds)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Start(ctx) }()
	defer server.Stop()

	waitForListen(t, addr)

	// mTLS client: registration succeeds.
	tlsCfg, err := BuildHubTLSConfig(HubTLSFiles{
		CertFile:   pki.clientCert,
		KeyFile:    pki.clientKey,
		CAFile:     pki.caPEM,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("BuildHubTLSConfig: %v", err)
	}

	// The spoke name must match the client certificate identity
	// (CN "kapture-spoke"): the hub binds spoke_id to the cert.
	client := NewHubClient(HubClientConfig{
		HubAddress: addr,
		SpokeName:  "kapture-spoke",
		ClusterID:  "kapture-spoke",
		Cell:       "cell-tls",
		TLSConfig:  tlsCfg,
		Logger:     logr.Discard(),
	})
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
	defer regCancel()
	if _, err := client.Register(regCtx); err != nil {
		t.Fatalf("Register over mTLS: %v", err)
	}

	// Impersonation: a valid fleet certificate must not allow registering
	// under a different spoke_id than its certificate identity.
	impostor := NewHubClient(HubClientConfig{
		HubAddress: addr,
		SpokeName:  "some-other-spoke",
		ClusterID:  "some-other-spoke",
		TLSConfig:  tlsCfg, // same valid cert, wrong claimed identity
		Logger:     logr.Discard(),
	})
	if err := impostor.Connect(ctx); err != nil {
		t.Fatalf("Connect (impostor): %v", err)
	}
	defer impostor.Close()
	impCtx, impCancel := context.WithTimeout(ctx, 5*time.Second)
	defer impCancel()
	if _, err := impostor.Register(impCtx); err == nil {
		t.Fatal("registration under a spoke_id not matching the certificate succeeded; identity binding not enforced")
	}

	// Client without a certificate: the hub must reject it.
	noCertCfg, err := BuildHubTLSConfig(HubTLSFiles{
		CAFile:     pki.caPEM,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	badClient := NewHubClient(HubClientConfig{
		HubAddress: addr,
		SpokeName:  "no-cert-spoke",
		ClusterID:  "no-cert-spoke",
		TLSConfig:  noCertCfg,
		Logger:     logr.Discard(),
	})
	if err := badClient.Connect(ctx); err != nil {
		t.Fatalf("Connect (lazy) should not fail: %v", err)
	}
	defer badClient.Close()

	badCtx, badCancel := context.WithTimeout(ctx, 5*time.Second)
	defer badCancel()
	if _, err := badClient.Register(badCtx); err == nil {
		t.Fatal("registration without a client certificate succeeded; mTLS not enforced")
	}
}

// TestHubTLSRotation_InPlace rotates the hub's entire PKI (server cert,
// key, and client CA) through DynamicServerTLS while the server keeps
// running: clients on the new PKI must connect without a server restart,
// and clients on the old PKI must be rejected afterwards.
func TestHubTLSRotation_InPlace(t *testing.T) {
	pkiA := newTestPKI(t)
	pkiB := newTestPKI(t)

	secretFor := func(p *testPKI) map[string][]byte {
		return map[string][]byte{
			hub.TLSCertKey: p.read(t, p.serverCert),
			hub.TLSKeyKey:  p.read(t, p.serverKey),
			hub.TLSCAKey:   p.read(t, p.caPEM),
		}
	}

	dynamicTLS, err := hub.NewDynamicServerTLS(secretFor(pkiA), true)
	if err != nil {
		t.Fatalf("NewDynamicServerTLS: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	server := hub.NewServerWithTLS(addr, dynamicTLS.Credentials())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Start(ctx) }()
	defer server.Stop()
	waitForListen(t, addr)

	register := func(p *testPKI, spokeName string) error {
		tlsCfg, err := BuildHubTLSConfig(HubTLSFiles{
			CertFile:   p.clientCert,
			KeyFile:    p.clientKey,
			CAFile:     p.caPEM,
			ServerName: "localhost",
		})
		if err != nil {
			return err
		}
		client := NewHubClient(HubClientConfig{
			HubAddress: addr,
			SpokeName:  spokeName,
			ClusterID:  spokeName,
			TLSConfig:  tlsCfg,
			Logger:     logr.Discard(),
		})
		if err := client.Connect(ctx); err != nil {
			return err
		}
		defer client.Close()
		regCtx, regCancel := context.WithTimeout(ctx, 5*time.Second)
		defer regCancel()
		_, err = client.Register(regCtx)
		return err
	}

	if err := register(pkiA, "kapture-spoke"); err != nil {
		t.Fatalf("register with original PKI: %v", err)
	}

	// Rotate the whole PKI in place — no server restart.
	if err := dynamicTLS.Update(secretFor(pkiB), true); err != nil {
		t.Fatalf("rotate TLS: %v", err)
	}

	if err := register(pkiB, "kapture-spoke"); err != nil {
		t.Fatalf("register with rotated PKI after in-place rotation: %v", err)
	}
	if err := register(pkiA, "kapture-spoke"); err == nil {
		t.Fatal("client on the retired PKI still accepted after rotation")
	}
}

func TestBuildServerCredentials_Validation(t *testing.T) {
	pki := newTestPKI(t)

	// mTLS without a CA must fail.
	_, err := hub.BuildServerCredentials(map[string][]byte{
		hub.TLSCertKey: pki.read(t, pki.serverCert),
		hub.TLSKeyKey:  pki.read(t, pki.serverKey),
	}, true)
	if err == nil {
		t.Error("mTLS without ca.crt accepted")
	}

	// Missing key material must fail.
	if _, err := hub.BuildServerCredentials(map[string][]byte{}, false); err == nil {
		t.Error("empty secret accepted")
	}
}

func TestBuildHubTLSConfig_Validation(t *testing.T) {
	pki := newTestPKI(t)

	// Cert without key must fail.
	if _, err := BuildHubTLSConfig(HubTLSFiles{CertFile: pki.clientCert}); err == nil {
		t.Error("client cert without key accepted")
	}
	// Garbage CA must fail.
	bad := filepath.Join(t.TempDir(), "bad.crt")
	if err := os.WriteFile(bad, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildHubTLSConfig(HubTLSFiles{CAFile: bad}); err == nil {
		t.Error("garbage CA accepted")
	}
}

func waitForListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("hub server never listened on %s", addr)
}
