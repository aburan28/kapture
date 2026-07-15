package hub

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// verifySpokeIdentity binds the spoke_id claimed in a request to the
// identity in the caller's verified client certificate. With mTLS, a spoke
// may only act as the spoke_id matching its certificate's CommonName or
// one of its DNS SANs — without this check, any spoke holding a valid
// fleet certificate could register as another spoke, receive its
// directives, or forge its statuses.
//
// Connections without a verified client certificate (plaintext deployments
// or TLS without authentication.type: mTLS) are not subject to the check:
// identity enforcement is exactly as strong as the transport
// authentication configured on the CaptureHub.
func verifySpokeIdentity(ctx context.Context, spokeID string) error {
	identities, ok := peerCertIdentities(ctx)
	if !ok {
		return nil // no verified client certificate: nothing to bind against
	}

	for _, identity := range identities {
		if identity == spokeID {
			return nil
		}
	}
	return status.Errorf(codes.PermissionDenied,
		"spoke %q does not match the client certificate identity %v", spokeID, identities)
}

// peerCertIdentities returns the CommonName and DNS SANs of the caller's
// verified client certificate. ok is false when the connection carries no
// verified client certificate.
func peerCertIdentities(ctx context.Context) ([]string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return nil, false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, false
	}
	// VerifiedChains is only populated when the server verified the client
	// certificate (ClientAuth: RequireAndVerifyClientCert).
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return nil, false
	}

	leaf := tlsInfo.State.VerifiedChains[0][0]
	identities := make([]string, 0, 1+len(leaf.DNSNames))
	if leaf.Subject.CommonName != "" {
		identities = append(identities, leaf.Subject.CommonName)
	}
	identities = append(identities, leaf.DNSNames...)
	return identities, true
}
