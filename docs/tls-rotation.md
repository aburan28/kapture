# Hub–Spoke mTLS and Certificate Rotation

The hub gRPC server serves TLS from the secret referenced by
`CaptureHub.spec.tls.certSecretRef` (keys `tls.crt`, `tls.key`, and, for
mTLS, `ca.crt`). With `spec.authentication.type: mTLS`, spokes must
present client certificates signed by that CA, and each spoke's
`spoke_id` is bound to its certificate identity (CN or DNS SAN).

## Rotation semantics

- **Hub**: the controller watches the referenced secret. When its content
  changes, the server certificate and client CA are swapped **in place** —
  the listener keeps running and connected spokes are not disturbed.
  Existing connections continue on the old certificates until they
  naturally reconnect; new handshakes use the rotated material
  immediately. Only changing the gRPC address, the secret *reference*, or
  toggling TLS/mTLS restarts the listener.
- **Spoke**: client certificates are mounted from a secret
  (`spoke.hub.tls.secretName` in the Helm chart). The kubelet refreshes
  mounted secrets in place, and the spoke re-reads the certificate files
  on every TLS handshake, so a rotated client certificate is picked up on
  the next reconnect without a pod restart. Changes to the CA *bundle*
  the spoke trusts are read at startup; rotating the CA itself requires a
  spoke restart (or a rolling update, which the overlap window below
  makes safe).

To rotate a CA without downtime, publish a bundle containing both old and
new CA certificates first, then rotate leaf certificates, then remove the
old CA from the bundle.

## cert-manager example

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: kapture-ca
  namespace: capture-system
spec:
  ca:
    secretName: kapture-root-ca   # your CA keypair
---
# Hub server certificate; cert-manager renews it and updates the secret,
# and the hub rotates in place.
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: kapture-hub-tls
  namespace: capture-system
spec:
  secretName: kapture-hub-tls
  issuerRef:
    name: kapture-ca
    kind: Issuer
  commonName: kapture-hub
  dnsNames:
    - kapture-hub
    - kapture-hub.capture-system.svc
    - kapture-hub.capture-system.svc.cluster.local
  duration: 2160h    # 90 days
  renewBefore: 360h  # renew 15 days early
  usages: ["server auth", "digital signature"]
---
# Spoke client certificate. The CN/DNS SAN must equal the spoke's
# SPOKE_NAME: the hub rejects spokes whose claimed identity does not
# match their certificate.
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: kapture-spoke-tls
  namespace: capture-system
spec:
  secretName: kapture-spoke-tls
  issuerRef:
    name: kapture-ca
    kind: Issuer
  commonName: kapture-spoke
  dnsNames: ["kapture-spoke"]
  duration: 2160h
  renewBefore: 360h
  usages: ["client auth", "digital signature"]
---
apiVersion: capture.gateway.io/v1alpha1
kind: CaptureHub
metadata:
  name: kapture-hub
spec:
  grpcAddress: ":9443"
  tls:
    certSecretRef:
      name: kapture-hub-tls
      namespace: capture-system
  authentication:
    type: mTLS
```

Spoke Helm values:

```yaml
spoke:
  cell: cell-a
  hub:
    address: kapture-hub.capture-system.svc:9443
    tls:
      secretName: kapture-spoke-tls
      serverName: kapture-hub.capture-system.svc
```

Note: cert-manager writes `ca.crt` into both secrets, which serves as the
client-CA bundle on the hub side and the server-trust bundle on the spoke
side.
