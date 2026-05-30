//go:build !wasm

// AWS Signature V4 request verification (server side). We put this in an extra
// file so that it gets excluded in wasm builds (it depends on net/http).

package http

import (
	"crypto/hmac"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

// VerifySigV4 re-signs the request with `secretAccessKey` and compares to the
// Authorization header. The header's access key must equal `accessKeyID`.
// `now` is the server's clock for the ±15-minute skew window check.
func VerifySigV4(
	req *http.Request,
	body []byte,
	region string,
	accessKeyID,
	secretAccessKey string,
	now time.Time,
) error {
	authHeader := req.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, sigv4Algorithm+" ") {
		return lib.Errorf("missing or unsupported Authorization scheme")
	}
	credential, signedHeaders, signature := parseAuth(authHeader[len(sigv4Algorithm)+1:])
	if credential == "" || signature == "" {
		return lib.Errorf("malformed Authorization header")
	}
	if signedHeaders != sigv4SignedHeaders {
		return lib.Errorf("unexpected signed-headers %q", signedHeaders)
	}
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 || credParts[2] != region || credParts[3] != sigv4Service || credParts[4] != "aws4_request" {
		return lib.Errorf("invalid credential scope %q", credential)
	}
	accessKey, scopeDate := credParts[0], credParts[1]
	if accessKey != accessKeyID {
		return lib.Errorf("unknown access key")
	}

	amzDate := req.Header.Get("X-Amz-Date")
	t, err := time.Parse(sigv4TimeFmt, amzDate)
	if err != nil {
		return lib.WrapErrorf(err, "invalid X-Amz-Date")
	}
	if t.Format(sigv4DateFmt) != scopeDate {
		return lib.Errorf("X-Amz-Date date does not match credential scope")
	}
	skew := now.Sub(t)
	if skew < 0 {
		skew = -skew
	}
	if skew > sigv4MaxClockSkew {
		return lib.Errorf("request expired (clock skew %s)", skew)
	}

	payloadHash := req.Header.Get("X-Amz-Content-Sha256")
	if payloadHash != bodyHash(body) {
		return lib.Errorf("body hash mismatch")
	}
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	canonical := canonicalRequest(req.Method, urlPath(req.URL), req.URL.RawQuery, host, payloadHash, amzDate)
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", scopeDate, region, sigv4Service)
	toSign := fmt.Sprintf("%s\n%s\n%s\n%s", sigv4Algorithm, amzDate, credentialScope, bodyHash([]byte(canonical)))
	expected := hex.EncodeToString(hmacSHA256(deriveSigningKey(secretAccessKey, scopeDate, region), []byte(toSign)))
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return lib.Errorf("signature mismatch")
	}
	return nil
}

func parseAuth(s string) (credential, signedHeaders, signature string) {
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			credential = part[len("Credential="):]
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeaders = part[len("SignedHeaders="):]
		case strings.HasPrefix(part, "Signature="):
			signature = part[len("Signature="):]
		}
	}
	return credential, signedHeaders, signature
}
