// AWS Signature V4.
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

const (
	sigv4Algorithm     = "AWS4-HMAC-SHA256"
	sigv4Service       = "s3"
	sigv4TimeFmt       = "20060102T150405Z"
	sigv4DateFmt       = "20060102"
	sigv4MaxClockSkew  = 15 * time.Minute
	sigv4SignedHeaders = "host;x-amz-content-sha256;x-amz-date"
	emptyBodySHA256    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type SigV4Signer struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

// Sign adds X-Amz-Date, X-Amz-Content-Sha256, and Authorization entries to
// `headers`. `fullURL` is used only to extract host, path, and query for the canonical request.
func (s SigV4Signer) Sign(method, fullURL string, headers map[string]string, body []byte, now time.Time) error {
	u, err := url.Parse(fullURL)
	if err != nil {
		return lib.WrapErrorf(err, "invalid URL for signing")
	}
	now = now.UTC()
	amzDate := now.Format(sigv4TimeFmt)
	scopeDate := now.Format(sigv4DateFmt)
	payloadHash := bodyHash(body)

	headers["X-Amz-Date"] = amzDate
	headers["X-Amz-Content-Sha256"] = payloadHash

	canonical := canonicalRequest(method, urlPath(u), u.RawQuery, u.Host, payloadHash, amzDate)
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", scopeDate, s.Region, sigv4Service)
	toSign := fmt.Sprintf("%s\n%s\n%s\n%s", sigv4Algorithm, amzDate, credentialScope, bodyHash([]byte(canonical)))
	signature := hex.EncodeToString(
		hmacSHA256(deriveSigningKey(s.SecretAccessKey, scopeDate, s.Region), []byte(toSign)),
	)

	headers["Authorization"] = fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigv4Algorithm, s.AccessKeyID, credentialScope, sigv4SignedHeaders, signature,
	)
	return nil
}

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

func canonicalRequest(method, path, rawQuery, host, payloadHash, amzDate string) string {
	return fmt.Sprintf(
		"%s\n%s\n%s\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n\n%s\n%s",
		method, path, rawQuery, host, payloadHash, amzDate, sigv4SignedHeaders, payloadHash,
	)
}

func urlPath(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}
	return u.Path
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

func deriveSigningKey(secret, scopeDate, region string) []byte {
	k := hmacSHA256(fmt.Appendf(nil, "AWS4%s", secret), []byte(scopeDate))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(sigv4Service))
	return hmacSHA256(k, []byte("aws4_request"))
}

func hmacSHA256(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}

func bodyHash(b []byte) string {
	if len(b) == 0 {
		return emptyBodySHA256
	}
	h := lib.CalculateSha256(b)
	return hex.EncodeToString(h[:])
}
