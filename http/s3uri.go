// S3 URI encoding and decoding of the form:
//
//	s3+https://<base64url(argon2id-phc)>:<base64url(ciphertext)>@<host>[/<prefix>]
package http

import (
	"bytes"
	"crypto/cipher"
	"encoding/base64"
	"net/url"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

const s3URIPrefix = "s3+"

type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey []byte
}

func IsS3StorageURI(uri string) bool {
	return strings.HasPrefix(uri, s3URIPrefix+"http://") || strings.HasPrefix(uri, s3URIPrefix+"https://")
}

// RejectBareHTTPURI returns an error when `uri` is a plain `http://` or
// `https://` URL.
func RejectBareHTTPURI(uri string) error {
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return lib.Errorf("remote URIs must use the `s3+` prefix, got %s", uri)
	}
	return nil
}

func ParseS3Endpoint(endpoint string, creds S3Credentials) (S3StorageConfig, error) {
	inner, err := parseS3URL(endpoint)
	if err != nil {
		return S3StorageConfig{}, err
	}
	if inner.User != nil {
		return S3StorageConfig{}, lib.Errorf("endpoint must not carry credentials")
	}
	return S3StorageConfig{
		BucketURL:       inner.Scheme + "://" + inner.Host,
		Region:          regionFromHost(inner.Host),
		Prefix:          strings.Trim(inner.Path, "/"),
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
	}, nil
}

// S3URIHasEmbeddedCredentials reports whether the URI already carries an
// encrypted credentials blob in its userinfo (as produced by [EncodeS3URI]).
func S3URIHasEmbeddedCredentials(uri string) bool {
	u, err := parseS3URL(uri)
	if err != nil {
		return false
	}
	return u.User != nil
}

func EncodeS3URI(rawURL string, creds S3Credentials, passphrase []byte) (string, error) {
	if creds.AccessKeyID == "" || len(creds.SecretAccessKey) == 0 {
		return "", lib.Errorf("S3 credentials must not be empty")
	}
	if strings.ContainsAny(creds.AccessKeyID, ":\n") {
		return "", lib.Errorf("access key must not contain ':' or newline")
	}
	inner, err := parseS3URL(rawURL)
	if err != nil {
		return "", err
	}
	if inner.User != nil {
		return "", lib.Errorf("URL already contains userinfo")
	}

	salt, err := lib.NewSalt()
	if err != nil {
		return "", lib.WrapErrorf(err, "failed to generate salt")
	}
	argon := lib.NewArgon2id(salt)
	aead, err := cipherFromPassphrase(passphrase, argon)
	if err != nil {
		return "", err
	}

	plain := append([]byte(creds.AccessKeyID+":"), creds.SecretAccessKey...)
	encrypted := make([]byte, len(plain)+lib.TotalCipherOverhead)
	out, err := lib.Encrypt(plain, aead, []byte(uriAAD(inner)), encrypted)
	if err != nil {
		return "", lib.WrapErrorf(err, "failed to encrypt credentials")
	}

	argonB64 := base64.RawURLEncoding.EncodeToString([]byte(argon.Marshal()))
	inner.User = url.UserPassword(argonB64, base64.RawURLEncoding.EncodeToString(out))
	return s3URIPrefix + inner.String(), nil
}

func DecodeS3URI(uri string, passphrase []byte) (S3StorageConfig, string, error) {
	inner, err := parseS3URL(uri)
	if err != nil {
		return S3StorageConfig{}, "", err
	}
	if inner.User == nil {
		return S3StorageConfig{}, "", lib.Errorf("S3 URI is missing credentials")
	}
	argonB64 := inner.User.Username()
	encB64, ok := inner.User.Password()
	if !ok || encB64 == "" {
		return S3StorageConfig{}, "", lib.Errorf("S3 URI is missing ciphertext")
	}

	argonBytes, err := base64.RawURLEncoding.DecodeString(argonB64)
	if err != nil {
		return S3StorageConfig{}, "", lib.WrapErrorf(err, "invalid base64url argon2id field in URI")
	}
	argon, err := lib.UnmarshalArgon2idConfig(string(argonBytes))
	if err != nil {
		return S3StorageConfig{}, "", lib.WrapErrorf(err, "invalid argon2id config in URI")
	}
	encrypted, err := base64.RawURLEncoding.DecodeString(encB64)
	if err != nil {
		return S3StorageConfig{}, "", lib.WrapErrorf(err, "invalid base64url in URI")
	}
	if len(encrypted) <= lib.TotalCipherOverhead {
		return S3StorageConfig{}, "", lib.Errorf("ciphertext too short")
	}
	aead, err := cipherFromPassphrase(passphrase, argon)
	if err != nil {
		return S3StorageConfig{}, "", err
	}

	cleartext := *inner
	cleartext.User = nil
	plain, err := lib.Decrypt(encrypted, aead, []byte(uriAAD(&cleartext)), make([]byte, len(encrypted)))
	if err != nil {
		return S3StorageConfig{}, "", lib.WrapErrorf(err, "failed to decrypt credentials (wrong passphrase?)")
	}
	akBytes, secretKey, ok := bytes.Cut(plain, []byte{':'})
	if !ok {
		return S3StorageConfig{}, "", lib.Errorf("decrypted credentials missing separator")
	}
	return S3StorageConfig{
		BucketURL:       cleartext.Scheme + "://" + cleartext.Host,
		Region:          regionFromHost(cleartext.Host),
		Prefix:          strings.Trim(cleartext.Path, "/"),
		AccessKeyID:     string(akBytes),
		SecretAccessKey: secretKey,
	}, s3URIPrefix + cleartext.String(), nil
}

func cipherFromPassphrase(passphrase []byte, argon lib.Argon2id) (cipher.AEAD, error) {
	key, err := lib.DeriveUserKey(passphrase, argon)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to derive key")
	}
	c, err := lib.NewCipher(key)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to build cipher")
	}
	return c, nil
}

func uriAAD(u *url.URL) string {
	out := u.Scheme + "://" + u.Host + u.Path
	if u.RawQuery != "" {
		out += "?" + u.RawQuery
	}
	return out
}

// parseS3URL strips the `s3+` prefix and parses the inner http(s) URL.
func parseS3URL(raw string) (*url.URL, error) {
	rest, ok := strings.CutPrefix(raw, s3URIPrefix)
	if !ok {
		return nil, lib.Errorf("expected %q prefix, got %q", s3URIPrefix, raw)
	}
	u, err := url.Parse(rest)
	if err != nil {
		return nil, lib.WrapErrorf(err, "invalid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, lib.Errorf("expected http(s):// inside s3+ URI, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, lib.Errorf("URL is missing host")
	}
	u.Fragment = ""
	return u, nil
}

// regionFromHost extracts the AWS-style region label (the segment after
// `s3.` in the hostname). Returns "us-east-1" when no such label is present
// (e.g. `localhost:9000` for our own server).
func regionFromHost(host string) string {
	parts := strings.Split(host, ".")
	for i, lbl := range parts {
		if lbl == "s3" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return "us-east-1"
}
