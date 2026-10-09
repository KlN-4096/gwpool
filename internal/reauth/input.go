// Package reauth implements a bounded login state machine. It does not solve
// browser/anti-bot challenges, register accounts, or persist login secrets.
package reauth

import (
	"crypto/hmac"
	"crypto/sha1" // RFC 6238's interoperable six-digit TOTP profile.
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"strings"
	"time"
)

type Input struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	TOTP     string `json:"totp"`
}

func (Input) String() string   { return "LoginInput{***}" }
func (Input) GoString() string { return "LoginInput{***}" }

// Validate never trims the password: leading/trailing whitespace may be real.
func (in *Input) Validate() error {
	in.Email = strings.TrimSpace(in.Email)
	address, err := mail.ParseAddress(in.Email)
	if err != nil || address.Address != in.Email || len(in.Email) > 254 ||
		strings.ContainsAny(in.Email, "\r\n") {
		return errors.New("邮箱格式不正确")
	}
	if in.Password == "" || len(in.Password) > 1024 || strings.ContainsAny(in.Password, "\x00\r\n") {
		return errors.New("密码为空、过长或包含换行")
	}
	in.TOTP = strings.TrimSpace(in.TOTP)
	if in.TOTP == "" {
		return nil
	}
	normalized, err := normalizeTOTP(in.TOTP)
	if err != nil {
		return err
	}
	in.TOTP = normalized
	return nil
}

func normalizeTOTP(secret string) (string, error) {
	invalid := errors.New("2FA 需为验证器密钥或六位/30秒/SHA1的 otpauth 地址，不是六位动态码")
	if len(secret) > 2048 {
		return "", invalid
	}
	if strings.HasPrefix(strings.ToLower(secret), "otpauth:") {
		u, err := url.Parse(secret)
		if err != nil || u.Scheme != "otpauth" || u.Host != "totp" {
			return "", invalid
		}
		q := u.Query()
		if (q.Get("digits") != "" && q.Get("digits") != "6") ||
			(q.Get("period") != "" && q.Get("period") != "30") ||
			(q.Get("algorithm") != "" && !strings.EqualFold(q.Get("algorithm"), "SHA1")) {
			return "", invalid
		}
		secret = q.Get("secret")
	}
	secret = strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(secret), " ", ""), "-", ""))
	secret = strings.TrimRight(secret, "=")
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret)
	if err != nil || len(raw) < 10 || len(raw) > 128 {
		return "", invalid
	}
	return secret, nil
}

func totp(secret string, now time.Time) (string, error) {
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret)
	if err != nil || now.Unix() < 0 {
		return "", errors.New("2FA 密钥或系统时间无效")
	}
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], uint64(now.Unix()/30))
	mac := hmac.New(sha1.New, raw)
	_, _ = mac.Write(counter[:])
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 15
	code := (binary.BigEndian.Uint32(sum[offset:offset+4]) & 0x7fffffff) % 1000000
	return fmt.Sprintf("%06d", code), nil
}
