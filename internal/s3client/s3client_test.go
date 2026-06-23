package s3client

import "testing"

// New derives the endpoint host + TLS flag from the scheme the control plane
// sends: https:// (or no scheme) => TLS; http:// => plaintext (a local MinIO).
// We can't reach a real bucket here, but a successful client build proves the
// endpoint parsing accepted the input; an empty endpoint must be rejected.
func TestNew_endpointSchemeParsing(t *testing.T) {
	cases := []struct {
		endpoint string
		wantErr  bool
	}{
		{"s3.amazonaws.com", false},
		{"https://s3.amazonaws.com", false},
		{"http://127.0.0.1:9000", false},
		{"https://minio.example.com:9000/", false},
		{"", true},
	}
	for _, tc := range cases {
		_, err := New(Config{
			Endpoint:  tc.endpoint,
			Bucket:    "b",
			AccessKey: "ak",
			SecretKey: "sk",
		})
		if tc.wantErr && err == nil {
			t.Errorf("endpoint %q: expected an error", tc.endpoint)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("endpoint %q: unexpected error %v", tc.endpoint, err)
		}
	}
}

// PathStyle selects path-style bucket addressing (MinIO/compatibles); the
// default is DNS/virtual-host (AWS). A built client just needs to accept both.
func TestNew_pathStyle(t *testing.T) {
	for _, ps := range []bool{true, false} {
		if _, err := New(Config{Endpoint: "s3.example.com", Bucket: "b", AccessKey: "a", SecretKey: "s", PathStyle: ps}); err != nil {
			t.Errorf("pathStyle=%v: %v", ps, err)
		}
	}
}
