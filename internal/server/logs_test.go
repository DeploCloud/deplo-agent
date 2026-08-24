package server

import (
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The argv is the whole contract of the time-range feature: a dropped or
// misspelled flag streams the WRONG window and looks exactly like a container
// that had nothing to say. No docker daemon involved.
func TestLogArgs(t *testing.T) {
	cases := []struct {
		name string
		req  *pb.FollowLogsRequest
		want string
	}{
		{
			// An older control plane sends none of the new fields, and must get
			// byte-for-byte the argv this produced before they existed.
			name: "no window",
			req:  &pb.FollowLogsRequest{Container: "deplo-web"},
			want: "logs -f --tail 500 deplo-web",
		},
		{
			name: "since only",
			req:  &pb.FollowLogsRequest{Container: "deplo-web", SinceUnix: 1756000000},
			want: "logs -f --tail 500 --since 1756000000 deplo-web",
		},
		{
			name: "full window with timestamps",
			req: &pb.FollowLogsRequest{
				Container:  "deplo-web",
				SinceUnix:  1756000000,
				UntilUnix:  1756003600,
				Timestamps: true,
			},
			want: "logs -f --tail 500 --timestamps --since 1756000000 --until 1756003600 deplo-web",
		},
		{
			// 0 is "unset", not "the epoch": a zero must never become `--since 0`,
			// which docker reads as 1970 and would defeat the tail window.
			name: "zero is unset, not the epoch",
			req:  &pb.FollowLogsRequest{Container: "deplo-web", SinceUnix: 0, UntilUnix: 0},
			want: "logs -f --tail 500 deplo-web",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(logArgs(defaultLogTail, tc.req), " ")
			if got != tc.want {
				t.Fatalf("logArgs = %q, want %q", got, tc.want)
			}
		})
	}
}
