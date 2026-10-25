// Package capture takes the deployment screenshots: a browser with no network of its own, whose
// only way out is the egress proxy the host serves on a unix socket.
package capture

// SocketPath is where the host mounts its egress socket inside the browser's container.
const SocketPath = "/egress/egress.sock"

// Spec is what the host hands the browser on stdin; it never travels in argv or the environment.
type Spec struct {
	URL      string `json:"url"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Outcomes a Result carries, mirrored by the proto's CaptureOutcome.
const (
	OutcomeOK          = "ok"
	OutcomeHTTPError   = "http_error"
	OutcomeBlank       = "blank"
	OutcomeUnreachable = "unreachable"
	OutcomeTimeout     = "timeout"
)

// Result is the one JSON object the browser writes to stdout. Fields only ever get added: during a
// self-update the container can run the new binary against the old host.
type Result struct {
	Outcome string `json:"outcome"`
	Status  int    `json:"status,omitempty"`
	Light   []byte `json:"light,omitempty"`
	Dark    []byte `json:"dark,omitempty"`
	Detail  string `json:"detail,omitempty"`
}
