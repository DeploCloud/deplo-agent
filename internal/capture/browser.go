package capture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

const (
	maxSpec         = 64 << 10
	browserDeadline = 75 * time.Second
	navTimeout      = 30 * time.Second
	idleTimeout     = 10 * time.Second
	callTimeout     = 25 * time.Second
	// Main-frame documents a page may still load after its first one: a JS redirect, not a refresh loop.
	navigationsAfterLoad = 2
	settleDelay          = time.Second
	consentGrace         = 3 * time.Second
	viewportWidth        = 1280
	viewportHeight       = 800
	shotScale            = 0.75
	shotQuality          = 80
	headlessShell        = "/headless-shell/headless-shell"
	userAgentSuffix      = " Deplo-Screenshot/1"
)

// Main runs inside the browser's container: the spec on stdin, one Result on stdout.
func Main(in io.Reader, out io.Writer) int {
	var spec Spec
	if err := json.NewDecoder(io.LimitReader(in, maxSpec)).Decode(&spec); err != nil {
		_ = json.NewEncoder(out).Encode(Result{Outcome: OutcomeUnreachable, Detail: "unreadable spec"})
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), browserDeadline)
	defer cancel()
	if err := json.NewEncoder(out).Encode(capturePage(ctx, spec, SocketPath, headlessShell)); err != nil {
		return 1
	}
	return 0
}

func capturePage(ctx context.Context, spec Spec, sock, exe string) Result {
	target, err := url.Parse(spec.URL)
	if err != nil || target.Hostname() == "" {
		return Result{Outcome: OutcomeUnreachable, Detail: "bad url"}
	}
	proxy, err := ListenSplice(ctx, sock)
	if err != nil {
		return Result{Outcome: OutcomeUnreachable, Detail: err.Error()}
	}
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(exe),
		chromedp.NoSandbox,
		chromedp.WindowSize(viewportWidth, viewportHeight),
		chromedp.Flag("proxy-server", "http://"+proxy),
		chromedp.Flag("proxy-bypass-list", "<-loopback>"),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("disable-quic", true),
		chromedp.Flag("force-webrtc-ip-handling-policy", "disable_non_proxied_udp"),
		chromedp.Flag("disable-features", "Translate,BlinkGenPropertyTrees,IsolateOrigins,site-per-process"),
		chromedp.Flag("disable-site-isolation-trials", true),
		chromedp.Flag("hide-scrollbars", true),
		chromedp.Flag("mute-audio", true),
	)
	actx, cancel := chromedp.NewExecAllocator(ctx, opts...)
	defer cancel()
	bctx, cancel := chromedp.NewContext(actx)
	defer cancel()
	p := &pageRun{ctx: bctx, spec: spec, host: strings.ToLower(target.Hostname())}
	if err := p.prepare(); err != nil {
		return failure(ctx, err)
	}
	return p.capture()
}

type pageRun struct {
	ctx     context.Context
	spec    Spec
	host    string
	consent *consentBridge

	mu          sync.Mutex
	main        cdp.FrameID
	blankLoader cdp.LoaderID
	domReady    bool
	netIdle     bool
	docStatus   int64
	loaded      bool
	laterDocs   int
}

func (p *pageRun) prepare() error {
	ver, err := chromedp.CallBrowser(p.ctx, browser.GetVersion, cdp.Empty{})
	if err != nil {
		return err
	}
	ua := strings.Replace(ver.UserAgent, "HeadlessChrome", "Chrome", 1) + userAgentSuffix
	if _, err := chromedp.Call(p.ctx, emulation.SetUserAgentOverride, emulation.SetUserAgentOverrideParams{UserAgent: ua}); err != nil {
		return err
	}
	if _, err := chromedp.Call(p.ctx, emulation.SetDeviceMetricsOverride, emulation.SetDeviceMetricsOverrideParams{
		Width: viewportWidth, Height: viewportHeight, DeviceScaleFactor: 1,
	}); err != nil {
		return err
	}
	if err := p.setScheme("light"); err != nil {
		return err
	}
	if _, err := chromedp.Call(p.ctx, page.SetLifecycleEventsEnabled, page.SetLifecycleEventsEnabledParams{Enabled: true}); err != nil {
		return err
	}
	tree, err := chromedp.Call(p.ctx, page.GetFrameTree, cdp.Empty{})
	if err != nil || tree.FrameTree == nil {
		return fmt.Errorf("frame tree: %v", err)
	}
	p.main, p.blankLoader = tree.FrameTree.Frame.ID, tree.FrameTree.Frame.LoaderID
	go p.watchLifecycle(chromedp.Events(p.ctx, page.LifecycleEvent))
	go p.watchDocuments(chromedp.Events(p.ctx, network.ResponseReceived))
	go p.dismissDialogs(chromedp.Events(p.ctx, page.JavascriptDialogOpening))
	if p.consent, err = startConsent(p.ctx); err != nil {
		return err
	}
	return p.interceptRequests()
}

func (p *pageRun) setScheme(scheme string) error {
	ctx, cancel := context.WithTimeout(p.ctx, callTimeout)
	defer cancel()
	_, err := chromedp.Call(ctx, emulation.SetEmulatedMedia, emulation.SetEmulatedMediaParams{
		Features: []*emulation.MediaFeature{{Name: "prefers-color-scheme", Value: scheme}},
	})
	return err
}

func (p *pageRun) watchLifecycle(events iter.Seq2[page.EventLifecycleEvent, error]) {
	for ev, err := range events {
		if err != nil {
			return
		}
		if ev.FrameID != p.main || ev.LoaderID == p.blankLoader {
			continue
		}
		p.mu.Lock()
		switch ev.Name {
		case "init":
			p.netIdle = false
		case "DOMContentLoaded":
			p.domReady = true
		case "networkIdle":
			p.netIdle = true
		}
		p.mu.Unlock()
	}
}

func (p *pageRun) watchDocuments(events iter.Seq2[network.EventResponseReceived, error]) {
	for ev, err := range events {
		if err != nil {
			return
		}
		if ev.Type == network.ResourceTypeDocument && ev.FrameID == p.main && ev.Response != nil {
			p.mu.Lock()
			p.docStatus = ev.Response.Status
			p.mu.Unlock()
		}
	}
}

// dismissDialogs answers alert, confirm and beforeunload with Cancel, so none can hold the page.
func (p *pageRun) dismissDialogs(events iter.Seq2[page.EventJavascriptDialogOpening, error]) {
	for _, err := range events {
		if err != nil {
			return
		}
		_, _ = chromedp.Call(p.ctx, page.HandleJavaScriptDialog, page.HandleJavaScriptDialogParams{Accept: false})
	}
}

// interceptRequests stops a page that keeps navigating once it has loaded, and hands the
// credentials only to a challenge from the deployment's own host, once.
func (p *pageRun) interceptRequests() error {
	auth := p.spec.Username != ""
	patterns := []*fetch.RequestPattern{{URLPattern: "*", ResourceType: network.ResourceTypeDocument}}
	if auth {
		patterns = append(patterns, &fetch.RequestPattern{URLPattern: "http://" + p.host + "/*"}, &fetch.RequestPattern{URLPattern: "https://" + p.host + "/*"})
	}
	paused := chromedp.Events(p.ctx, fetch.RequestPaused)
	challenged := chromedp.Events(p.ctx, fetch.AuthRequired)
	if _, err := chromedp.Call(p.ctx, fetch.Enable, fetch.EnableParams{Patterns: patterns, HandleAuthRequests: &auth}); err != nil {
		return err
	}
	go func() {
		for ev, err := range paused {
			if err != nil {
				return
			}
			if ev.ResourceType == network.ResourceTypeDocument && ev.FrameID == p.main && p.navigationRefused() {
				_, _ = chromedp.Call(p.ctx, fetch.FailRequest, fetch.FailRequestParams{RequestID: ev.RequestID, ErrorReason: network.ErrorReasonAborted})
				continue
			}
			_, _ = chromedp.Call(p.ctx, fetch.ContinueRequest, fetch.ContinueRequestParams{RequestID: ev.RequestID})
		}
	}()
	if !auth {
		return nil
	}
	go func() {
		answered := map[fetch.RequestID]bool{}
		for ev, err := range challenged {
			if err != nil {
				return
			}
			resp := &fetch.AuthChallengeResponse{Response: fetch.AuthChallengeResponseResponseCancelAuth}
			if ev.AuthChallenge != nil && ev.AuthChallenge.Source != fetch.AuthChallengeSourceProxy &&
				originHost(ev.AuthChallenge.Origin) == p.host && !answered[ev.RequestID] {
				answered[ev.RequestID] = true
				resp = &fetch.AuthChallengeResponse{
					Response: fetch.AuthChallengeResponseResponseProvideCredentials,
					Username: p.spec.Username,
					Password: p.spec.Password,
				}
			}
			_, _ = chromedp.Call(p.ctx, fetch.ContinueWithAuth, fetch.ContinueWithAuthParams{RequestID: ev.RequestID, AuthChallengeResponse: resp})
		}
	}()
	return nil
}

func (p *pageRun) navigationRefused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.loaded {
		return false
	}
	p.laterDocs++
	return p.laterDocs > navigationsAfterLoad
}

func originHost(origin string) string {
	u, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

func (p *pageRun) capture() Result {
	navCtx, cancel := context.WithTimeout(p.ctx, navTimeout)
	defer cancel()
	nav, err := chromedp.Call(navCtx, page.Navigate, page.NavigateParams{URL: p.spec.URL})
	if err != nil {
		return failure(navCtx, err)
	}
	if nav.ErrorText != "" {
		return Result{Outcome: OutcomeUnreachable, Detail: nav.ErrorText}
	}
	// The DOM, not the load event: one hanging pixel must not cost the whole picture.
	if !p.waitFor(navCtx, func() bool { return p.domReady }) {
		return Result{Outcome: OutcomeTimeout, Detail: "the page never finished parsing"}
	}
	p.mu.Lock()
	status := int(p.docStatus)
	p.loaded = true
	p.mu.Unlock()
	if status == 0 {
		status = 200
	}
	if status < 200 || status >= 300 {
		return Result{Outcome: OutcomeHTTPError, Status: status}
	}
	p.waitIdle()
	sleep(p.ctx, settleDelay)
	p.consent.wait(p.ctx, consentGrace)

	light, err := p.shot(page.CaptureScreenshotFormatWebp, shotScale)
	if err != nil {
		return failure(p.ctx, err)
	}
	thumb, err := p.shot(page.CaptureScreenshotFormatPng, 0.1)
	if err != nil {
		return failure(p.ctx, err)
	}
	if blank, err := isBlank(thumb); err == nil && blank {
		return Result{Outcome: OutcomeBlank, Status: status}
	}
	// ponytail: the scheme switches without a reload; reload if sites miss it.
	if err := p.setScheme("dark"); err != nil {
		return failure(p.ctx, err)
	}
	sleep(p.ctx, settleDelay)
	dark, err := p.shot(page.CaptureScreenshotFormatWebp, shotScale)
	if err != nil {
		dark = nil
	} else if darkThumb, err := p.shot(page.CaptureScreenshotFormatPng, 0.1); err != nil || looksSame(thumb, darkThumb) {
		dark = nil
	}
	return Result{Outcome: OutcomeOK, Status: status, Light: light, Dark: dark}
}

func (p *pageRun) waitFor(ctx context.Context, cond func() bool) bool {
	for ctx.Err() == nil {
		p.mu.Lock()
		ok := cond()
		p.mu.Unlock()
		if ok {
			return true
		}
		sleep(ctx, 100*time.Millisecond)
	}
	return false
}

func (p *pageRun) waitIdle() {
	ctx, cancel := context.WithTimeout(p.ctx, idleTimeout)
	defer cancel()
	p.waitFor(ctx, func() bool { return p.netIdle })
}

func (p *pageRun) shot(format page.CaptureScreenshotFormat, scale float64) ([]byte, error) {
	params := page.CaptureScreenshotParams{
		Format: format,
		Clip:   &page.Viewport{X: 0, Y: 0, Width: viewportWidth, Height: viewportHeight, Scale: scale},
	}
	if format == page.CaptureScreenshotFormatWebp {
		q := int64(shotQuality)
		params.Quality = &q
	}
	ctx, cancel := context.WithTimeout(p.ctx, callTimeout)
	defer cancel()
	res, err := chromedp.Call(ctx, page.CaptureScreenshot, params)
	return res.Data, err
}

func failure(ctx context.Context, err error) Result {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return Result{Outcome: OutcomeTimeout, Detail: err.Error()}
	}
	return Result{Outcome: OutcomeUnreachable, Detail: err.Error()}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// ListenSplice gives a loopback proxy address whose every connection is carried to sock.
func ListenSplice(ctx context.Context, sock string) (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("proxy listener: %w", err)
	}
	context.AfterFunc(ctx, func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				u, err := net.Dial("unix", sock)
				if err != nil {
					return
				}
				defer u.Close()
				splice(c, u, nil)
			}()
		}
	}()
	return l.Addr().String(), nil
}
