package capture

import (
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

// Vendored @duckduckgo/autoconsent (MPL-2.0, see autoconsent/LICENSE and VERSION), gzipped.
//
//go:embed autoconsent/autoconsent.playwright.js.gz
var consentBundleGz []byte

//go:embed autoconsent/rules.json.gz
var consentRulesGz []byte

const consentBinding = "__deploAutoconsent"

// consentConfig rejects the banners of the consent tools autoconsent knows, and guesses at nothing else.
const consentConfig = `{"enabled":true,"autoAction":"optOut","enablePrehide":true,"enableCosmeticRules":true,"enableHeuristicDetection":false,"detectRetries":20,"logs":{"errors":false}}`

type consentBridge struct {
	popup, done atomic.Bool
	initResp    string
}

type consentMessage struct {
	Type string          `json:"type"`
	ID   json.RawMessage `json:"id"`
	Code string          `json:"code"`
}

func gunzip(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// startConsent injects autoconsent into every frame before its own scripts run and answers its messages.
func startConsent(ctx context.Context) (*consentBridge, error) {
	bundle, err := gunzip(consentBundleGz)
	if err != nil {
		return nil, err
	}
	rules, err := gunzip(consentRulesGz)
	if err != nil {
		return nil, err
	}
	c := &consentBridge{initResp: fmt.Sprintf(`{"type":"initResp","config":%s,"rules":%s}`, consentConfig, rules)}
	calls := chromedp.Events(ctx, runtime.BindingCalled)
	if _, err := chromedp.Call(ctx, runtime.AddBinding, runtime.AddBindingParams{Name: consentBinding}); err != nil {
		return nil, err
	}
	shim := `(() => { const send = window.` + consentBinding + `; window.autoconsentSendMessage = (m) => { try { send(JSON.stringify(m)); } catch (e) {} return Promise.resolve(); }; })();`
	if _, err := chromedp.Call(ctx, page.AddScriptToEvaluateOnNewDocument, page.AddScriptToEvaluateOnNewDocumentParams{Source: shim + "\n" + string(bundle)}); err != nil {
		return nil, err
	}
	go func() {
		for ev, err := range calls {
			if err != nil {
				return
			}
			if ev.Name == consentBinding {
				c.handle(ctx, ev)
			}
		}
	}()
	return c, nil
}

func (c *consentBridge) handle(ctx context.Context, ev runtime.EventBindingCalled) {
	var msg consentMessage
	if json.Unmarshal([]byte(ev.Payload), &msg) != nil {
		return
	}
	var expr string
	switch msg.Type {
	case "init":
		expr = "autoconsentReceiveMessage(" + c.initResp + ")"
	case "eval":
		code, _ := json.Marshal(msg.Code)
		id := msg.ID
		if len(id) == 0 {
			id = json.RawMessage("null")
		}
		expr = fmt.Sprintf(`(async () => { let r = false; try { r = await (0, eval)(%s); } catch (e) {} return autoconsentReceiveMessage({ id: %s, type: "evalResp", result: r }); })()`, code, id)
	case "popupFound":
		c.popup.Store(true)
	case "optOutResult", "autoconsentDone":
		c.done.Store(true)
	}
	if expr == "" {
		return
	}
	silent := true
	_, _ = chromedp.Call(ctx, runtime.Evaluate, runtime.EvaluateParams{Expression: expr, ContextID: ev.ExecutionContextID, Silent: &silent})
}

// wait gives a banner that was found the time to be dismissed.
func (c *consentBridge) wait(ctx context.Context, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for c.popup.Load() && !c.done.Load() && time.Now().Before(deadline) && ctx.Err() == nil {
		sleep(ctx, 100*time.Millisecond)
	}
}
