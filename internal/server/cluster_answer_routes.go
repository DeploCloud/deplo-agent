package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// The proxy's routes for one App's answer, as Deplo renders them: Traefik dynamic configuration, JSON being
// YAML too. Only these forms pass, so a table can route an App's own hostnames to its own ports and nothing else.

const (
	// answerTransport is the proxy's TCP serversTransport that sends the PROXY header, from Deplo's own file.
	answerTransport = "deplo-answer@file"
	answerChallenge = "PathPrefix(`/.well-known/acme-challenge/`)"
)

var (
	answerHost = `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+`
	// A TCP router takes the App's hostnames by SNI, and an HTTP router their certificate checks only.
	answerSNIRule = regexp.MustCompile("^HostSNI\\(`" + answerHost + "`\\)( \\|\\| HostSNI\\(`" + answerHost + "`\\))*$")
	answerWebRule = regexp.MustCompile("^(Host\\(`" + answerHost + "`\\)|\\(Host\\(`" + answerHost + "`\\)( \\|\\| Host\\(`" +
		answerHost + "`\\))+\\)) && " + regexp.QuoteMeta(answerChallenge) + "$")
	answerSuffix  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	answerRuleArg = regexp.MustCompile("`([^`]*)`")
)

type answerRoutes struct {
	TCP  *answerTCP  `json:"tcp,omitempty"`
	HTTP *answerHTTP `json:"http,omitempty"`
	// TLS is this agent's own: the certificates a failover installed as files. Never from a table.
	TLS *answerTLSFiles `json:"tls,omitempty"`
}

type answerTLSFiles struct {
	Certificates []answerTLSFile `json:"certificates"`
}

// answerTLSFile is one pair given inline: Traefik reads a value that names no file as the content itself.
type answerTLSFile struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

type answerTCP struct {
	Routers  map[string]answerTCPRouter  `json:"routers"`
	Services map[string]answerTCPService `json:"services"`
}

type answerTCPRouter struct {
	Rule        string   `json:"rule"`
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Priority    int64    `json:"priority,omitempty"`
	TLS         struct {
		Passthrough bool `json:"passthrough"`
	} `json:"tls"`
}

type answerTCPService struct {
	LoadBalancer struct {
		ServersTransport string `json:"serversTransport"`
		Servers          []struct {
			Address string `json:"address"`
		} `json:"servers"`
	} `json:"loadBalancer"`
}

type answerHTTP struct {
	Routers  map[string]answerHTTPRouter  `json:"routers"`
	Services map[string]answerHTTPService `json:"services"`
}

type answerHTTPRouter struct {
	Rule        string   `json:"rule"`
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Priority    int64    `json:"priority,omitempty"`
	// TLS is only this agent's own, on the router that orders a zone's certificate. Never from a table.
	TLS *answerRouterTLS `json:"tls,omitempty"`
}

type answerRouterTLS struct {
	CertResolver string            `json:"certResolver"`
	Domains      []answerTLSDomain `json:"domains"`
}

type answerTLSDomain struct {
	Main string   `json:"main"`
	SANs []string `json:"sans"`
}

type answerHTTPService struct {
	LoadBalancer struct {
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
	} `json:"loadBalancer"`
}

// answerName is the prefix of every router and service of one App's answer.
func answerName(appID string) string { return "deplo-answer-" + appID }

// parseAnswerRoutes reads one App's routes and refuses any form the answer does not use.
func parseAnswerRoutes(e answer, raw string) (*answerRoutes, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var r answerRoutes
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("its routes are not the proxy's: %v", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("its routes are more than one document")
	}
	if r.TLS != nil {
		return nil, fmt.Errorf("its routes carry certificates, which only a failover installs")
	}
	if r.TCP == nil || len(r.TCP.Routers) == 0 {
		return nil, fmt.Errorf("it passes no hostname on")
	}
	own := func(name string) bool {
		p := answerName(e.AppID)
		return name == p || (strings.HasPrefix(name, p+"-") && answerSuffix.MatchString(name[len(p)+1:]))
	}
	for name, s := range r.TCP.Services {
		lb := s.LoadBalancer
		if !own(name) || lb.ServersTransport != answerTransport || len(lb.Servers) != 1 ||
			lb.Servers[0].Address != answerAlias+":"+strconv.Itoa(int(e.TLSPort)) {
			return nil, fmt.Errorf("TCP service %q does not reach its own port with the PROXY header", name)
		}
	}
	for name, rt := range r.TCP.Routers {
		if !own(name) || !answerSNIRule.MatchString(rt.Rule) || !slices.Equal(rt.EntryPoints, []string{"websecure"}) ||
			!rt.TLS.Passthrough || rt.Priority < 0 {
			return nil, fmt.Errorf("TCP router %q is not a passthrough of its hostnames on websecure", name)
		}
		if _, ok := r.TCP.Services[rt.Service]; !ok {
			return nil, fmt.Errorf("TCP router %q names a service it does not define", name)
		}
	}
	if r.HTTP == nil {
		return &r, nil
	}
	for name, s := range r.HTTP.Services {
		if !own(name) || len(s.LoadBalancer.Servers) != 1 ||
			s.LoadBalancer.Servers[0].URL != "http://"+answerAlias+":"+strconv.Itoa(int(e.WebPort)) {
			return nil, fmt.Errorf("HTTP service %q does not reach its own port", name)
		}
	}
	for name, rt := range r.HTTP.Routers {
		if !own(name) || !answerWebRule.MatchString(rt.Rule) || !slices.Equal(rt.EntryPoints, []string{"web"}) || rt.Priority < 0 ||
			rt.TLS != nil {
			return nil, fmt.Errorf("HTTP router %q is not the certificate checks of its hostnames on web", name)
		}
		if _, ok := r.HTTP.Services[rt.Service]; !ok {
			return nil, fmt.Errorf("HTTP router %q names a service it does not define", name)
		}
	}
	return &r, nil
}

// hosts is every hostname the routes pass on by SNI.
func (r *answerRoutes) hosts() []string {
	var out []string
	for _, rt := range r.TCP.Routers {
		for _, m := range answerRuleArg.FindAllStringSubmatch(rt.Rule, -1) {
			if !slices.Contains(out, m[1]) {
				out = append(out, m[1])
			}
		}
	}
	return out
}

// renderAnswerRoutes is the proxy's routes file: every App's checked routes in one document, written from
// what was checked rather than from what was sent, and this agent's own zone routers. Names never collide.
func renderAnswerRoutes(answers []answer, files []proxyCert, zones map[string]answerHTTPRouter) []byte {
	all := answerRoutes{TCP: &answerTCP{Routers: map[string]answerTCPRouter{}, Services: map[string]answerTCPService{}},
		HTTP: &answerHTTP{Routers: map[string]answerHTTPRouter{}, Services: map[string]answerHTTPService{}}}
	for _, e := range answers {
		if e.Routes == nil {
			continue
		}
		if e.Routes.TCP != nil {
			for k, v := range e.Routes.TCP.Routers {
				all.TCP.Routers[k] = v
			}
			for k, v := range e.Routes.TCP.Services {
				all.TCP.Services[k] = v
			}
		}
		if e.Routes.HTTP != nil {
			for k, v := range e.Routes.HTTP.Routers {
				all.HTTP.Routers[k] = v
			}
			for k, v := range e.Routes.HTTP.Services {
				all.HTTP.Services[k] = v
			}
		}
	}
	for k, v := range zones {
		all.HTTP.Routers[k] = v
	}
	for _, c := range files {
		if all.TLS == nil {
			all.TLS = &answerTLSFiles{}
		}
		all.TLS.Certificates = append(all.TLS.Certificates, answerTLSFile{CertFile: c.Cert, KeyFile: c.Key})
	}
	if len(all.TCP.Routers) == 0 && len(all.HTTP.Routers) == 0 && all.TLS == nil {
		return []byte("{}\n")
	}
	if len(all.TCP.Routers) == 0 {
		all.TCP = nil
	}
	if len(all.HTTP.Routers) == 0 {
		all.HTTP = nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(all)
	return buf.Bytes()
}
