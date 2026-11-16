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

// The proxy's routes for one App's entrance, as Deplo renders them: Traefik dynamic configuration, JSON being
// YAML too. Only these forms pass, so a table can route an App's own hostnames to its own ports and nothing else.

const (
	// entranceTransport is the proxy's TCP serversTransport that sends the PROXY header, from Deplo's own file.
	entranceTransport = "deplo-entrance@file"
	entranceChallenge = "PathPrefix(`/.well-known/acme-challenge/`)"
)

var (
	entranceHost = `[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+`
	// A TCP router takes the App's hostnames by SNI, and an HTTP router their certificate checks only.
	entranceSNIRule = regexp.MustCompile("^HostSNI\\(`" + entranceHost + "`\\)( \\|\\| HostSNI\\(`" + entranceHost + "`\\))*$")
	entranceWebRule = regexp.MustCompile("^(Host\\(`" + entranceHost + "`\\)|\\(Host\\(`" + entranceHost + "`\\)( \\|\\| Host\\(`" +
		entranceHost + "`\\))+\\)) && " + regexp.QuoteMeta(entranceChallenge) + "$")
	entranceSuffix  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	entranceRuleArg = regexp.MustCompile("`([^`]*)`")
)

type entranceRoutes struct {
	TCP  *entranceTCP  `json:"tcp,omitempty"`
	HTTP *entranceHTTP `json:"http,omitempty"`
}

type entranceTCP struct {
	Routers  map[string]entranceTCPRouter  `json:"routers"`
	Services map[string]entranceTCPService `json:"services"`
}

type entranceTCPRouter struct {
	Rule        string   `json:"rule"`
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Priority    int64    `json:"priority,omitempty"`
	TLS         struct {
		Passthrough bool `json:"passthrough"`
	} `json:"tls"`
}

type entranceTCPService struct {
	LoadBalancer struct {
		ServersTransport string `json:"serversTransport"`
		Servers          []struct {
			Address string `json:"address"`
		} `json:"servers"`
	} `json:"loadBalancer"`
}

type entranceHTTP struct {
	Routers  map[string]entranceHTTPRouter  `json:"routers"`
	Services map[string]entranceHTTPService `json:"services"`
}

type entranceHTTPRouter struct {
	Rule        string   `json:"rule"`
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Priority    int64    `json:"priority,omitempty"`
}

type entranceHTTPService struct {
	LoadBalancer struct {
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
	} `json:"loadBalancer"`
}

// entranceName is the prefix of every router and service of one App's entrance.
func entranceName(appID string) string { return "deplo-entrance-" + appID }

// parseEntranceRoutes reads one App's routes and refuses any form the entrance does not use.
func parseEntranceRoutes(e entrance, raw string) (*entranceRoutes, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var r entranceRoutes
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("its routes are not the proxy's: %v", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("its routes are more than one document")
	}
	if r.TCP == nil || len(r.TCP.Routers) == 0 {
		return nil, fmt.Errorf("it passes no hostname on")
	}
	own := func(name string) bool {
		p := entranceName(e.AppID)
		return name == p || (strings.HasPrefix(name, p+"-") && entranceSuffix.MatchString(name[len(p)+1:]))
	}
	for name, s := range r.TCP.Services {
		lb := s.LoadBalancer
		if !own(name) || lb.ServersTransport != entranceTransport || len(lb.Servers) != 1 ||
			lb.Servers[0].Address != entranceAlias+":"+strconv.Itoa(int(e.TLSPort)) {
			return nil, fmt.Errorf("TCP service %q does not reach its own port with the PROXY header", name)
		}
	}
	for name, rt := range r.TCP.Routers {
		if !own(name) || !entranceSNIRule.MatchString(rt.Rule) || !slices.Equal(rt.EntryPoints, []string{"websecure"}) ||
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
			s.LoadBalancer.Servers[0].URL != "http://"+entranceAlias+":"+strconv.Itoa(int(e.WebPort)) {
			return nil, fmt.Errorf("HTTP service %q does not reach its own port", name)
		}
	}
	for name, rt := range r.HTTP.Routers {
		if !own(name) || !entranceWebRule.MatchString(rt.Rule) || !slices.Equal(rt.EntryPoints, []string{"web"}) || rt.Priority < 0 {
			return nil, fmt.Errorf("HTTP router %q is not the certificate checks of its hostnames on web", name)
		}
		if _, ok := r.HTTP.Services[rt.Service]; !ok {
			return nil, fmt.Errorf("HTTP router %q names a service it does not define", name)
		}
	}
	return &r, nil
}

// hosts is every hostname the routes pass on by SNI.
func (r *entranceRoutes) hosts() []string {
	var out []string
	for _, rt := range r.TCP.Routers {
		for _, m := range entranceRuleArg.FindAllStringSubmatch(rt.Rule, -1) {
			if !slices.Contains(out, m[1]) {
				out = append(out, m[1])
			}
		}
	}
	return out
}

// renderEntranceRoutes is the proxy's entrances file: every App's checked routes in one document, written from
// what was checked rather than from what was sent. Names are the App's own, so nothing collides.
func renderEntranceRoutes(entrances []entrance) []byte {
	all := entranceRoutes{TCP: &entranceTCP{Routers: map[string]entranceTCPRouter{}, Services: map[string]entranceTCPService{}},
		HTTP: &entranceHTTP{Routers: map[string]entranceHTTPRouter{}, Services: map[string]entranceHTTPService{}}}
	for _, e := range entrances {
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
	if len(all.TCP.Routers) == 0 {
		return []byte("{}\n")
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
