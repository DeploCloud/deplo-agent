package pkgupdate

import (
	"reflect"
	"testing"
)

func TestCommands(t *testing.T) {
	for name, tc := range map[string]struct {
		spec Spec
		want [][]string
	}{
		"npm direct keeps its range": {
			Spec{Manager: "npm", Package: "minimist", Version: "1.2.6", Section: "dependencies", Prefix: "^"},
			[][]string{{"npm", "pkg", "set", "dependencies[minimist]=^1.2.6"}, {"npm", "install", "--package-lock-only", "--ignore-scripts"}},
		},
		"npm override, dotted name": {
			Spec{Manager: "npm", Package: "lodash.merge", Version: "4.6.2"},
			[][]string{{"npm", "pkg", "set", "overrides[lodash.merge]=4.6.2"}, {"npm", "install", "--package-lock-only", "--ignore-scripts"}},
		},
		"pnpm override, installed first": {
			Spec{Manager: "pnpm", Package: "@babel/core", Version: "7.26.0", Tool: "pnpm@9"},
			[][]string{
				{"npm", "install", "--global", "--prefix", "/tmp/pm", "--ignore-scripts", "pnpm@9"},
				{"npm", "pkg", "set", "pnpm.overrides[@babel/core]=7.26.0"},
				{"node", "/tmp/pm/bin/pnpm", "install", "--lockfile-only", "--ignore-scripts"},
			},
		},
		"yarn classic dev dependency, exact": {
			Spec{Manager: "yarn", Package: "axios", Version: "1.6.1", Section: "devDependencies"},
			[][]string{{"npm", "pkg", "set", "devDependencies[axios]=1.6.1"}, {"yarn", "install", "--ignore-scripts", "--non-interactive"}},
		},
		"npm direct and nested follows the direct range": {
			Spec{Manager: "npm", Package: "minimist", Version: "1.2.8", Section: "dependencies", Prefix: "^", Nested: true},
			[][]string{
				{"npm", "pkg", "set", "dependencies[minimist]=^1.2.8"},
				{"npm", "pkg", "set", "overrides[minimist]=$minimist"},
				{"npm", "install", "--package-lock-only", "--ignore-scripts"},
			},
		},
		"yarn berry override": {
			Spec{Manager: "yarn-berry", Package: "axios", Version: "1.6.1"},
			[][]string{{"npm", "pkg", "set", "resolutions[axios]=1.6.1"}, {"node", "/tmp/pm/bin/yarn", "install", "--mode=update-lockfile"}},
		},
	} {
		if got := Commands(tc.spec); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got %v\nwant %v", name, got, tc.want)
		}
	}
	if Commands(Spec{Manager: "bun"}) != nil {
		t.Error("an unknown manager has no steps")
	}
}
