package server

import "testing"

// The plan carries what the generated Dockerfile only declares. An app's own
// variable must not come from here: its value would land on argv and in the log.
func TestPlanVariablesKeepsNixpacksOwnAndDropsTheAppsOwn(t *testing.T) {
	plan := []byte(`{"variables":{"CI":"true","NIXPACKS_SPA_OUTPUT_DIR":"dist",
		"NODE_ENV":"production","PORT":"4173","SECRET_TOKEN":"s3cr3t"}}`)

	vars, err := planVariables(plan, []string{"PORT", "SECRET_TOKEN"})
	if err != nil {
		t.Fatalf("planVariables: %v", err)
	}
	if vars["NIXPACKS_SPA_OUTPUT_DIR"] != "dist" {
		t.Fatalf("the SPA output dir is what Caddy roots on, got %q", vars["NIXPACKS_SPA_OUTPUT_DIR"])
	}
	if vars["NODE_ENV"] != "production" || vars["CI"] != "true" {
		t.Fatalf("nixpacks' own build variables were dropped: %v", vars)
	}
	for _, k := range []string{"PORT", "SECRET_TOKEN"} {
		if _, ok := vars[k]; ok {
			t.Fatalf("%s must stay on the bare --build-arg path", k)
		}
	}

	args := appendBuildArgValues([]string{"build"}, vars)
	want := []string{"build", "--build-arg", "CI=true", "--build-arg", "NIXPACKS_SPA_OUTPUT_DIR=dist",
		"--build-arg", "NODE_ENV=production"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args = %v, want %v", args, want)
		}
	}
}
