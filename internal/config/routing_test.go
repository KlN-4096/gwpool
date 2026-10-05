package config

import "testing"

func TestInventoryRoutingModes(t *testing.T) {
	if Defaults().InventoryRoutingMode() != InventoryRoutingShadow ||
		(Config{}).InventoryRoutingMode() != InventoryRoutingShadow {
		t.Fatal("prediction must default to shadow")
	}
	for _, mode := range []string{InventoryRoutingOff, InventoryRoutingShadow, InventoryRoutingAssist, ""} {
		c, err := Load(writeCfg(t, `{"inventory_routing":"`+mode+`",`+okProxies+`}`))
		if err != nil {
			t.Fatalf("%q: %v", mode, err)
		}
		want := mode
		if want == "" {
			want = InventoryRoutingShadow
		}
		if c.InventoryRoutingMode() != want {
			t.Fatalf("mode=%q want=%q", c.InventoryRoutingMode(), want)
		}
	}
	if _, err := Load(writeCfg(t, `{"inventory_routing":"guess",`+okProxies+`}`)); err == nil {
		t.Fatal("unrecognized routing mode accepted")
	}
}
