package config

const (
	InventoryRoutingOff    = "off"
	InventoryRoutingShadow = "shadow"
	InventoryRoutingAssist = "assist"
)

// InventoryRoutingMode defaults to shadow even for direct Config construction.
// Prediction never implicitly enables selection assistance.
func (c Config) InventoryRoutingMode() string {
	if c.InventoryRouting == "" {
		return InventoryRoutingShadow
	}
	return c.InventoryRouting
}
