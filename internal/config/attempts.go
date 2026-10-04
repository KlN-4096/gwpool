package config

func (c Config) CountryAttemptLimit() int {
	if c.UpstreamPerRegionHour > 0 {
		return c.UpstreamPerRegionHour
	}
	return Defaults().UpstreamPerRegionHour
}

func (c Config) AccountAttemptLimit() int {
	if c.UpstreamPerAccountHour > 0 {
		return c.UpstreamPerAccountHour
	}
	return Defaults().UpstreamPerAccountHour
}
