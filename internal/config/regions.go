package config

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"gwpool/internal/geography"
	"gwpool/internal/proxycatalog"
	"gwpool/internal/types"
)

const (
	MaxExplorationRegions = 256
	MaxCoverageTargets    = 512
)

// ExplorationRegion 的 Key 和出口一经建立就不可改；名称/启停可改。
// 展示分组另存于 CoverageGroup，不以改名复用旧种子、限额或历史。
type ExplorationRegion struct {
	Key     string `json:"key"`
	Name    string `json:"name"`
	Country string `json:"country"`
	State   string `json:"state,omitempty"`
	City    string `json:"city,omitempty"`
	Enabled bool   `json:"enabled"`
}

func (r ExplorationRegion) Exit() types.Exit {
	return types.Exit{Region: r.Country, State: r.State, City: r.City}
}

type CoverageGroup struct {
	Key      string   `json:"key"`
	Name     string   `json:"name"`
	ExitKeys []string `json:"exit_keys"`
	Targets  []string `json:"targets"`
	// Complete 只是管理员确认「目标清单完整」，不是发现结果。
	Complete bool `json:"complete"`
}

var defaultRegionNames = map[string]string{
	"us-east": "美国默认出口", "us-west": "美国西雅图出口", "south-america": "南美",
	"west-europe": "西欧", "europe": "欧洲", "east-asia": "东亚",
	"oceania": "大洋洲", "south-asia": "南亚", "middle-east": "中东",
}

func DefaultRegions() []ExplorationRegion {
	out := make([]ExplorationRegion, 0, len(types.Regions))
	for _, r := range types.Regions {
		out = append(out, defaultRegion(r))
	}
	return out
}

func defaultRegion(r types.Region) ExplorationRegion {
	return ExplorationRegion{Key: r.Key, Name: defaultRegionNames[r.Key],
		Country: r.Exit.Region, City: r.Exit.City, Enabled: true}
}

func (c Config) RegionDefinitions() []ExplorationRegion {
	if c.Regions == nil {
		return DefaultRegions()
	}
	return c.Regions
}

func (c Config) RegionByKey(key string) (ExplorationRegion, bool) {
	if c.Regions == nil {
		for _, r := range types.Regions {
			if r.Key == key {
				return defaultRegion(r), true
			}
		}
		return ExplorationRegion{}, false
	}
	for _, r := range c.Regions {
		if r.Key == key {
			return r, true
		}
	}
	return ExplorationRegion{}, false
}

func (c Config) RegionKeyOf(exit types.Exit) string {
	if c.Regions == nil {
		for _, r := range types.Regions {
			if defaultRegion(r).Exit() == exit {
				return r.Key
			}
		}
		return ""
	}
	for _, r := range c.Regions {
		if r.Exit() == exit {
			return r.Key // 停用仅阻止新探索，历史/配额反查仍保留原键。
		}
	}
	return ""
}

func (c Config) RegionLabel(key string) string {
	if r, ok := c.RegionByKey(key); ok {
		return r.Name
	}
	return key
}

func (c Config) CandidateRegions(want types.Exit) []types.Region {
	var out []types.Region
	for _, r := range c.RegionDefinitions() {
		if r.Enabled && geography.Supported(r.Country) && (want == (types.Exit{}) || want == r.Exit()) {
			out = append(out, types.Region{Key: r.Key, Exit: r.Exit()})
		}
	}
	return out
}

func (c Config) Groups() []CoverageGroup {
	if c.CoverageGroups != nil {
		return c.CoverageGroups
	}
	out := make([]CoverageGroup, 0, len(c.RegionDefinitions()))
	for _, r := range c.RegionDefinitions() {
		out = append(out, CoverageGroup{Key: r.Key, Name: r.Name, ExitKeys: []string{r.Key}})
	}
	return out
}

var (
	regionKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)
	countryPattern   = regexp.MustCompile(`^[A-Z]{2}$`)
	gatewayPattern   = regexp.MustCompile(`^unified-[1-9][0-9]{0,5}$`)
)

func validRegionName(name string) bool {
	return name != "" && utf8.ValidString(name) && strings.TrimSpace(name) == name && len(name) <= 120 &&
		strings.IndexFunc(name, unicode.IsControl) < 0
}

func (c Config) validateExploration() error {
	defs := c.RegionDefinitions()
	if len(defs) == 0 || len(defs) > MaxExplorationRegions {
		return fmt.Errorf("实际出口数必须为 1–%d；停用请保留定义", MaxExplorationRegions)
	}
	keys, exits := map[string]bool{}, map[types.Exit]bool{}
	for _, r := range defs {
		if !regionKeyPattern.MatchString(r.Key) || r.Key == "unknown" || r.Key == "all" ||
			keys[r.Key] || !validRegionName(r.Name) || !countryPattern.MatchString(r.Country) ||
			(r.State != "" && !types.ValidLocationName(r.State)) ||
			(r.City != "" && !types.ValidLocationName(r.City)) || exits[r.Exit()] {
			return fmt.Errorf("出口 %q 无效或重复：ID须唯一，国家用大写ISO两字母，州/城市使用供应商目录值，禁止注入代理选项", r.Key)
		}
		keys[r.Key], exits[r.Exit()] = true, true
	}
	// 内置键必须始终存在且出口固定；停用也不能借改名重置配额。
	for _, old := range DefaultRegions() {
		if next, ok := c.RegionByKey(old.Key); !ok || next.Exit() != old.Exit() {
			return fmt.Errorf("内置出口 %s 不可删除或修改国家/城市；请新增独立ID", old.Key)
		}
	}
	groups := c.Groups()
	if len(groups) > MaxExplorationRegions {
		return fmt.Errorf("覆盖分组最多 %d 个", MaxExplorationRegions)
	}
	groupKeys := map[string]bool{}
	for _, group := range groups {
		if !regionKeyPattern.MatchString(group.Key) || group.Key == "unknown" || groupKeys[group.Key] ||
			!validRegionName(group.Name) || len(group.Targets) > MaxCoverageTargets ||
			len(group.ExitKeys) == 0 || len(group.ExitKeys) > MaxExplorationRegions ||
			(group.Complete && len(group.Targets) == 0) {
			return fmt.Errorf("覆盖分组 %q 无效：必须关联实际出口；完整清单不可为空", group.Key)
		}
		groupKeys[group.Key] = true
		seen := map[string]bool{}
		for _, key := range group.ExitKeys {
			if !keys[key] || seen[key] {
				return fmt.Errorf("覆盖分组 %s 引用了未知或重复出口", group.Key)
			}
			seen[key] = true
		}
		seen = map[string]bool{}
		for _, name := range group.Targets {
			if !gatewayPattern.MatchString(name) || seen[name] {
				return fmt.Errorf("覆盖分组 %s 网关名无效或重复", group.Key)
			}
			seen[name] = true
		}
	}
	return nil
}

func ValidateRegionTransition(old, next Config) error {
	if err := next.validateExploration(); err != nil {
		return err
	}
	for _, before := range old.RegionDefinitions() {
		after, ok := next.RegionByKey(before.Key)
		if !ok || after.Exit() != before.Exit() {
			return fmt.Errorf("已有出口 %s 不可删除或修改国家/城市；请停用并新增独立ID", before.Key)
		}
	}
	return nil
}

// GlobalCountryRegions adds known provider-directory countries without duplicating
// an existing country-only exit or changing its enabled state. It changes neither
// the account's original total mint budget nor concurrent request limits.
func GlobalCountryRegions(current Config) (Config, error) {
	next := current
	next.Regions = append([]ExplorationRegion(nil), current.RegionDefinitions()...)
	next.CoverageGroups = append([]CoverageGroup{}, current.Groups()...)
	countries, keys := map[string]bool{}, map[string]bool{}
	for _, region := range next.Regions {
		keys[region.Key] = true
		if region.State == "" && region.City == "" {
			countries[region.Country] = true
		}
	}
	for _, country := range proxycatalog.GlobalCountries() {
		if countries[country.Code] || !geography.Supported(country.Code) {
			continue
		}
		key := "country-" + strings.ToLower(country.Code)
		if keys[key] {
			return Config{}, fmt.Errorf("全球候选ID %s 已被其它定义使用；未保存任何改动", key)
		}
		name := strings.TrimSpace(country.Label) + " (" + country.Code + ")"
		next.Regions = append(next.Regions, ExplorationRegion{
			Key: key, Name: name, Country: country.Code, Enabled: true,
		})
		// Leave unassigned coverage unknown, instead of manufacturing 249 empty
		// target groups (and a quadratic group×exit editor).
		countries[country.Code], keys[key] = true, true
	}
	if err := ValidateRegionTransition(current, next); err != nil {
		return Config{}, err
	}
	return next, nil
}
