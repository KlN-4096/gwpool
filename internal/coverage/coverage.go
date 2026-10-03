// Package coverage reports candidate coverage from observed routing, never from IP geography.
package coverage

import (
	"sort"
	"strings"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// 2026-10-01: G:/temp/Claude_temp/azure_gw/gw_dns.txt, unified-1..220 DNS enumeration.
// 201 resolvable names, not proof of mintability or an exhaustive upstream inventory.
const dnsCandidateNumbers = `
1 2 3 4 5 6 7 8 9 11 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 29 30 31 32
35 36 37 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62
63 64 65 66 67 68 69 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89
91 92 93 94 95 96 97 98 99 100 101 102 103 107 108 109 110 111 112 113 114 115 116
117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137
138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158
159 160 161 162 163 164 165 166 167 168 169 173 174 175 176 177 178 179 180 181 182
183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203
204 205 206 207 208 209 210 211 212 213 214 215`

type Candidate struct {
	Name       string
	InDNS      bool
	Found      bool
	Ready      bool
	ExitKeys   []string
	GroupNames []string
}

type Group struct {
	config.CoverageGroup
	Found, Ready int
	Observed     []string // 在关联出口发现过的全集，包含目标清单外的落点。
	Missing      []string
	Done         bool
}

type Snapshot struct {
	Candidates []Candidate
	Groups     []Group
	Unassigned []string
	DNSCount   int
	Found      int
	Ready      int
}

func Build(cfg config.Config, gateways map[string]types.Gateway, pairs map[string]types.Pair, now time.Time) Snapshot {
	out := Snapshot{}
	directory := map[string]bool{}
	for _, n := range strings.Fields(dnsCandidateNumbers) {
		directory["unified-"+n] = true
	}
	out.DNSCount = len(directory)
	names, exits := map[string]bool{}, map[string]map[string]bool{}
	for name := range directory {
		names[name] = true
	}
	for name, gateway := range gateways {
		names[name] = true
		exits[name] = map[string]bool{}
		for _, exit := range gateway.ReachableExits {
			if key := cfg.RegionKeyOf(exit); key != "" {
				exits[name][key] = true
			}
		}
	}
	ready := func(name string) bool {
		pair, ok := pairs[name]
		return ok && pair.Cookie != "" && pair.ExpiresAt.Sub(now) >= types.DeliverFloor
	}
	assigned := map[string][]string{}
	for _, definition := range cfg.Groups() {
		group := Group{CoverageGroup: definition}
		matches := func(name string) bool {
			for _, key := range definition.ExitKeys {
				if exits[name][key] {
					return true
				}
			}
			return false
		}
		for name := range gateways {
			if matches(name) {
				group.Observed = append(group.Observed, name)
			}
		}
		sort.Strings(group.Observed)
		for _, name := range definition.Targets {
			names[name] = true
			assigned[name] = append(assigned[name], definition.Name)
			if !matches(name) {
				group.Missing = append(group.Missing, name)
				continue
			}
			group.Found++
			if ready(name) {
				group.Ready++
			}
		}
		group.Done = definition.Complete && len(definition.Targets) > 0 && len(group.Missing) == 0
		out.Groups = append(out.Groups, group)
	}
	for name := range names {
		_, found := gateways[name]
		row := Candidate{Name: name, InDNS: directory[name], Found: found, Ready: found && ready(name), GroupNames: assigned[name]}
		for key := range exits[name] {
			row.ExitKeys = append(row.ExitKeys, key)
		}
		sort.Strings(row.ExitKeys)
		if found {
			out.Found++
		}
		if row.Ready {
			out.Ready++
		}
		if len(row.GroupNames) == 0 {
			out.Unassigned = append(out.Unassigned, name)
		}
		out.Candidates = append(out.Candidates, row)
	}
	sort.Strings(out.Unassigned)
	sort.Slice(out.Candidates, func(i, j int) bool { return out.Candidates[i].Name < out.Candidates[j].Name })
	return out
}

// IncompleteRegions 只用历史覆盖判探索优先级，不能把票过期误当成“没发现”。
// 同一出口关联多个分组时，有一个未完成就仍需探索；未关联分组的出口也算未知。
func IncompleteRegions(cfg config.Config, gateways map[string]types.Gateway) map[string]bool {
	out, grouped := map[string]bool{}, map[string]bool{}
	for _, group := range Build(cfg, gateways, nil, time.Time{}).Groups {
		for _, key := range group.ExitKeys {
			grouped[key] = true
			out[key] = out[key] || !group.Done
		}
	}
	for _, r := range cfg.RegionDefinitions() {
		if !grouped[r.Key] {
			out[r.Key] = true
		}
		if !r.Enabled {
			delete(out, r.Key)
		}
	}
	return out
}
