// Package geography contains display groups and the upstream country policy.
// These are not claims about gateway topology or interchangeable proxy exits.
package geography

import "strings"

const UnknownArea = "unknown"

type Area struct {
	Key, Label, Countries string
}

// Middle East includes Central Asia as an operational display bucket. Every
// country remains an independent exit and attempt budget underneath the UI.
var Areas = []Area{
	{"north-america", "北美", "AG AI AW BB BL BM BQ BS BZ CA CR CU CW DM DO GD GL GP GT HN HT JM KN KY LC MF MQ MS MX NI PA PM PR SV SX TC TT US VC VG VI"},
	{"south-america", "南美", "AR BO BR CL CO EC FK GF GY PE PY SR UY VE"},
	{"europe", "欧洲", "AD AL AM AT AX AZ BA BE BG BY CH CY CZ DE DK EE ES FI FO FR GB GE GG GI GR HR HU IE IM IS IT JE LI LT LU LV MC MD ME MK MT NL NO PL PT RO RS RU SE SI SJ SK SM UA VA"},
	{"east-asia", "东亚", "CN HK JP KP KR MN MO TW"},
	{"southeast-asia", "东南亚", "BN ID KH LA MM MY PH SG TH TL VN"},
	{"south-asia", "南亚", "AF BD BT IN LK MV NP PK"},
	{"middle-east", "中东", "AE BH IL IQ IR JO KG KW KZ LB OM PS QA SA SY TJ TM TR UZ YE"},
	{"africa", "非洲", "AO BF BI BJ BW CD CF CG CI CM CV DJ DZ EG EH ER ET GA GH GM GN GQ GW KE KM LR LS LY MA MG ML MR MU MW MZ NA NE NG RE RW SC SD SH SL SN SO SS ST SZ TD TF TG TN TZ UG YT ZA ZM ZW"},
	{"oceania", "大洋洲", "AS AU CC CK CX FJ FM GU KI MH MP NC NF NR NU NZ PF PG PN PW SB TK TO TV UM VU WF WS"},
}

// Verified 2026-10-04 against the official upstream allowlist (208 ISO codes):
// https://developers.openai.com/api/docs/supported-countries
// Provider inventory alone never enables a country absent from this policy.
const supportedCodes = `
AD AE AF AG AL AM AO AR AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BR BS BT BW BZ
CA CD CF CG CH CI CL CM CO CR CV CY CZ DE DJ DK DM DO DZ EC EE EG ER ES ET FI FJ FM FO FR
GA GB GD GE GF GH GL GM GN GP GQ GR GT GW GY HN HR HT HU ID IE IL IN IQ IS IT JM JO JP
KE KG KH KI KM KN KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML
MM MN MQ MR MT MU MV MW MX MY MZ NA NC NE NG NI NL NO NP NR NZ OM PA PE PF PG PH PK PL PM
PS PT PW PY QA RE RO RS RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SZ TD TF
TG TH TJ TL TM TN TO TR TT TV TW TZ UA UG US UY UZ VA VC VN VU WF WS YE YT ZA ZM ZW
`

var supported = func() map[string]bool {
	out := map[string]bool{}
	for _, code := range strings.Fields(supportedCodes) {
		out[code] = true
	}
	return out
}()

var countryAreas = func() map[string]Area {
	out := map[string]Area{}
	for _, area := range Areas {
		for _, country := range strings.Fields(area.Countries) {
			out[country] = area
		}
	}
	return out
}()

func CountryKey(country string) string {
	country = strings.ToUpper(strings.TrimSpace(country))
	if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' || country[1] < 'A' || country[1] > 'Z' {
		return ""
	}
	return country
}

func Supported(country string) bool { return supported[CountryKey(country)] }

func AreaOf(country string) Area {
	if area, ok := countryAreas[CountryKey(country)]; ok {
		return area
	}
	return Area{Key: UnknownArea, Label: "未归类"}
}
