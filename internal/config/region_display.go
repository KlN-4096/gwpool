package config

import "strings"

// Geographic display buckets only: never use these to infer gateway reachability
// or to merge quota keys. Unlisted/custom countries remain explicitly ungrouped.
var countryAreas = func() map[string]string {
	out := map[string]string{}
	for _, group := range []struct{ label, codes string }{
		{"亚洲", "AE AF AM AZ BD BH BN BT CC CN CX CY GE HK ID IL IN IO IQ IR JO JP KG KH KP KR KW KZ LA LB LK MM MN MO MV MY NP OM PH PK PS QA SA SG SY TH TJ TL TM TR TW UZ VN YE"},
		{"欧洲", "AD AL AT AX BA BE BG BY CH CZ DE DK EE ES FI FO FR GB GG GI GR HR HU IE IM IS IT JE LI LT LU LV MC MD ME MK MT NL NO PL PT RO RS RU SE SI SJ SK SM UA VA"},
		{"非洲", "AO BF BI BJ BW CD CF CG CI CM CV DJ DZ EG EH ER ET GA GH GM GN GQ GW KE KM LR LS LY MA MG ML MR MU MW MZ NA NE NG RE RW SC SD SH SL SN SO SS ST SZ TD TF TG TN TZ UG YT ZA ZM ZW"},
		{"北美洲", "AG AI AW BB BL BM BQ BS BZ CA CR CU CW DM DO GD GL GP GT HN HT JM KN KY LC MF MQ MS MX NI PA PM PR SV SX TC TT US VC VG VI"},
		{"南美洲", "AR BO BR CL CO EC FK GF GY PE PY SR UY VE"},
		{"大洋洲", "AS AU CK FJ FM GU KI MH MP NC NF NR NU NZ PF PG PN PW SB TK TO TV UM VU WF WS"},
		{"南极及南大洋", "AQ BV GS HM"},
	} {
		for _, code := range strings.Fields(group.codes) {
			out[code] = group.label
		}
	}
	return out
}()

func CountryArea(country string) string {
	if label := countryAreas[strings.ToUpper(country)]; label != "" {
		return label
	}
	return "未归类"
}
