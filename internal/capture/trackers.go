package capture

import "strings"

// trackers are ad and analytics hosts the screenshot browser never loads: they cost most of a news
// page's CPU, draw nothing worth a cover, and would count the browser as a visit.
var trackers = []string{
	"doubleclick.net", "googlesyndication.com", "googleadservices.com", "googletagservices.com",
	"google-analytics.com", "googletagmanager.com", "analytics.google.com", "adservice.google.com",
	"amazon-adsystem.com", "adnxs.com", "criteo.com", "criteo.net", "taboola.com", "outbrain.com",
	"rubiconproject.com", "pubmatic.com", "openx.net", "casalemedia.com", "adsrvr.org",
	"smartadserver.com", "teads.tv", "yieldlab.net", "33across.com", "sharethrough.com",
	"3lift.com", "indexww.com", "media.net", "moatads.com", "adform.net", "quantserve.com",
	"quantcount.com", "scorecardresearch.com", "chartbeat.com", "chartbeat.net", "permutive.com",
	"bluekai.com", "krxd.net", "demdex.net", "omtrdc.net", "2mdn.net", "serving-sys.com",
	"flashtalking.com", "hotjar.com", "hotjar.io", "clarity.ms", "segment.io", "segment.com",
	"mixpanel.com", "amplitude.com", "heapanalytics.com", "fullstory.com", "nr-data.net",
	"plausible.io", "usefathom.com", "statcounter.com", "mc.yandex.ru", "bat.bing.com",
	"connect.facebook.net", "analytics.tiktok.com", "snap.licdn.com", "px.ads.linkedin.com",
	"ads-twitter.com", "ads.pubmatic.com", "securepubads.g.doubleclick.net",
}

func isTracker(host string) bool {
	for _, t := range trackers {
		if host == t || strings.HasSuffix(host, "."+t) {
			return true
		}
	}
	return false
}
