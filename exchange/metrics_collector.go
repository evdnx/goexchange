package exchange

import (
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	metrics "github.com/evdnx/gotrademetrics"
)

// httpMetricsCollector adapts gotrademetrics to gohttpcl's MetricsCollector interface.
type httpMetricsCollector struct {
	metrics *metrics.Metrics
	service string
}

func newHTTPMetricsCollector(m *metrics.Metrics, service string) *httpMetricsCollector {
	if m == nil {
		return nil
	}
	service = strings.TrimSpace(service)
	if service == "" {
		service = "http_client"
	}
	return &httpMetricsCollector{
		metrics: m,
		service: service,
	}
}

func (c *httpMetricsCollector) IncRequests(method, target string) {
	if c == nil || c.metrics == nil {
		return
	}
	c.metrics.RecordAPIRequest(c.service, formatEndpointLabel(method, target))
}

func (c *httpMetricsCollector) IncRetries(method, target string, attempt int) {
	if c == nil || c.metrics == nil {
		return
	}
	if attempt == 1 {
		c.metrics.RecordRetryRequest()
	}
	c.metrics.RecordRetryAttempt()
}

func (c *httpMetricsCollector) IncFailures(method, target string, statusCode int) {
	if c == nil || c.metrics == nil {
		return
	}
	c.metrics.RecordAPIError(c.service, failureReason(statusCode))
}

func (c *httpMetricsCollector) ObserveLatency(method, target string, duration time.Duration) {
	if c == nil || c.metrics == nil {
		return
	}
	label := formatEndpointLabel(method, target)
	seconds := duration.Seconds()
	c.metrics.RecordAPILatency(c.service, label, seconds)
	c.metrics.RecordAPIRequestDuration(c.service, label, seconds)
}

func failureReason(statusCode int) metrics.Reason {
	switch {
	case statusCode == http.StatusTooManyRequests:
		return metrics.ReasonRateLimit
	case statusCode >= http.StatusInternalServerError:
		return metrics.ReasonInternal
	case statusCode <= 0:
		return metrics.ReasonNetworkError
	default:
		return metrics.ReasonAPIError
	}
}

func formatEndpointLabel(method, rawTarget string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	endpoint := sanitizeTarget(rawTarget)
	if method == "" {
		return endpoint
	}
	if endpoint == "" {
		return method
	}
	return method + " " + endpoint
}

func sanitizeTarget(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "/") {
		return raw
	}
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	var b strings.Builder
	if u.Host != "" {
		b.WriteString(u.Host)
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	b.WriteString(path)
	if u.RawQuery != "" {
		b.WriteByte('?')
		b.WriteString(u.RawQuery)
	}
	return b.String()
}
