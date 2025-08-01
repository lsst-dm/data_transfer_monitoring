logging {
  level = "info"
  format = "logfmt"
}

// Discover all containers
discovery.docker "docker" {
  host = "unix:///var/run/docker.sock"
  refresh_interval = "10s"
}

// Collect logs from all discovered containers
loki.source.docker "containers" {
  host    = "unix:///var/run/docker.sock"
  targets = discovery.docker.docker.targets
  forward_to = [loki.process.add_day_obs.receiver]
}

// Extract day_obs from log line as a label
loki.process "add_day_obs" {
  stage.regex {
    expression = "day_obs=(?P<day_obs>\\d{4}-\\d{2}-\\d{2})"
  }
  stage.labels {
    values = {
      day_obs = "day_obs",
    }
  }
  forward_to = [loki.write.grafana_loki.receiver]
}

// Send processed logs to Loki
loki.write "grafana_loki" {
  endpoint {
    url = "http://loki:3100/loki/api/v1/push"
  }
}
