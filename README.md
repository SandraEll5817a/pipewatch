# pipewatch

A lightweight CLI tool to monitor and alert on ETL pipeline health using configurable thresholds and webhook notifications.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Define your pipeline checks in a `pipewatch.yaml` config file:

```yaml
pipelines:
  - name: daily_sales_etl
    check: row_count
    threshold:
      min: 1000
      max: 500000
    alert:
      webhook: https://hooks.slack.com/services/your/webhook/url
```

Then run the monitor:

```bash
pipewatch run --config pipewatch.yaml
```

You can also run a one-off check directly from the CLI:

```bash
pipewatch check --pipeline daily_sales_etl --metric row_count --value 850
```

pipewatch will evaluate the value against your configured thresholds and fire a webhook notification if a breach is detected.

---

## Features

- Configurable thresholds (min/max, freshness, null rate)
- Webhook alerts (Slack, PagerDuty, or any HTTP endpoint)
- Lightweight with no heavy dependencies
- Easy integration into existing CI/CD or orchestration workflows

---

## License

This project is licensed under the [MIT License](LICENSE).