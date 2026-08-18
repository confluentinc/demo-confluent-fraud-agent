# 🎬 Demo Path — One-Command Deploy

The fastest way to see the system: Terraform provisions **everything** — infrastructure
*and* the full Flink AI pipeline — and you just run the producer and dashboard. For the
hands-on version where you build the AI pipeline yourself, see [`WORKSHOP.md`](WORKSHOP.md).

> Complete the shared [Prerequisites](README.md#prerequisites) first (Confluent Cloud +
> AWS Bedrock credentials, Claude model access in `us-east-1`, and the required tools).

## 1. Clone the repository

```bash
git clone https://github.com/confluentinc/demo-confluent-fraud-agent.git && cd demo-confluent-fraud-agent
```

## 2. Provide your four credentials

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars — Confluent Cloud key/secret + AWS Bedrock IAM key/secret
```

These four values are the **only** inputs. Region (`us-east-1`), the Claude model, resource
names, and sizing are all preset.

## 3. Deploy everything

```bash
terraform init && terraform apply
```

This provisions the environment, Kafka cluster, Schema Registry, Flink compute pool, the
Bedrock connection, the model, the UDF tools (JAR upload + functions + tools), the agent,
and the detection query — and writes a ready-to-use `.env` for the local apps.

## 4. Install the Python dependencies

<details open>
<summary>macOS / Linux</summary>

```bash
cd .. && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
```
</details>

<details>
<summary>Windows (PowerShell)</summary>

```powershell
cd .. ; python -m venv venv ; venv\Scripts\Activate.ps1 ; pip install -r requirements.txt
```
</details>

## 5. Start the producer

```bash
python producer/generate_events.py
```

## 6. Start the dashboard

In a **second** terminal (activate the venv first), launch the UI at http://localhost:8501:

```bash
streamlit run dashboard/app.py
```

Watch fraud alerts appear in real time — the injected scenarios surface as high-risk alerts
(scores ~75–95) with `freeze_account` / `flag_transaction` actions and the flagged
transaction ids.

> [!NOTE]
> The dashboard reads from `latest`, so keep the producer running and allow ~1 minute for
> the first window-firing batch of alerts. Inspect the running statements and tables in the
> [Flink workspace](https://confluent.cloud/go/flink).

## Next: the guided walkthrough

Deployed and running? Follow the **[guided walkthrough → `WALKTHROUGH.md`](WALKTHROUGH.md)** —
a ~15-minute, screenshot-by-screenshot tour through **Stream Lineage** (source topics → the
Flink Streaming Agent with union, tools & AI agent → the alerts topic) and finishing on the
live dashboard.

## Cleanup

```bash
cd terraform && terraform destroy
```
