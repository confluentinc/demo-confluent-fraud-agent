# Fraud Detection Agent Tools (Flink UDFs)

Three Java `ScalarFunction` UDFs exposed to the Streaming Agent as function-based tools:

| Function | Signature | Mirrors |
|----------|-----------|---------|
| `flag_transaction` | `(transaction_id, reason) → String` | `agent/tools.py` (original) |
| `freeze_account` | `(user_id, reason) → String` | `agent/tools.py` (original) |
| `notify_user` | `(user_id, message) → String` | `agent/tools.py` (original) |

These are **mock** actions — each returns a confirmation string and has no real side
effect (same as the original Python stubs). The difference is that on Confluent Cloud
they are genuinely invoked by the agent during tool-calling.

## You probably don't need to build this

`target/fraud-tools.jar` is committed to the repo and uploaded to Confluent Cloud by
Terraform (`confluent_flink_artifact`). Running the demo requires **only Terraform** — no
JDK or Maven.

## Rebuilding the JAR (maintainers only)

Only needed when the Java sources change. Builds inside a Docker container, so no host
Java toolchain is required:

```bash
./build.sh
```

This produces `target/fraud-tools.jar`. Commit it. Confluent Cloud for Apache Flink
supports Java 11–17 UDFs; the build targets Java 11.
