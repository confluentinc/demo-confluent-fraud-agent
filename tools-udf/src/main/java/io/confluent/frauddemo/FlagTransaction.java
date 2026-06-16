package io.confluent.frauddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flag a transaction as potentially fraudulent for manual review.
 *
 * <p>Mock tool: returns a confirmation string with no real side effect, mirroring the
 * original {@code agent/tools.py:flag_transaction} stub. Registered as a Streaming Agent
 * tool and invoked by the agent during its reasoning loop.
 */
public class FlagTransaction extends ScalarFunction {
    public String eval(String transactionId, String reason) {
        return "Transaction " + transactionId + " flagged for review: " + reason;
    }
}
