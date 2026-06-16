package io.confluent.frauddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Temporarily freeze a user account due to suspected fraud.
 *
 * <p>Mock tool: returns a confirmation string with no real side effect, mirroring the
 * original {@code agent/tools.py:freeze_account} stub. Registered as a Streaming Agent
 * tool and invoked by the agent during its reasoning loop.
 */
public class FreezeAccount extends ScalarFunction {
    public String eval(String userId, String reason) {
        return "Account " + userId + " frozen: " + reason;
    }
}
