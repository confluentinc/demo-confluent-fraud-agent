package io.confluent.frauddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Send a fraud alert notification to the user.
 *
 * <p>Mock tool: returns a confirmation string with no real side effect, mirroring the
 * original {@code agent/tools.py:notify_user} stub. Registered as a Streaming Agent
 * tool and invoked by the agent during its reasoning loop.
 */
public class NotifyUser extends ScalarFunction {
    public String eval(String userId, String message) {
        return "Notification sent to " + userId + ": " + message;
    }
}
