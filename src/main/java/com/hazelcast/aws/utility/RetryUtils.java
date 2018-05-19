package com.hazelcast.aws.utility;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Callable;

/**
 * Static utility class to retry operations related to connecting to AWS Services.
 */
public class RetryUtils {
    private static final ILogger logger = Logger.getLogger(RetryUtils.class);

    /**
     * Calls {@code callable.call()} until it does not throw an exception (but no more than {@code retries} times).
     * <p>
     * Note that {@code callable} should be an idempotent operation which is a call to the AWS Service.
     * <p>
     * If {@code callable} throws an unchecked exception, it is wrapped into {@link HazelcastException}.
     */
    public static <T> T retry(Callable<T> callable, int retries) {
        int retryCount = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                retryCount++;
                if (retryCount > retries) {
                    throw ExceptionUtil.rethrow(e);
                }
                logger.warning(String.format("Couldn't connect to the AWS service, %s retrying...", retryCount));
            }
        }
    }
}
