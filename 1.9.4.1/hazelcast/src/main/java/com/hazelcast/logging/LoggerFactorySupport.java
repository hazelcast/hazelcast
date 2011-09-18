package com.hazelcast.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class LoggerFactorySupport implements LoggerFactory {
	
	final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);
	
	public final ILogger getLogger(String name) {
        ILogger logger = mapLoggers.get(name);
        if (logger == null) {
            ILogger newLogger = createLogger(name);
            logger = mapLoggers.putIfAbsent(name, newLogger);
            if (logger == null) {
                logger = newLogger;
            }
        }
        return logger;
    }

	protected abstract ILogger createLogger(String name);
}
