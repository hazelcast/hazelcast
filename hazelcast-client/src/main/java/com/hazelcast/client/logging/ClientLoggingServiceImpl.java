package com.hazelcast.client.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.logging.LoggingService;


public class ClientLoggingServiceImpl implements LoggingService {
	private volatile Level minLevel = Level.OFF;
	
    private final CopyOnWriteArrayList<LogListenerRegistration> lsListeners = new CopyOnWriteArrayList<LogListenerRegistration>();
    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);
    private final LoggerFactory loggerFactory;
    
    public ClientLoggingServiceImpl(String loggingType) {
    	this.loggerFactory = Logger.newLoggerFactory(loggingType);
    }
    public void addLogListener(Level level, LogListener logListener) {
        lsListeners.add(new LogListenerRegistration(level, logListener));
        if (level.intValue() < minLevel.intValue()) {
            minLevel = level;
        }
    }

    public void removeLogListener(LogListener logListener) {
        lsListeners.remove(new LogListenerRegistration(Level.ALL, logListener));
    }

	public ILogger getLogger(String name) {
        ILogger logger = mapLoggers.get(name);
        if (logger == null) {
            ILogger newLogger = loggerFactory.getLogger(name);
            logger = mapLoggers.putIfAbsent(name, newLogger);
            if (logger == null) {
                logger = newLogger;
            }
        }
        return logger;
	}
    class LogListenerRegistration {
        Level level;
        LogListener logListener;

        LogListenerRegistration(Level level, LogListener logListener) {
            this.level = level;
            this.logListener = logListener;
        }

        public Level getLevel() {
            return level;
        }

        public LogListener getLogListener() {
            return logListener;
        }

        /**
         * True if LogListeners are equal.
         */
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LogListenerRegistration other = (LogListenerRegistration) obj;
            if (logListener == null) {
                if (other.logListener != null)
                    return false;
            } else if (!logListener.equals(other.logListener))
                return false;
            return true;
        }
    }
}
