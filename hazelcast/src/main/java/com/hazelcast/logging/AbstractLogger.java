package com.hazelcast.logging;

import java.util.logging.Level;


/**
 * Abstract {@link ILogger} implementation that provides implementations for the convenience methods like
 * finest,info,warning and severe.
 */
public abstract class AbstractLogger implements ILogger {

    public void finest(String message) {
        log(Level.FINEST, message);
    }

    public void finest(String message, Throwable thrown) {
        log(Level.FINEST, message, thrown);
    }

    public void finest(Throwable thrown) {
        log(Level.FINEST, thrown.getMessage(), thrown);
    }

    public boolean isFinestEnabled() {
        return isLoggable(Level.FINEST);
    }

    public void info(String message) {
        log(Level.INFO, message);
    }

    public void severe(String message) {
        log(Level.SEVERE, message);
    }

    public void severe(Throwable thrown) {
        log(Level.SEVERE, thrown.getMessage(), thrown);
    }

    public void severe(String message, Throwable thrown) {
        log(Level.SEVERE, message, thrown);
    }

    public void warning(String message) {
        log(Level.WARNING, message);
    }

    public void warning(Throwable thrown) {
        log(Level.WARNING, thrown.getMessage(), thrown);
    }

    public void warning(String message, Throwable thrown) {
        log(Level.WARNING, message, thrown);
    }
}
