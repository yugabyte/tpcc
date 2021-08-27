package com.oltpbenchmark.util;

/**
 * Thrown while parsing any for of user input to indicate that
 * the given configuration is invalid.
 */
public class InvalidUserConfiguration extends RuntimeException {
    private static final long serialVersionUID = -1L;

    /**
     * Default Constructor
     */
    public InvalidUserConfiguration(String msg, Throwable ex) {
        super(msg, ex);
    }

    /**
     * Constructs a new InvalidUserConfiguration
     * with the specified detail message.
     */
    public InvalidUserConfiguration(String msg) {
        this(msg, null);
    }
}
