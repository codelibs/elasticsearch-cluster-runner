package org.codelibs.elasticsearch.runner;

public class ClusterRunnerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ClusterRunnerException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ClusterRunnerException(final String message) {
        super(message);
    }

}
