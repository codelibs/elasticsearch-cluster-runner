package org.codelibs.elasticsearch.runner;

import org.elasticsearch.action.ActionResponse;

public class ClusterRunnerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private transient ActionResponse response;

    public ClusterRunnerException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ClusterRunnerException(final String message) {
        super(message);
    }

    public ClusterRunnerException(final String message,
            final ActionResponse response) {
        this(message);
        this.response = response;
    }

    @SuppressWarnings("unchecked")
    public <T extends ActionResponse> T getActionResponse() {
        return (T) response;
    }
}
