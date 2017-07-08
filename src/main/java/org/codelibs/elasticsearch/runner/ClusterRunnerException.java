package org.codelibs.elasticsearch.runner;

import org.elasticsearch.action.ActionResponse;

public class ClusterRunnerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private transient final ActionResponse response;

    public ClusterRunnerException(final String message, final Throwable cause) {
        super(message, cause);
        this.response = null;
    }

    public ClusterRunnerException(final String message) {
        super(message);
        this.response = null;
    }

    public ClusterRunnerException(final String message,
            final ActionResponse response) {
        super(message);
        this.response = response;
    }

    @SuppressWarnings("unchecked")
    public <T extends ActionResponse> T getActionResponse() {
        return (T) response;
    }
}
