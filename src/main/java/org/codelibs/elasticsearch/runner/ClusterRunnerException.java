/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.codelibs.elasticsearch.runner;

import org.elasticsearch.action.ActionResponse;

public class ClusterRunnerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient ActionResponse response;

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
