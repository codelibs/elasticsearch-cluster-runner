/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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
package org.codelibs.elasticsearch.runner.net;

import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import org.codelibs.curl.Curl.Method;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.Node;

public class EcrCurl {

    protected EcrCurl() {
        // nothing
    }

    public static CurlRequest get(final Node node, final String path) {
        return new CurlRequest(Method.GET, getUrl(node, path));
    }

    public static CurlRequest post(final Node node, final String path) {
        return new CurlRequest(Method.POST, getUrl(node, path));
    }

    public static CurlRequest put(final Node node, final String path) {
        return new CurlRequest(Method.PUT, getUrl(node, path));
    }

    public static CurlRequest delete(final Node node, final String path) {
        return new CurlRequest(Method.DELETE, getUrl(node, path));
    }

    protected static String getUrl(final Node node, final String path) {
        final StringBuilder urlBuf = new StringBuilder(200);
        urlBuf.append("http://localhost:").append(node.settings().get("http.port"));
        if (path.startsWith("/")) {
            urlBuf.append(path);
        } else {
            urlBuf.append('/').append(path);
        }
        return urlBuf.toString();
    }

    public static CurlRequest get(final String url) {
        return new CurlRequest(Method.GET, url);
    }

    public static CurlRequest post(final String url) {
        return new CurlRequest(Method.POST, url);
    }

    public static CurlRequest put(final String url) {
        return new CurlRequest(Method.PUT, url);
    }

    public static CurlRequest delete(final String url) {
        return new CurlRequest(Method.DELETE, url);
    }

    public static Function<CurlResponse, Map<String, Object>> jsonParser() {
        return PARSER;
    }

    protected static final Function<CurlResponse, Map<String, Object>> PARSER = response -> {
        try (InputStream is = response.getContentAsStream()) {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, is).map();
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    };

}
