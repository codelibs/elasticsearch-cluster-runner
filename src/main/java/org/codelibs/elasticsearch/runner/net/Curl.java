package org.codelibs.elasticsearch.runner.net;

import java.net.HttpURLConnection;

import org.elasticsearch.node.Node;

public class Curl {

    protected Curl() {
        // nothing
    }

    public static CurlRequest get(Node node, String path) {
        return new CurlRequest(Method.GET, node, path);
    }

    public static CurlRequest post(Node node, String path) {
        return new CurlRequest(Method.POST, node, path);
    }

    public static CurlRequest put(Node node, String path) {
        return new CurlRequest(Method.PUT, node, path);
    }

    public static CurlRequest delete(Node node, String path) {
        return new CurlRequest(Method.DELETE, node, path);
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

    public enum Method {
        GET, POST, PUT, DELETE;
    }

    public interface ResponseListener {
        public void onResponse(HttpURLConnection con);
    }

}
