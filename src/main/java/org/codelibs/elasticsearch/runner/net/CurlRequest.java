package org.codelibs.elasticsearch.runner.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.elasticsearch.runner.net.Curl.Method;
import org.codelibs.elasticsearch.runner.net.Curl.ResponseListener;
import org.elasticsearch.node.Node;

public class CurlRequest {
    protected String url;

    protected String encoding = "UTF-8";

    protected Method method;

    protected List<String> paramList;

    protected String body;

    public CurlRequest(final Method method, final String url) {
        this.method = method;
        this.url = url;
    }

    public CurlRequest(Method method, Node node, String path) {
        this.method = method;
        StringBuilder urlBuf = new StringBuilder(200);
        urlBuf.append("http://localhost:").append(
                node.settings().get("http.port"));
        if (path.startsWith("/")) {
            urlBuf.append(path);
        } else {
            urlBuf.append('/').append(path);
        }
        this.url = urlBuf.toString();
    }

    public CurlRequest encoding(final String encoding) {
        if (paramList != null) {
            throw new CurlException(
                    "This method must be called before param method.");
        }
        this.encoding = encoding;
        return this;
    }

    public CurlRequest body(final String body) {
        this.body = body;
        return this;
    }

    public CurlRequest param(final String key, final String value) {
        if (paramList == null) {
            paramList = new ArrayList<>();
        }
        paramList.add(encode(key) + "=" + encode(value));
        return this;
    }

    public void execute(final ResponseListener listener) {
        if (paramList != null) {
            char sp;
            if (url.indexOf('?') == -1) {
                sp = '?';
            } else {
                sp = '&';
            }
            final StringBuilder urlBuf = new StringBuilder(url.length() + 100);
            for (final String param : paramList) {
                urlBuf.append(sp).append(param);
                if (sp == '?') {
                    sp = '&';
                }
            }
            if (Method.GET == method || body != null) {
                url = url + urlBuf.toString();
            } else {
                body = urlBuf.substring(1);
            }
        }

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod(method.toString());
            if (body != null) {
                connection.setDoOutput(true);
                try (BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(connection.getOutputStream(),
                                encoding))) {
                    writer.write(body);
                    writer.flush();
                }
            }
            listener.onResponse(connection);
        } catch (final Exception e) {
            throw new CurlException("Failed to access to " + url, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public CurlResponse execute() {
        final CurlResponse response = new CurlResponse();
        execute(new ResponseListener() {
            @Override
            public void onResponse(final HttpURLConnection con) {
                try {
                    response.setHttpStatusCode(con.getResponseCode());
                    response.setEncoding(encoding);
                    final Path tempFile = Files.createTempFile("esrunner-",
                            ".tmp");
                    try (BufferedInputStream bis = new BufferedInputStream(
                            con.getInputStream());
                            BufferedOutputStream bos = new BufferedOutputStream(
                                    Files.newOutputStream(tempFile,
                                            StandardOpenOption.WRITE))) {
                        byte[] bytes = new byte[4096];
                        try {
                            int length = bis.read(bytes);
                            while (length != -1) {
                                if (length != 0) {
                                    bos.write(bytes, 0, length);
                                }
                                length = bis.read(bytes);
                            }
                        } finally {
                            bytes = null;
                        }
                        bos.flush();
                        response.setContentFile(tempFile);
                    } catch (Exception e) {
                        response.setContentException(e);
                        try {
                            Files.deleteIfExists(tempFile);
                        } catch (Exception ignore) {
                            // ignore
                        }
                    }
                } catch (final Exception e) {
                    throw new CurlException("Failed to access the response.", e);
                }
            }
        });
        return response;
    }

    protected String encode(final String value) {
        try {
            return URLEncoder.encode(value, encoding);
        } catch (final UnsupportedEncodingException e) {
            throw new CurlException("Invalid encoding: " + encoding, e);
        }
    }

}
