package org.codelibs.elasticsearch.runner.net;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.elasticsearch.common.xcontent.json.JsonXContent;

public class CurlResponse implements Closeable {

    private int httpStatusCode;

    private Path tempFile;

    private String encoding;

    private Exception contentException;

    @Override
    public void close() throws IOException {
        if (tempFile != null) {
            Files.delete(tempFile);
        }
    }

    public Map<String, Object> getContentAsMap() {
        try (InputStream is = getContentAsStream()) {
            return JsonXContent.jsonXContent.createParser(is).map();
        } catch (Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    }

    public String getContentAsString() {
        final byte[] bytes = new byte[4096];
        try (BufferedInputStream bis = new BufferedInputStream(
                getContentAsStream());
                ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            int length = bis.read(bytes);
            while (length != -1) {
                if (length != 0) {
                    baos.write(bytes, 0, length);
                }
                length = bis.read(bytes);
            }
            return baos.toString(encoding);
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    }

    public InputStream getContentAsStream() throws IOException {
        if (tempFile == null) {
            throw new CurlException("The content does not exist.");
        }
        return Files.newInputStream(tempFile, StandardOpenOption.READ);
    }

    public void setContentFile(final Path tempFile) {
        this.tempFile = tempFile;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public void setHttpStatusCode(final int httpStatusCode) {
        this.httpStatusCode = httpStatusCode;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(final String encoding) {
        this.encoding = encoding;
    }

    public void setContentException(Exception e) {
        this.contentException = e;
    }

    public Exception getContentException() {
        return contentException;
    }
}
