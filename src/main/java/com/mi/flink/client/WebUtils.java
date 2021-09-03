package com.mi.flink.client;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class WebUtils {
    private static final Logger LOG = LoggerFactory.getLogger(WebUtils.class);

    public static final Time DEFAULT_HTTP_TIMEOUT = Time.seconds(10L);

    public static String getFromHTTP(String url) throws Exception {
        return getFromHTTP(url, DEFAULT_HTTP_TIMEOUT);
    }

    public static String getFromHTTP(String url, Time timeout) throws Exception {
        final URL u = new URL(url);
        LOG.info("Accessing URL " + url + " as URL: " + u);

        final long deadline = timeout.toMilliseconds() + System.currentTimeMillis();

        while (System.currentTimeMillis() <= deadline) {
            HttpURLConnection connection = (HttpURLConnection) u.openConnection();
            connection.setConnectTimeout(100000);
            connection.connect();

            if (Objects.equals(
                    HttpResponseStatus.SERVICE_UNAVAILABLE,
                    HttpResponseStatus.valueOf(connection.getResponseCode()))) {
                // service not available --> Sleep and retry
                LOG.debug("Web service currently not available. Retrying the request in a bit.");
                Thread.sleep(100L);
            } else {
                InputStream is;

                if (connection.getResponseCode() >= 400) {
                    // error!
                    LOG.warn(
                            "HTTP Response code when connecting to {} was {}",
                            url,
                            connection.getResponseCode());
                    is = connection.getErrorStream();
                } else {
                    is = connection.getInputStream();
                }

                return IOUtils.toString(is, ConfigConstants.DEFAULT_CHARSET);
            }
        }

        throw new TimeoutException(
                "Could not get HTTP response in time since the service is still unavailable.");
    }
}
