/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.enstratus;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.X509SSLSocketFactory;
import org.dasein.cloud.util.X509Store;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Date;
import java.util.Properties;

/**
 * Handles communication with the Enstratus REST endpoint by abstracting out the specifics of authentication and
 * HTTP negotiation.
 * <p>Created by George Reese: 10/25/12 7:43 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.02
 */
public class EnstratusMethod {
    static private final Logger logger = Enstratus.getLogger(EnstratusMethod.class);
    static private final Logger wire   = Enstratus.getWireLogger(EnstratusMethod.class);

    static private final String VERSION = "v3";

    static private final int OK             = 200;
    static private final int CREATED        = 201;
    static private final int NO_CONTENT     = 204;
    static private final int BAD_REQUEST    = 400;
    static private final int NOT_FOUND      = 404;

    private Enstratus provider;

    public EnstratusMethod(@Nonnull Enstratus provider) { this.provider = provider; }

    public @Nullable String create(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".create(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                }
                catch( URISyntaxException e ) {
                    throw new EnstratusConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if( ctx == null ) {
                        throw new NoContextException();
                    }
                    HttpPost post = new HttpPost(target);

                    post.addHeader("Content-type", "application/xml;charset=utf-8");
                    try {
                        post.setEntity(new StringEntity(body, "utf-8"));
                    }
                    catch( UnsupportedEncodingException e ) {
                        logger.error("Unsupported encoding UTF-8: " + e.getMessage());
                        throw new InternalException(e);
                    }

                    if( wire.isDebugEnabled() ) {
                        wire.debug(post.getRequestLine().toString());
                        for( Header header : post.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, "POST " + resource);
                        response = client.execute(post);
                        status = response.getStatusLine();
                    }
                    catch( IOException e ) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if( wire.isDebugEnabled() ) {
                        wire.debug(status.toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if( status.getStatusCode() == NOT_FOUND ) {
                        return null;
                    }
                    if( status.getStatusCode() != CREATED ) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                    else {
                        Header location = response.getFirstHeader("Location");

                        if( location == null || location.getValue() == null ) {
                            throw new CloudException("No location header specified");
                        }
                        int idx = location.getValue().lastIndexOf("/");

                        if( idx == -1 ) {
                            return location.getValue();
                        }
                        return location.getValue().substring(idx+1);
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".create()");
            }
        }
    }

    public void delete(@Nonnull String resource) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".delete(" + resource + ")");
        }

        try {
            String target = getEndpoint(resource);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [DELETE (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                }
                catch( URISyntaxException e ) {
                    throw new EnstratusConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if( ctx == null ) {
                        throw new NoContextException();
                    }
                    HttpDelete delete = new HttpDelete(target);

                    if( wire.isDebugEnabled() ) {
                        wire.debug(delete.getRequestLine().toString());
                        for( Header header : delete.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, "DELETE " + resource);
                        response = client.execute(delete);
                        status = response.getStatusLine();
                    }
                    catch( IOException e ) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if( wire.isDebugEnabled() ) {
                        wire.debug(status.toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if( status.getStatusCode() == NOT_FOUND ) {
                        throw new CloudException("No such endpoint: " + resource);
                    }
                    if( status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT ) {
                        logger.error("Expected OK or NO CONTENT for DELETE request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        String body;

                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [DELETE (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".delete()");
            }
        }
    }

    public @Nullable Document getObject(@Nonnull String resource) throws InternalException, CloudException {
        String body = getString(resource);

        if( body == null || body.trim().length() < 1 ) {
            return null;
        }
        try {
            ByteArrayInputStream bas = new ByteArrayInputStream(body.getBytes("utf-8"));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = factory.newDocumentBuilder();

            return parser.parse(bas);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
        catch( ParserConfigurationException e ) {
            logger.error("Misconfigured XML parser: " + e.getMessage());
            throw new InternalException(e);
        }
        catch( SAXException e ) {
            logger.error("Error parsing XML from the cloud provider: " + e.getMessage());
            throw new CloudException(e);
        }
        catch( IOException e ) {
            logger.error("Error communicating with the cloud provider: " + e.getMessage());
            throw new CloudException(e);
        }
    }

    public @Nullable String getString(@Nonnull String resource) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".getString(" + resource + ")");
        }

        try {
            String target = getEndpoint(resource);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                }
                catch( URISyntaxException e ) {
                    throw new EnstratusConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if( ctx == null ) {
                        throw new NoContextException();
                    }
                    HttpGet get = new HttpGet(target);

                    if( wire.isDebugEnabled() ) {
                        wire.debug(get.getRequestLine().toString());
                        for( Header header : get.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, "GET " + resource);
                        response = client.execute(get);
                        status = response.getStatusLine();
                    }
                    catch( IOException e ) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if( wire.isDebugEnabled() ) {
                        wire.debug(status.toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if( status.getStatusCode() == NOT_FOUND ) {
                        return null;
                    }
                    if( status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT ) {
                        logger.error("Expected OK for GET request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();
                        String body;

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        if( status.getStatusCode() == BAD_REQUEST && body.contains("could not be found") ) {
                            return null;
                        }
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            return "";
                        }
                        String body;

                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".getString()");
            }
        }
    }

    private @Nonnull HttpClient getClient(URI uri) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }

        boolean ssl = uri.getScheme().startsWith("https");
        int targetPort = uri.getPort();

        if( targetPort < 1 ) {
            targetPort = (ssl ? 443 : 80);
        }
        HttpParams params = new BasicHttpParams();

        SchemeRegistry registry = new SchemeRegistry();

        try {
            registry.register(new Scheme(ssl ? "https" : "http", targetPort, new X509SSLSocketFactory(new X509Store(ctx))));
        }
        catch( KeyManagementException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( UnrecoverableKeyException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( NoSuchAlgorithmException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( KeyStoreException e ) {
            e.printStackTrace();
            throw new InternalException(e);
        }

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "");
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, 300000);

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        ClientConnectionManager ccm = new ThreadSafeClientConnManager(registry);

        return new DefaultHttpClient(ccm, params);
    }

    private @Nonnull String getEndpoint(@Nonnull String resource) throws EnstratusConfigurationException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null ) {
            logger.error("Null endpoint for the Enstratus cloud");
            throw new EnstratusConfigurationException("Null endpoint for Enstratus cloud");
        }
        /*
        if( endpoint.endsWith("/") ) {
            endpoint = endpoint + "api/services";
        }
        else {
            endpoint = endpoint + "/api/services";
        }
        */
        while( endpoint.endsWith("/") && !endpoint.equals("") ) {
            endpoint = endpoint.substring(0, endpoint.length()-1);
        }
        if( resource.startsWith("/") ) {
            return endpoint + resource;
        }
        else {
            return endpoint + "/" + resource;
        }
    }

    public @Nullable Document postObject(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        String response = postString(resource, body);

        if( response == null || response.trim().length() < 1 ) {
            return null;
        }
        try {
            ByteArrayInputStream bas = new ByteArrayInputStream(response.getBytes("utf-8"));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder parser = factory.newDocumentBuilder();

            return parser.parse(bas);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
        catch( ParserConfigurationException e ) {
            logger.error("Misconfigured XML parser: " + e.getMessage());
            throw new InternalException(e);
        }
        catch( SAXException e ) {
            logger.error("Error parsing XML from the cloud provider: " + e.getMessage());
            throw new CloudException(e);
        }
        catch( IOException e ) {
            logger.error("Error communicating with the cloud provider: " + e.getMessage());
            throw new CloudException(e);
        }
    }

    public @Nullable String postString(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".postString(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                }
                catch( URISyntaxException e ) {
                    throw new EnstratusConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if( ctx == null ) {
                        throw new NoContextException();
                    }
                    HttpPost post = new HttpPost(target);

                    post.addHeader("Content-type", "application/xml;charset=utf-8");
                    try {
                        post.setEntity(new StringEntity(body, "utf-8"));
                    }
                    catch( UnsupportedEncodingException e ) {
                        logger.error("Unsupported encoding UTF-8: " + e.getMessage());
                        throw new InternalException(e);
                    }

                    if( wire.isDebugEnabled() ) {
                        wire.debug(post.getRequestLine().toString());
                        for( Header header : post.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, "POST " + resource);
                        response = client.execute(post);
                        status = response.getStatusLine();
                    }
                    catch( IOException e ) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if( wire.isDebugEnabled() ) {
                        wire.debug(status.toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if( status.getStatusCode() == NOT_FOUND ) {
                        return null;
                    }
                    if( status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT ) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".postString()");
            }
        }
    }
}
