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

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
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
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

    static private final String VERSION = "2012-06-15";

    static public final int OK             = 200;
    static public final int CREATED        = 201;
    static public final int ACCEPTED       = 202;
    static public final int NO_CONTENT     = 204;
    static public final int BAD_REQUEST    = 400;
    static public final int NOT_FOUND      = 404;

    static public class APIResponse {
        public int code;
        public JSONObject json;
        public InputStream data;
    }

    private Enstratus provider;

    public EnstratusMethod(@Nonnull Enstratus provider) { this.provider = provider; }

    public void delete(@Nonnull String resource, @Nonnull String id, @Nullable NameValuePair ... parameters) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".delete(" + resource + "," + id + "," + Arrays.toString(parameters) + ")");
        }
        try {
            String target = getEndpoint(resource, id, parameters);

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
                        throw new CloudException("No such endpoint: " + target);
                    }
                    if( status.getStatusCode() != NO_CONTENT ) {
                        logger.error("Expected NO CONTENT for DELETE request, got " + status.getStatusCode());
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

    public @Nonnull APIResponse get(@Nonnull String resource, @Nullable String id, @Nullable NameValuePair ... parameters) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".get(" + resource + "," + id + "," + Arrays.toString(parameters) + ")");
        }
        try {
            String target = getEndpoint(resource, id, parameters);

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
                    long timestamp = System.currentTimeMillis();

                    String signature = getSignature(ctx.getAccessPublic(), ctx.getAccessPrivate(), "GET", resource, id, timestamp);

                    try {
                        get.addHeader("x-esauth-access", new String(ctx.getAccessPublic(), "utf-8"));
                    }
                    catch( UnsupportedEncodingException e ) {
                        throw new InternalException(e);
                    }
                    get.addHeader("Accept", "application/json");
                    get.addHeader("x-esauth-signature", signature);
                    get.addHeader("x-esauth-timestamp", String.valueOf(timestamp));
                    get.addHeader("x-es-details", "basic");
                    //x-es-with-perms: false
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
                        APIResponse r = new APIResponse();

                        r.code = status.getStatusCode();
                        return r;
                    }
                    if( status.getStatusCode() != OK ) {
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
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new CloudException("No entity was returned from an HTTP GET");
                        }
                        APIResponse r = new APIResponse();

                        r.code = status.getStatusCode();
                        if( entity.getContentType() == null || entity.getContentType().getValue().contains("json") ) {
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

                            try {
                                r.json = new JSONObject(body);
                            }
                            catch( JSONException e ) {
                                throw new CloudException(e);
                            }
                            return r;
                        }
                        else {
                            try {
                                r.data = entity.getContent();
                            }
                            catch( IOException e ) {
                                throw new CloudException(e);
                            }
                        }
                        return r;
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
                logger.trace("EXIT - " + Enstratus.class.getName() + ".get()");
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


        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");
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
        return new DefaultHttpClient(params);
    }

    private @Nonnull String getEndpoint(@Nonnull String resource, @Nullable String id, @Nullable NameValuePair... parameters) throws EnstratusConfigurationException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null ) {
            logger.error("Null endpoint for the Enstratus cloud");
            throw new EnstratusConfigurationException("Null endpoint for Enstratus cloud");
        }
        if( endpoint.endsWith("/") ) {
            endpoint = endpoint + "api/enstratus/" + VERSION;
        }
        else {
            endpoint = endpoint + "/api/enstratus/" + VERSION;
        }
        if( resource.startsWith("/") ) {
            endpoint =  endpoint + resource;
        }
        else {
            endpoint = endpoint + "/" + resource;
        }
        if( id != null ) {
            if( endpoint.endsWith("/") ) {
                endpoint = endpoint + id;
            }
            else {
                endpoint = endpoint + "/" + id;
            }
        }
        if( parameters != null && parameters.length > 0 ) {
            while( endpoint.endsWith("/") ) {
                endpoint = endpoint.substring(0, endpoint.length()-1);
            }
            endpoint = endpoint + "?";
            ArrayList<NameValuePair> params = new ArrayList<NameValuePair>();

            Collections.addAll(params, parameters);
            endpoint = endpoint + URLEncodedUtils.format(params, "utf-8");
        }
        return endpoint;
    }

    private @Nonnull String getSignature(@Nonnull byte[] accessKey, @Nonnull byte[] privateKey, @Nonnull String method, @Nonnull String resource, @Nullable String id, long timestamp) throws InternalException {
        try {
            String stringToSign = (new String(accessKey, "utf-8")) + ":" + method + ":/api/enstratus/" + VERSION;

            if( resource.startsWith("/") ) {
                stringToSign = stringToSign + resource;
            }
            else {
                stringToSign = stringToSign + "/" + resource;
            }
            if( id != null ) {
                if( id.startsWith("/") ) {
                    stringToSign = stringToSign + id;
                }
                else
                    stringToSign = stringToSign + "/" + id;
            }
            stringToSign = stringToSign + ":" + timestamp + ":Dasein Cloud";

            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(privateKey, "HmacSHA256"));
                return new String(Base64.encodeBase64(mac.doFinal(stringToSign.getBytes("utf-8"))));
            }
            catch( NoSuchAlgorithmException e ) {
                throw new InternalException(e);
            }
            catch( InvalidKeyException e ) {
                throw new InternalException(e);
            }
            catch( UnsupportedEncodingException e ) {
                throw new InternalException(e);
            }
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
    }

    public @Nonnull APIResponse post(@Nonnull String resource, @Nonnull String json) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".post(" + resource + "," + json + ")");
        }
        try {
            String target = getEndpoint(resource, null);

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

                    post.addHeader("Content-type", "application/json;charset=utf-8");
                    try {
                        post.setEntity(new StringEntity(json, "utf-8"));
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
                        wire.debug(json);
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
                        throw new CloudException("No such endpoint: " + target);
                    }
                    if( status.getStatusCode() != ACCEPTED && status.getStatusCode() != CREATED ) {
                        logger.error("Expected ACCEPTED or CREATED for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            json = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                        }
                        wire.debug("");
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), json);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new CloudException("No response to the POST");
                        }
                        try {
                            json = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                        }
                        wire.debug("");
                        APIResponse r = new APIResponse();

                        r.code = status.getStatusCode();
                        try {
                            r.json = new JSONObject(json);
                        }
                        catch( JSONException e ) {
                            throw new CloudException(e);
                        }
                        return r;
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
                logger.trace("EXIT - " + Enstratus.class.getName() + ".post()");
            }
        }
    }

    public @Nonnull APIResponse put(@Nonnull String resource, @Nonnull String id, @Nonnull String json) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".put(" + resource + "," + id + "," + json + ")");
        }
        try {
            String target = getEndpoint(resource, id, null);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
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

                    post.addHeader("Content-type", "application/json;charset=utf-8");
                    try {
                        post.setEntity(new StringEntity(json, "utf-8"));
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
                        wire.debug(json);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        APITrace.trace(provider, "PUT " + resource);
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
                    if( status.getStatusCode() == NOT_FOUND || status.getStatusCode() == NO_CONTENT ) {
                        APIResponse r = new APIResponse();

                        r.code = status.getStatusCode();
                        return r;
                    }
                    if( status.getStatusCode() != ACCEPTED ) {
                        logger.error("Expected ACCEPTED or CREATED for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            json = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                        }
                        wire.debug("");
                        throw new EnstratusException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), json);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            throw new CloudException("No response to the PUT");
                        }
                        try {
                            json = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new EnstratusException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                        }
                        wire.debug("");
                        APIResponse r = new APIResponse();

                        r.code = status.getStatusCode();
                        try {
                            r.json = new JSONObject(json);
                        }
                        catch( JSONException e ) {
                            throw new CloudException(e);
                        }
                        return r;
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".put()");
            }
        }
    }

}
