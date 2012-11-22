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

import org.dasein.cloud.CloudException;
import org.dasein.util.CalendarWrapper;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;

/**
 * A response from the enStratus API. It handles all of the semantics of asynchronous page navigation.
 * <p>Created by George Reese: 11/22/12 1:05 PM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class APIResponse {
    private int code;
    private JSONObject json;
    private InputStream data;
    private Boolean complete;

    private CloudException error;
    private APIResponse next;

    public APIResponse() { }

    public int getCode() throws CloudException {
        synchronized( this ) {
            while( complete == null && error == null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            return code;
        }
    }

    public InputStream getData() throws CloudException {
        synchronized( this ) {
            while( complete == null && error == null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            return data;
        }
    }

    public JSONObject getJSON() throws CloudException {
        synchronized( this ) {
            while( complete == null && error ==null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            return json;
        }
    }

    public boolean isComplete() throws CloudException {
        synchronized( this ) {
            while( complete == null && error == null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            return complete;
        }
    }

    public @Nonnull APIResponse next() throws CloudException {
        synchronized( this ) {
            while( complete == null && error == null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            if( complete ) {
                return null;
            }
            while( next == null && error == null ) {
                try { wait(CalendarWrapper.MINUTE); }
                catch( InterruptedException ignore ) { }
            }
            if( error != null ) {
                throw error;
            }
            return next;
        }
    }

    void receive() {
        synchronized( this ) {
            this.code = EnstratusMethod.NOT_FOUND;
            this.complete = true;
            notifyAll();
        }
    }

    void receive(CloudException error) {
        synchronized( this ) {
            this.code = error.getHttpCode();
            this.error = error;
            this.complete = true;
            notifyAll();
        }
    }

    void receive(int statusCode, @Nonnull InputStream data) {
        synchronized( this ) {
            this.code = statusCode;
            this.data = data;
            this.complete = true;
            notifyAll();
        }
    }

    void receive(int statusCode, @Nonnull JSONObject json, boolean complete) {
        synchronized( this ) {
            this.code = statusCode;
            this.json = json;
            this.complete = complete;
            notifyAll();
        }
    }

    void setNext(APIResponse next) {
        synchronized( this ) {
            this.next = next;
            notifyAll();
        }
    }
}
