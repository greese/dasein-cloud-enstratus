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
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * Contains information about the subscription for cloud services in the current region. Because a given region in
 * enStratus can be representing a region in any cloud, all support must be dynamically looked up. The subscription
 * objects (which get cached) hold that information.
 * <p>Created by George Reese: 11/22/12 9:34 AM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class Subscription {
    static public @Nonnull Subscription getSubscription(@Nonnull Enstratus provider) throws CloudException, InternalException {
        APITrace.begin(provider, "getSubscription");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new NoContextException();
            }
            Cache<Subscription> cache = Cache.getInstance(provider, "regionSubscriptions", Subscription.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
            Iterable<Subscription> s = cache.get(ctx);

            if( s != null ) {
                for( Subscription subscription : s ) {
                    if( subscription.getRegionId().equals(ctx.getRegionId()) ) {
                        return subscription;
                    }
                }
            }
            EnstratusMethod method = new EnstratusMethod(provider);
            ArrayList<Subscription> subscriptions = new ArrayList<Subscription>();
            APIResponse r = method.get("getSubscription.get", "geography/Subscription", null);

            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No response and no error message");
            }
            if( r.getJSON().has("subscriptions") ) {
                try {
                    JSONArray list = r.getJSON().getJSONArray("subscriptions");

                    for( int i=0; i<list.length(); i++ ) {
                        Subscription subscription = toSubscription(list.getJSONObject(i));

                        if( subscription != null ) {
                            subscriptions.add(subscription);
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException(e);
                }
            }
            cache.put(ctx, subscriptions);
            for( Subscription subscription : subscriptions ) {
                if( subscription.getRegionId().equals(ctx.getRegionId()) ) {
                    return subscription;
                }
            }
            throw new CloudException("No subscription exists for " + ctx.getRegionId());
        }
        finally {
            APITrace.end();
        }
    }

    static private @Nullable Subscription toSubscription(@Nonnull JSONObject json) throws CloudException, InternalException {
        Subscription s = new Subscription();

        try {
            if( json.has("regionId") ) {
                s.regionId = json.getString("regionId");
            }
            s.subscribedMachineImage = json.has("subscribedMachineImage") && json.getBoolean("subscribedMachineImage");
            s.subscribedVirtualMachine = json.has("subscribedServer") && json.getBoolean("subscribedServer");
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
        if( s.regionId == null ) {
            return null;
        }
        return s;
    }

    private String  regionId;
    private boolean subscribedMachineImage;
    private boolean subscribedVirtualMachine;

    private Subscription() { }

    public @Nonnull String getRegionId() {
        return regionId;
    }

    public boolean isSubscribedMachineImage() {
        return subscribedMachineImage;
    }

    public boolean isSubscribedVirtualMachine() {
        return subscribedVirtualMachine;
    }
}
