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

import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.Hour;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

/**
 * Maps enStratus regions and data centers into Dasein Cloud regions and data centers.
 * <p>Created by George Reese: 11/22/12 4:35 PM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class DataCenters implements DataCenterServices {
    static private final Logger logger = Enstratus.getLogger(DataCenters.class);

    static public final String DATA_CENTER = "geography/DataCenter";
    static public final String REGION      = "geography/Region";

    private Enstratus provider;

    DataCenters(@Nonnull Enstratus provider) { this.provider = provider; }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dataCenterId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            for( DataCenter dc : listDataCenters(region.getProviderRegionId()) ) {
                if( dataCenterId.equals(dc.getProviderDataCenterId()) ) {
                    return dc;
                }
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String providerRegionId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            if( providerRegionId.equals(r.getProviderRegionId()) ) {
                return r;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, "listDataCenters");
        try {
            Region region = getRegion(providerRegionId);

            if( region == null ) {
                throw new CloudException("No such region: " + providerRegionId);
            }
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new NoContextException();
            }
            Cache<DataCenter> cache = Cache.getInstance(provider, "dataCenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
            Collection<DataCenter> dcList = (Collection<DataCenter>)cache.get(ctx);

            if( dcList != null ) {
                return dcList;
            }
            EnstratusMethod method = new EnstratusMethod(provider);

            EnstratusMethod.APIResponse r = method.get(DATA_CENTER, null, new BasicNameValuePair("regionId", providerRegionId));

            if( r.code == EnstratusMethod.OK ) {
                ArrayList<DataCenter> dataCenters = new ArrayList<DataCenter>();

                if( r.json.has("dataCenters") ) {
                    try {
                        JSONArray list = r.json.getJSONArray("dataCenters");

                        for( int i=0; i<list.length(); i++ ) {
                            DataCenter dc = toDataCenter(list.getJSONObject(i));

                            if( dc != null ) {
                                dataCenters.add(dc);
                            }
                        }
                    }
                    catch( JSONException e ) {
                        logger.error("Invalid JSON from enStratus: " + e.getMessage());
                        throw new CloudException(e);
                    }
                }
                cache.put(ctx, dataCenters);
                return dataCenters;
            }
            logger.error("No data returned for data centers query, but no error (regionId=" + providerRegionId + ")");
            throw new CloudException("Failed to identify data centers in " + providerRegionId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, "listRegions");
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new NoContextException();
            }
            Cache<Region> cache = Cache.getInstance(provider, "regions", Region.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Hour>(10, TimePeriod.HOUR));
            Collection<Region> regions = (Collection<Region>)cache.get(ctx);

            if( regions != null ) {
                return regions;
            }

            EnstratusMethod method = new EnstratusMethod(provider);
            EnstratusMethod.APIResponse r = method.get(REGION, null);

            if( r.code == EnstratusMethod.OK ) {
                regions = new ArrayList<Region>();
                if( r.json.has("regions") ) {
                    try {
                        JSONArray list = r.json.getJSONArray("regions");

                        for( int i=0; i<list.length(); i++ ) {
                            Region region = toRegion(list.getJSONObject(i));

                            if( region != null ) {
                                regions.add(region);
                            }
                        }
                    }
                    catch( JSONException e ) {
                        logger.error("Invalid JSON from enStratus: " + e.getMessage());
                        throw new CloudException(e);
                    }
                }
                return regions;
            }
            logger.error("The enStratus regions endpoint did not provide any data and did not error");
            throw new CloudException("Failed to identify regions");

        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable DataCenter toDataCenter(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        DataCenter dc = new DataCenter();

        dc.setAvailable(true);
        try {
            if( json.has("status") ) {
                dc.setActive("ACTIVE".equalsIgnoreCase(json.getString("status")));
            }
            if( json.has("name") ) {
                dc.setName(json.getString("name"));
            }
            else if( json.has("providerId") ) {
                dc.setName(json.getString("providerId"));
            }
            else if( json.has("description") ) {
                dc.setName(json.getString("description"));
            }
            if( json.has("dataCenterId") ) {
                dc.setProviderDataCenterId(json.getString("dataCenterId"));
            }
            if( json.has("region") ) {
                JSONObject r = json.getJSONObject("region");

                if( r.has("regionId") ) {
                    dc.setRegionId(r.getString("regionId"));
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( dc.getProviderDataCenterId() == null || dc.getRegionId() == null ) {
            return null;
        }
        if( dc.getName() == null ) {
            dc.setName(dc.getProviderDataCenterId());
        }
        return dc;
    }

    private @Nullable Region toRegion(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        Region region = new Region();

        region.setAvailable(true);
        try {
            if( json.has("status") ) {
                region.setActive("ACTIVE".equalsIgnoreCase(json.getString("status")));
            }
            if( json.has("name") ) {
                region.setName(json.getString("name"));
            }
            else if( json.has("providerId") ) {
                region.setName(json.getString("providerId"));
            }
            else if( json.has("description") ) {
                region.setName(json.getString("description"));
            }
            if( json.has("regionId") ) {
                region.setProviderRegionId(json.getString("regionId"));
            }
            if( json.has("jurisdiction") ) {
                region.setJurisdiction(json.getString("jurisdiction"));
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( region.getProviderRegionId() == null ) {
            return null;
        }
        if( region.getName() == null ) {
            region.setName(region.getProviderRegionId());
        }
        return region;
    }
}
