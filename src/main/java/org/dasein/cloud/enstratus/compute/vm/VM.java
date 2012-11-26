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
package org.dasein.cloud.enstratus.compute.vm;

import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VMScalingOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.enstratus.APIResponse;
import org.dasein.cloud.enstratus.Enstratus;
import org.dasein.cloud.enstratus.EnstratusMethod;
import org.dasein.cloud.enstratus.NoContextException;
import org.dasein.cloud.enstratus.DetailLevel;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.Hour;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Implementation of cross-cloud virtual machine support using the enStratus API.
 * <p>Created by George Reese: 11/22/12 9:25 AM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class VM implements VirtualMachineSupport {
    static private final Logger logger = Enstratus.getLogger(VM.class);

    static public final String SERVER         = "infrastructure/Server";
    static public final String SERVER_PRODUCT = "infrastructure/ServerProduct";

    private Enstratus provider;

    public VM(@Nonnull Enstratus provider) { this.provider = provider; }

    @Override
    public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void disableAnalytics(String vmId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void enableAnalytics(String vmId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        for( Architecture architecture : listSupportedArchitectures() ) {
            for( VirtualMachineProduct product : listProducts(architecture) ) {
                if( productId.equals(product.getProviderProductId()) ) {
                    return product;
                }
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        APIResponse r = method.get("getVirtualMachine", SERVER, vmId);

        if( r.getCode() == EnstratusMethod.NOT_FOUND ) {
            return null;
        }
        if( r.getCode() != EnstratusMethod.OK ) {
            throw new CloudException("No error and no response: " + r.getCode());
        }
        JSONObject json = r.getJSON();

        if( json.has("servers") ) {
            try {
                JSONArray list = json.getJSONArray("servers");

                for( int i=0; i<list.length(); i++ ) {
                    VirtualMachine vm = toVirtualMachine(list.getJSONObject(i));

                    if( vm != null ) {
                        return vm;
                    }
                }
            }
            catch( JSONException e ) {
                logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                throw new CloudException(e);
            }
        }
        return null;
    }

    @Override
    public VmStatistics getVMStatistics(String vmId, long from, long to) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, @Nonnegative long from, @Nonnegative long to) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.NONE);
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        ArrayList<Map<String,Object>> servers = new ArrayList<Map<String, Object>>();
        HashMap<String,Object> request = new HashMap<String, Object>();
        HashMap<String,Object> tmp;

        request.put("launch", servers);

        HashMap<String,Object> server = new HashMap<String,Object>();

        servers.add(server);

        tmp = new HashMap<String, Object>();
        tmp.put("machineImageId", withLaunchOptions.getMachineImageId());
        server.put("machineImage", tmp);
        server.put("name", withLaunchOptions.getFriendlyName());
        server.put("description", withLaunchOptions.getDescription());

        if( withLaunchOptions.getDataCenterId() != null ) {
            tmp = new HashMap<String, Object>();
            tmp.put("dataCenterId", withLaunchOptions.getDataCenterId());
            server.put("dataCenter", tmp);
        }
        if( withLaunchOptions.getFirewallIds().length > 0 ) {
            ArrayList<Map<String,Object>> list = new ArrayList<Map<String, Object>>();

            for( String id : withLaunchOptions.getFirewallIds() ) {
                HashMap<String,Object> fw = new HashMap<String, Object>();

                fw.put("firewallId", id);
                list.add(fw);
            }
            server.put("firewalls", list);
        }
        tmp = new HashMap<String, Object>();
        tmp.put("productId", withLaunchOptions.getStandardProductId());
        server.put("product", tmp);

        String meta = (String)withLaunchOptions.getMetaData().get("label");

        if( meta != null ) {
            server.put("label", meta);
        }
        meta = (String)withLaunchOptions.getMetaData().get("budget");
        if( meta != null ) {
            server.put("budget", meta);
        }
        else {
            server.put("budget", provider.findBudget());
        }
        String json = (new JSONObject(request)).toString();
        EnstratusMethod method = new EnstratusMethod(provider);

        APIResponse response = method.post(SERVER, json);
        JSONObject j = response.getJSON();

        if( response.getCode() == EnstratusMethod.ACCEPTED ) {
            String id = provider.waitForJob("launchVM", response.getJSON());

            if( id == null ) {
                throw new CloudException("VM launch completed without providing a VM ID");
            }
            VirtualMachine vm =  getVirtualMachine(id);

            if( vm == null ) {
                throw new CloudException("Launched VM " + id + " does not exist");
            }
            return vm;
        }
        else if( response.getCode() == EnstratusMethod.CREATED ) {
            if( j.has("servers") ) {
                try {
                    j = j.getJSONArray("servers").getJSONObject(0);
                    if( j.has("serverId") ) {
                        VirtualMachine vm =  getVirtualMachine(j.getString("serverId"));

                        if( vm == null ) {
                            throw new CloudException("Launched VM " + j.getString("serverId") + " does not exist");
                        }
                        return vm;
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException(e);
                }
            }
            throw new CloudException("No servers were returned from the server as a result of the launch");
        }
        else {
            throw new CloudException("Unxpected response: " + response.getCode());
        }
    }

    @Override
    @Deprecated
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String... firewallIds) throws InternalException, CloudException {
        //noinspection deprecation
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewallIds, new Tag[0]);
    }

    @Override
    @Deprecated
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        VMLaunchOptions cfg = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description);

        if( withKeypairId != null ) {
            cfg.withBoostrapKey(withKeypairId);
        }
        if( inVlanId != null ) {
            NetworkServices svc = provider.getNetworkServices();

            if( svc != null ) {
                VLANSupport support = svc.getVlanSupport();

                if( support != null ) {
                    Subnet subnet = support.getSubnet(inVlanId);

                    if( subnet == null ) {
                        throw new CloudException("No such VPC subnet: " + inVlanId);
                    }
                    dataCenterId = subnet.getProviderDataCenterId();
                }
            }
            cfg.inVlan(null, dataCenterId, inVlanId);
        }
        else {
            cfg.inDataCenter(dataCenterId);
        }
        if( withAnalytics ) {
            cfg.withExtendedAnalytics();
        }
        if( firewallIds != null && firewallIds.length > 0 ) {
            cfg.behindFirewalls(firewallIds);
        }
        if( tags != null && tags.length > 0 ) {
            HashMap<String,Object> meta = new HashMap<String, Object>();

            for( Tag t : tags ) {
                meta.put(t.getKey(), t.getValue());
            }
            cfg.withMetaData(meta);
        }
        return launch(cfg);
    }

    @Nonnull
    @Override
    public Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        Cache<VirtualMachineProduct> cache = Cache.getInstance(provider, "serverProducts." + architecture.name(), VirtualMachineProduct.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Hour>(6, TimePeriod.HOUR));
        Iterable<VirtualMachineProduct> products = cache.get(ctx);

        if( products != null ) {
            return products;
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<VirtualMachineProduct> prdList = new ArrayList<VirtualMachineProduct>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listServerProducts", SERVER_PRODUCT, null, new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("serverProducts") ) {
                try {
                    JSONArray list = json.getJSONArray("serverProducts");

                    for( int i=0; i<list.length(); i++ ) {
                        VirtualMachineProduct prd = toProduct(list.getJSONObject(i), architecture);

                        if( prd != null ) {
                            prdList.add(prd);
                        }
                    }

                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        cache.put(ctx, prdList);
        return prdList;
    }

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        Cache<Architecture> cache = Cache.getInstance(provider, "architectures", Architecture.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        Iterable<Architecture> architectures = cache.get(ctx);

        if( architectures != null ) {
            return architectures;
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        TreeSet<Architecture> aList = new TreeSet<Architecture>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listSupportedArchitectures", SERVER_PRODUCT, null, new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("serverProducts") ) {
                try {
                    JSONArray list = json.getJSONArray("serverProducts");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject prd = list.getJSONObject(i);

                        if( prd.has("architecture") ) {
                            aList.add(Architecture.valueOf(prd.getString("architecture")));
                        }
                    }

                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        cache.put(ctx, aList);
        return aList;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<ResourceStatus> vms = new ArrayList<ResourceStatus>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listVirtualMachineStatus", SERVER, null, DetailLevel.none,  new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("servers") ) {
                try {
                    JSONArray list = json.getJSONArray("servers");

                    for( int i=0; i<list.length(); i++ ) {
                        ResourceStatus vm = toStatus(list.getJSONObject(i));

                        if( vm != null ) {
                            vms.add(vm);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        return vms;
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listVirtualMachines", SERVER, null, new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("servers") ) {
                try {
                    JSONArray list = json.getJSONArray("servers");

                    for( int i=0; i<list.length(); i++ ) {
                        VirtualMachine vm = toVirtualMachine(list.getJSONObject(i));

                        if( vm != null ) {
                            vms.add(vm);
                        }
                    }
                    return vms;
                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        return vms;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Pause is not currently supported in enStratus");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Resume is not currently supported in enStratus");
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(provider, "startVM");
        try {
            EnstratusMethod method = new EnstratusMethod(provider);
            HashMap<String,Object> action = new HashMap<String, Object>();
            ArrayList<Map<String,Object>> servers = new ArrayList<Map<String, Object>>();
            HashMap<String,Object> server = new HashMap<String, Object>();

            server.put("serverId", vmId);
            servers.add(server);
            action.put("start", servers);
            method.put(SERVER, vmId, (new JSONObject(action)).toString());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(provider, "stopVM");
        try {
            EnstratusMethod method = new EnstratusMethod(provider);
            HashMap<String,Object> action = new HashMap<String, Object>();
            ArrayList<Map<String,Object>> servers = new ArrayList<Map<String, Object>>();
            HashMap<String,Object> server = new HashMap<String, Object>();

            server.put("serverId", vmId);
            servers.add(server);
            action.put("pause", servers);
            method.put(SERVER, vmId, (new JSONObject(action)).toString());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Suspend is not supported in enStratus");
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(provider, "terminateVM");
        try {
            EnstratusMethod method = new EnstratusMethod(provider);

            method.delete(SERVER, vmId);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull VmState toState(@Nonnull String state) {
        try {
            return VmState.valueOf(state);
        }
        catch( Throwable t ) {
            logger.warn("DEBUG: Unknown enStratus VM state: " + state);
            System.out.println("DEBUG: Unknown enStratus VM state: " + state);
            return VmState.PENDING;
        }
    }

    private @Nullable ResourceStatus toStatus(@Nonnull JSONObject json) throws CloudException, InternalException {
        VmState state = VmState.PENDING;
        String id = null;

        try {
            if( json.has("serverId") ) {
                id = json.getString("serverId");
            }
            if( json.has("status") ) {
                state = toState(json.getString("status"));
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( id == null ) {
            return null;
        }
        return new ResourceStatus(id, state);
    }

    private @Nullable VirtualMachineProduct toProduct(@Nonnull JSONObject json, @Nullable Architecture architecture) throws CloudException, InternalException {
        VirtualMachineProduct prd = new VirtualMachineProduct();
        Architecture actual = null;

        try {
            if( json.has("productId") ) {
                prd.setProviderProductId(json.getString("productId"));
            }
            if( json.has("name") ) {
                prd.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                prd.setDescription(json.getString("description"));
            }
            if( json.has("diskSizeInGb") ) {
                prd.setRootVolumeSize(new Storage<Gigabyte>(json.getInt("diskSizeInGb"), Storage.GIGABYTE));
            }
            if( json.has("ramInMb") ) {
                prd.setRamSize(new Storage<Megabyte>(json.getInt("ramInMb"), Storage.MEGABYTE));
            }
            if( json.has("architecture") ) {
                actual = Architecture.valueOf(json.getString("architecture"));
            }
            if( json.has("hourlyRate") ) {
                prd.setStandardHourlyRate((float)json.getDouble("hourlyRate"));
            }
            if( json.has("cpuCount") ) {
                prd.setCpuCount(json.getInt("cpuCount"));
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( prd.getProviderProductId() == null || (architecture != null && !architecture.equals(actual)) ) {
            return null;
        }
        if( prd.getName() == null ) {
            prd.setName(prd.getProviderProductId());
        }
        if( prd.getDescription() == null ) {
            prd.setDescription(prd.getName());
        }
        return prd;
    }
    private @Nullable VirtualMachine toVirtualMachine(@Nonnull JSONObject json) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }

        VirtualMachine vm = new VirtualMachine();

        vm.setProviderOwnerId(ctx.getAccountNumber());
        try {
            if( json.has("serverId") ) {
                vm.setProviderVirtualMachineId(json.getString("serverId"));
            }
            if( json.has("region") ) {
                JSONObject ob = json.getJSONObject("region");

                if( ob.has("regionId") ) {
                    vm.setProviderRegionId(ob.getString("regionId"));
                }
            }
            if( json.has("name") ) {
                vm.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                vm.setDescription(json.getString("description"));
            }
            if( json.has("platform") ) {
                vm.setPlatform(Platform.guess(json.getString("platform")));
            }
            if( json.has("startDate") ) {
                vm.setCreationTimestamp(provider.parseTimestamp(json.getString("startDate")));
            }
            if( json.has("status") ) {
                vm.setCurrentState(toState(json.getString("status")));
            }
            if( json.has("dataCenter") ) {
                JSONObject ob = json.getJSONObject("dataCenter");

                if( ob.has("dataCenterId") ) {
                    vm.setProviderDataCenterId(ob.getString("dataCenterId"));
                }
            }
            if( json.has("privateIpAddresses") ) {
                TreeSet<String> addresses = new TreeSet<String>();
                JSONArray addrs = json.getJSONArray("privateIpAddresses");

                for( int i=0; i<addrs.length(); i++ ) {
                    addresses.add(addrs.getString(i));
                }
                vm.setPrivateIpAddresses(addresses.toArray(new String[addresses.size()]));
            }
            if( json.has("publicIpAddress") ) {
                String addr = json.getString("publicIpAddress");

                vm.setPublicIpAddresses(new String[] { addr });
            }
            else if( json.has("publicIpAddresses") ) {
                TreeSet<String> addresses = new TreeSet<String>();
                JSONArray addrs = json.getJSONArray("publicIpAddresses");

                for( int i=0; i<addrs.length(); i++ ) {
                    addresses.add(addrs.getString(i));
                }
                vm.setPublicIpAddresses(addresses.toArray(new String[addresses.size()]));
            }
            if( json.has("machineImage") ) {
                JSONObject ob = json.getJSONObject("machineImage");

                if( ob.has("machineImageId") ) {
                    vm.setProviderMachineImageId(ob.getString("machineImageId"));
                }
            }
            if( json.has("budget") ) {
                vm.setTag("budget", json.getString("budget"));
            }
            if( json.has("label") ) {
                vm.setTag("label", json.getString("label"));
            }
            if( json.has("providerId") ) {
                vm.setTag("providerId", json.getString("providerId"));
            }
            if( json.has("cloud") ) {
                JSONObject ob = json.getJSONObject("cloud");

                if( ob.has("cloudId") ) {
                    vm.setTag("cloudId", ob.getString("cloudId"));
                }
            }
            if( json.has("agentVersion") ) {
                vm.setTag("agentVersion", json.getString("agentVersion"));
            }
            if( json.has("owningUser") ) {
                JSONObject ob = json.getJSONObject("owningUser");

                if( ob.has("userId") ) {
                    vm.setTag("owningUserId", ob.getString("userId"));
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( vm.getProviderVirtualMachineId() == null ) {
            return null;
        }
        if( vm.getName() == null ) {
            vm.setName(vm.getProviderVirtualMachineId());
        }
        if( vm.getDescription() == null ) {
            vm.setDescription(vm.getName());
        }
        return vm;
    }

    @Override
    public void unpause(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Unpause is not currently supported in enStratus");
    }
}
