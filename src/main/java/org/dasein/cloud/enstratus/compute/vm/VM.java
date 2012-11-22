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
import org.dasein.cloud.enstratus.compute.DetailLevel;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLANSupport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
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

    static public final String SERVER = "infrastructure/Server";

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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
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

    private @Nullable VirtualMachine toVirtualMachine(@Nonnull JSONObject json) throws CloudException, InternalException {
        VirtualMachine vm = new VirtualMachine();

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
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
