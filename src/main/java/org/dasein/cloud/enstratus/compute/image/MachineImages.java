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
package org.dasein.cloud.enstratus.compute.image;

import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.enstratus.APIResponse;
import org.dasein.cloud.enstratus.DetailLevel;
import org.dasein.cloud.enstratus.Enstratus;
import org.dasein.cloud.enstratus.EnstratusMethod;
import org.dasein.cloud.enstratus.NoContextException;
import org.dasein.cloud.identity.ServiceAction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 11/24/12 10:25 AM</p>
 *
 * @author George Reese
 * @version 2012.02 (bugzid: [FOGBUGZID])
 * @since 2012.02
 */
public class MachineImages implements MachineImageSupport {
    static private final Logger logger = Logger.getLogger(MachineImages.class);

    static public final String MACHINE_IMAGE = "infrastructure/MachineImage";

    private Enstratus provider;

    public MachineImages(@Nonnull Enstratus provider) { this.provider = provider; }

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String bundleVirtualMachine(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void bundleVirtualMachineAsync(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name, @Nonnull AsynchronousTask<String> trackingTask) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull MachineImage captureImage(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void captureImageAsync(@Nonnull ImageCreateOptions options, @Nonnull AsynchronousTask<MachineImage> taskTracker) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nullable MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    @Deprecated
    public @Nullable MachineImage getMachineImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        return getImage(providerImageId);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return getProviderTermForImage(locale, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        switch( cls ) {
            case MACHINE: return "machine image";
            case KERNEL: return "kernel image";
            case RAMDISK: return "ramdisk image";
        }
        return "machine image";
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
    }

    @Override
    public boolean hasPublicLibrary() {
        return false;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Nonnull
    @Override
    public AsynchronousTask<String> imageVirtualMachine(String vmId, String name, String description) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String providerImageId) throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listImageStatus", MACHINE_IMAGE, null, DetailLevel.none,  new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("images") ) {
                try {
                    JSONArray list = json.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        ResourceStatus s = toStatus(list.getJSONObject(i));

                        if( s != null ) {
                            status.add(s);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        return status;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listImages", MACHINE_IMAGE, null, new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("images") ) {
                try {
                    JSONArray list = json.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        MachineImage img = toImage(list.getJSONObject(i));

                        if( img != null ) {
                            images.add(img);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls, @Nonnull String ownedBy) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        EnstratusMethod method = new EnstratusMethod(provider);
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();
        APIResponse r = null;

        do {
            if( r == null ) {
                r = method.get("listImages", MACHINE_IMAGE, null, new BasicNameValuePair("regionId", ctx.getRegionId()));
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("images") ) {
                try {
                    JSONArray list = json.getJSONArray("images");

                    for( int i=0; i<list.length(); i++ ) {
                        MachineImage img = toImage(list.getJSONObject(i));

                        if( img != null && ownedBy.equals(img.getProviderOwnerId()) ) {
                            images.add(img);
                        }
                    }
                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        return listImages(ImageClass.MACHINE);
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            return listImages(ImageClass.MACHINE);
        }
        return listImages(ImageClass.MACHINE, accountId);
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        return Collections.singletonList(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        ArrayList<MachineImageType> types = new ArrayList<MachineImageType>();

        Collections.addAll(types, MachineImageType.values());
        return types;
    }

    private boolean matches(@Nonnull MachineImage image, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) {
        if( architecture != null && !architecture.equals(image.getArchitecture()) ) {
            return false;
        }
        if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
            Platform mine = image.getPlatform();

            if( platform.isWindows() && !mine.isWindows() ) {
                return false;
            }
            if( platform.isUnix() && !mine.isUnix() ) {
                return false;
            }
            if( platform.isBsd() && !mine.isBsd() ) {
                return false;
            }
            if( platform.isLinux() && !mine.isLinux() ) {
                return false;
            }
            if( platform.equals(Platform.UNIX) ) {
                if( !mine.isUnix() ) {
                    return false;
                }
            }
            else if( !platform.equals(mine) ) {
                return false;
            }
        }
        if( keyword != null ) {
            keyword = keyword.toLowerCase();
            if( !image.getDescription().toLowerCase().contains(keyword) ) {
                if( !image.getName().toLowerCase().contains(keyword) ) {
                    if( !image.getProviderMachineImageId().toLowerCase().contains(keyword) ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove(@Nonnull String providerImageId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        return searchImages(null, keyword, platform, architecture, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        if( imageClasses != null ) {
            boolean hasMachine = false;

            for( ImageClass cls : imageClasses ) {
                if( cls.equals(ImageClass.MACHINE) ) {
                    hasMachine = true;
                    break;
                }
            }
            if( !hasMachine ) {
                return Collections.emptyList();
            }
        }
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();
        Iterable<MachineImage> raw;

        if( accountNumber == null ) {
            raw = listImages(ImageClass.MACHINE);
        }
        else {
            raw = listImages(ImageClass.MACHINE, accountNumber);
        }
        for( MachineImage img : raw ) {
            if( matches(img, keyword, platform, architecture) ) {
                images.add(img);
            }
        }
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public void shareMachineImage(@Nonnull String providerImageId, @Nullable String withAccountId, boolean allow) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsCustomImages() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharing() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return false;
    }

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable MachineImage toImage(@Nonnull JSONObject json) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        MachineImage image = new MachineImage();

        image.setImageClass(ImageClass.MACHINE);
        image.setProviderRegionId(ctx.getRegionId());
        try {
            if( json.has("machineImageId") ) {
                image.setProviderMachineImageId(json.getString("machineImageId"));
            }
            if( json.has("status") ) {
                image.setCurrentState(toState(json.getString("status")));
            }
            if( json.has("budget") ) {
                image.setTag("budget", json.getString("budget"));
            }
            if( json.has("agentVersion") ) {
                image.setTag("agentVersion", json.getString("agentVersion"));
            }
            if( json.has("architecture") ) {
                image.setArchitecture(Architecture.valueOf(json.getString("architecture")));
            }
            if( json.has("name") ) {
                image.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                image.setDescription(json.getString("description"));
            }
            if( json.has("software") ) {
                image.setSoftware(json.getString("json"));
            }
            if( json.has("label") ) {
                image.setTag("label", json.getString("label"));
            }
            if( json.has("owningAccount") ) {
                JSONObject ob = json.getJSONObject("owningAccount");

                if( ob.has("accountId") ) {
                    image.setProviderOwnerId(ob.getString("accountId"));
                }
            }
            else if( json.has("owningCloudAccountNumber") ) {
                image.setProviderOwnerId(json.getString("owningCloudAccountNumber"));
            }
            else if( json.has("owningUser") ) {
                JSONObject ob = json.getJSONObject("owningUser");

                if( ob.has("userId") ) {
                    image.setTag("owningUserId", ob.getString("userId"));
                }
            }
            else if( json.has("platform") ) {
                image.setPlatform(Platform.guess(json.getString("platform")));
            }
            else if( json.has("providerId") ) {
                image.setTag("providerId", json.getString("providerId"));
            }
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from enStratus: " + e.getMessage());
            throw new CloudException(e);
        }
        if( image.getProviderMachineImageId() == null ) {
            return null;
        }
        if( image.getName() == null ) {
            image.setName(image.getProviderMachineImageId());
        }
        if( image.getDescription() == null ) {
            image.setDescription(image.getName());
        }
        return image;
    }

    private @Nonnull MachineImageState toState(@Nonnull String status) {
        if( status.equals("ACTIVE") ) {
            return MachineImageState.ACTIVE;
        }
        else if( status.equals("INACTIVE") ) {
            return MachineImageState.DELETED;
        }
        else if( status.equals("PENDING") ) {
            return MachineImageState.PENDING;
        }
        else {
            logger.warn("DEBUG: Unknown machine image state from enStratus: " + status);
            return MachineImageState.PENDING;
        }
    }

    private @Nullable ResourceStatus toStatus(@Nonnull JSONObject json) throws CloudException, InternalException {
        MachineImageState state = MachineImageState.ACTIVE;
        String id = null;

        try {
            if( json.has("machineImageId") ) {
                id = json.getString("machineImageId");
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
}
