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
package org.dasein.cloud.enstratus.compute;

import org.apache.log4j.Logger;
import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.enstratus.Enstratus;
import org.dasein.cloud.enstratus.Subscription;
import org.dasein.cloud.enstratus.compute.image.MachineImages;
import org.dasein.cloud.enstratus.compute.vm.VM;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;

/**
 * Implementation of compute services for enStratus.
 * <p>Created by George Reese: 11/22/12 9:21 AM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class Compute extends AbstractComputeServices {
    static private final Logger logger = Enstratus.getLogger(Compute.class);

    private Enstratus provider;

    public Compute(@Nonnull Enstratus provider) { this.provider = provider; }

    @Override
    public @Nullable MachineImages getImageSupport() {
        try {
            Subscription s = Subscription.getSubscription(provider);

            if( s.isSubscribedMachineImage() ) {
                return new MachineImages(provider);
            }
            return null;
        }
        catch( Throwable t ) {
            logger.error("Failed to load subscription for this region: " + t.getMessage());
            return null;
        }
    }

    @Override
    public @Nullable VM getVirtualMachineSupport() {
        try {
            Subscription s = Subscription.getSubscription(provider);

            if( s.isSubscribedVirtualMachine() ) {
                return new VM(provider);
            }
            return null;
        }
        catch( Throwable t ) {
            logger.error("Failed to load subscription for this region: " + t.getMessage());
            return null;
        }
    }
}
