package org.dasein.cloud.enstratus;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.enstratus.compute.Compute;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Support for the enStratus API through Dasein Cloud. Because enStratus is not itself a cloud, but instead a
 * management layer, this one Dasein Cloud package can be used to interact with any number of clouds using a
 * single model.
 * <p>Created by George Reese: 11/22/12 4:33 PM</p>
 * @author George Reese
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class Enstratus extends AbstractCloud {
    static private final Logger logger = getLogger(Enstratus.class);

    static public final String API_KEY = "admin/ApiKey";

    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');

        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx + 1);
    }

    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());

        if( pkg.equals("enstratus") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.enstratus.std." + pkg + getLastItem(cls.getName()));
    }

    static public @Nonnull Logger getWireLogger(@Nonnull Class<?> cls) {
        return Logger.getLogger("dasein.cloud.enstratus.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }

    public Enstratus() { }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloudName());

        return (name == null ? "enStratus" : name);
    }

    @Override
    public @Nonnull Compute getComputeServices() {
        return new Compute(this);
    }

    @Override
    public @Nonnull DataCenters getDataCenterServices() {
        return new DataCenters(this);
    }

    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getProviderName());

        return (name == null ? "enStratus" : name);
    }

    public @Nonnegative long parseTimestamp(@Nullable String date) throws CloudException {
        if( date == null || date.equals("") ) {
            return 0L;
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        try {
            return fmt.parse(date).getTime();
        }
        catch( ParseException e ) {
            throw new CloudException("Could not parse date: " + date);
        }
    }

    @Override
    public @Nullable String testContext() {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + Enstratus.class.getName() + ".testContext()");
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                logger.warn("No context was provided for testing");
                return null;
            }
            try {
                String accessKey = new String(ctx.getAccessPublic(), "utf-8");
                EnstratusMethod method = new EnstratusMethod(this);
                APIResponse r = method.get("testContext", API_KEY, accessKey);

                if( r.getCode() != EnstratusMethod.OK || r.getJSON() == null ) {
                    return null;
                }
                if( r.getJSON().has("account") ) {
                    JSONObject account = r.getJSON().getJSONObject("account");

                    if( account.has("accountId") ) {
                        return account.getString("accountId");
                    }
                }
                logger.warn("JSON has no account info");
                return null;
            }
            catch( Throwable t ) {
                logger.error("Error querying API key: " + t.getMessage());
                t.printStackTrace();
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + Enstratus.class.getName() + ".textContext()");
            }
        }
    }
}