package org.dasein.cloud.enstratus;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.enstratus.compute.Compute;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
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

    static public final String API_KEY      = "admin/ApiKey";
    static public final String BILLING_CODE = "admin/BillingCode";
    static public final String JOB          = "admin/Job";

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

    public @Nonnull String findBudget() throws CloudException, InternalException {
        EnstratusMethod method = new EnstratusMethod(this);
        APIResponse r = null;
        String any = null;

        do {
            if( r == null ) {
                r = method.get("findBudget", BILLING_CODE, null);
            }
            else {
                r = r.next();
            }
            if( r.getCode() != EnstratusMethod.OK ) {
                throw new CloudException("No error and no response: " + r.getCode());
            }
            JSONObject json = r.getJSON();

            if( json.has("billingCodes") ) {
                try {
                    JSONArray list = json.getJSONArray("billingCodes");

                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject budget = list.getJSONObject(i);

                        if( budget.has("billingCodeId") && budget.has("status") && budget.getString("status").equalsIgnoreCase("active") ) {
                            if( budget.has("budgetState") && !budget.getString("budgetState").equals("HARD_ALARM") ) {
                                if( budget.getString("budgetState").equals("NORMAL") ) {
                                    return budget.getString("billingCodeId");
                                }
                                else if( any == null ) {
                                    any = budget.getString("billingCodeId");
                                }
                            }
                        }

                    }

                }
                catch( JSONException e ) {
                    logger.error("Received invalid JSON from enStratus: " + e.getMessage());
                    throw new CloudException(e);
                }
            }
        } while( !r.isComplete() );
        if( any != null ) {
            return any;
        }
        throw new CloudException("Unable to identify a billable budget to assign to this operation");
    }

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

    public @Nullable String waitForJob(@Nonnull String operation, @Nonnull JSONObject json) throws CloudException, InternalException {
        if( json.has("jobs") ) {
            try {
                JSONObject job = json.getJSONArray("jobs").getJSONObject(0);
                String jobId = null, status = null;

                if( job.has("jobId") ) {
                    jobId = job.getString("jobId");
                }
                if( job.has("status") ) {
                    status = job.getString("status");
                }
                if( jobId == null || status == null ) {
                    throw new CloudException("Invalid job object");
                }
                if( status.equals("COMPLETE") ) {
                    if( job.has("message") ) {
                        return job.getString("message");
                    }
                    else {
                        return null;
                    }
                }
                else if( status.equals("ERROR") ) {
                    if( job.has("message") ) {
                        throw new CloudException(job.getString("message"));
                    }
                    else {
                        throw new CloudException("Job failed without any message");
                    }
                }
                else {
                    try { Thread.sleep(15000L); }
                    catch( InterruptedException ignore ) { }
                    long timeout = System.currentTimeMillis() + (5* CalendarWrapper.MINUTE);

                    while( timeout > System.currentTimeMillis() ) {
                        EnstratusMethod method = new EnstratusMethod(this);
                        APIResponse response = method.get(operation, JOB, jobId);

                        if( response.getCode() == EnstratusMethod.OK ) {
                            return waitForJob(operation, response.getJSON());
                        }
                        else {
                            try { Thread.sleep(15000L); }
                            catch( InterruptedException ignore ) { }
                        }
                    }
                    throw new CloudException("Could not determine job status for job " + jobId);
                }
            }
            catch( JSONException e ) {
                logger.error("Invalid JSON from enStratus: " + e.getMessage());
                throw new CloudException(e);
            }
        }
        throw new CloudException("No jobs in response");
    }
}