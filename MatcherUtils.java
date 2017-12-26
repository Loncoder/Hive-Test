package tv.freewheel.reporting.matcher;

import com.google.common.base.Joiner;
import report.LogRecord;
import tv.freewheel.reporting.util.Utils;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lhan on 6/23/17.
 */
public class MatcherUtils
{
    private static final long INVALID_SEQUENCE = -1;

    private static final String EVENT_TYPE_IMPRESSION = "i";
    private static final String EVENT_NAME_VIDEO_VIEW = "videoView";
    private static final String EVENT_NAME_DEFAULT_IMPRESSION = "defaultImpression";

    private static final long FAKE_ACK_FLAGS = LogRecord.Callback_Log_Record.Flag.GENERATED_VALUE
            | LogRecord.Callback_Log_Record.Flag.FIRST_CALL_VALUE
            | LogRecord.Callback_Log_Record.Flag.IMPLICIT_VALUE;

    public static List<LogRecord.Callback_Log_Record> fakeAcks(LogRecord.Request_Log_Record request)
    {
        List<LogRecord.Callback_Log_Record> fakedAcks = new ArrayList<>();
        if (isImplicitVideoView(request.getFlags())) {
            LogRecord.Identifier identifier = fakeIdentifier(EVENT_NAME_VIDEO_VIEW, request.getTransactionId(), request.getServerId(), 0);

            LogRecord.Callback_Log_Record.Builder ackBuilder = LogRecord.Callback_Log_Record.newBuilder();
            ackBuilder.setServerId(request.getServerId());
            ackBuilder.setTransactionId(request.getTransactionId());
            ackBuilder.setTimestamp(request.getTimestamp());
            ackBuilder.setEventType(EVENT_TYPE_IMPRESSION);
            ackBuilder.setEventName(EVENT_NAME_VIDEO_VIEW);
            ackBuilder.setFlags(fakeAckFlag(requestHasReuseFlag(request.getFlags())));
            ackBuilder.setIdentifier(identifier);
            ackBuilder.setProcessTimestamp(request.getProcessTimestamp());
            ackBuilder.setNetworks(request.getNetworks());

            fakedAcks.add(ackBuilder.build());
        }

        boolean isPrefetch = requestIsPrefetch(request.getFlags());
        boolean hasTracking = requestHasTracking(request.getFlags());
        boolean isTracking = requestIsTracking(request.getFlags());
        if (!isPrefetch || (hasTracking && !isTracking)) {
            for (int i = 0; i < request.getAdvertisementList().size(); i++) {
                LogRecord.Request_Log_Record.Advertisement ad = request.getAdvertisementList().get(i);
                if ((ad.getFlags() & (LogRecord.Request_Log_Record.Advertisement.Flag.PREFETCH_VALUE | LogRecord.Request_Log_Record.Advertisement.Flag.HYLDA_PLACEHOLDER_VALUE)) != 0) {
                    continue;
                }
                if (ad.getSlotIndex() >= request.getSlotCount() || ad.getSlotIndex() < 0) {
                    continue;
                }

                LogRecord.Identifier identifier = fakeIdentifier(EVENT_NAME_DEFAULT_IMPRESSION, request.getTransactionId(), request.getServerId(), i);

                List<String> resellers = new ArrayList<>();
                for (LogRecord.Request_Log_Record.Partner partner : ad.getResellerList()) {
                    resellers.add(String.valueOf(Utils.unmask(partner.getNetworkId())));
                }

                LogRecord.Callback_Log_Record.Builder ackBuilder = LogRecord.Callback_Log_Record.newBuilder();
                ackBuilder.setServerId(request.getServerId());
                ackBuilder.setTransactionId(request.getTransactionId());
                ackBuilder.setTimestamp(request.getTimestamp());

                // set ad related values
                ackBuilder.setAdId(ad.getAdId());
                ackBuilder.setAdReplicaId(ad.getAdReplicaId());
                ackBuilder.setSlotId(ad.getSlotIndex());
                ackBuilder.setEventType(EVENT_TYPE_IMPRESSION);
                ackBuilder.setEventName(EVENT_NAME_DEFAULT_IMPRESSION);
                ackBuilder.setFlags(fakeAckFlag(requestHasReuseFlag(request.getFlags())));
                ackBuilder.setIdentifier(identifier);
                ackBuilder.setProcessTimestamp(request.getProcessTimestamp());
                ackBuilder.setResellerNetworks(Joiner.on(";").join(resellers));
                ackBuilder.setNetworks(request.getNetworks());

                fakedAcks.add(ackBuilder.build());
            }
        }
        return fakedAcks;
    }

    public static long fakeAckFlag(boolean reuse)
    {
        long flag = FAKE_ACK_FLAGS;
        if (reuse) {
            flag |= LogRecord.Callback_Log_Record.Flag.REUSE_RESPONSE_VALUE;
        }
        return flag;
    }

    public static LogRecord.Identifier fakeIdentifier(String eventName, String transactionId, String serverId, long sequence)
    {
        return buildIdentifier(String.format("generated-%s-%s-%s", eventName, transactionId, serverId), sequence);
    }

    public static boolean isImplicitVideoView(long flags)
    {
        return (flags & LogRecord.Request_Log_Record.Flag.LOG_VIDEO_VIEW_VALUE) != 0;
    }

    public static boolean requestIsPrefetch(long flags)
    {
        return (flags & LogRecord.Request_Log_Record.Flag.PREFETCH_VALUE) != 0;
    }

    public static boolean requestHasTracking(long flags)
    {
        return (flags & LogRecord.Request_Log_Record.Flag.HAS_TRACKING_VALUE) != 0;
    }

    public static boolean requestIsTracking(long flags)
    {
        return (flags & LogRecord.Request_Log_Record.Flag.TRACKING_VALUE) != 0;
    }

    public static long getFirstRequestFlags(long flags)
    {
        return flags | LogRecord.Request_Log_Record.Flag.PRIMARY_REQUEST_VALUE;
    }

    public static boolean requestHasReuseFlag(long flags)
    {
        return (flags & LogRecord.Request_Log_Record.Flag.REUSE_RESPONSE_VALUE) != 0;
    }

    public static LogRecord.Identifier buildIdentifier(String source, String sequenceString)
    {
        long sequence = INVALID_SEQUENCE;
        try {
            sequence = Long.parseLong(sequenceString);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return buildIdentifier(source, sequence);
    }

    public static LogRecord.Identifier buildIdentifier(String source, long sequence)
    {
        LogRecord.Identifier.Builder builder = LogRecord.Identifier.newBuilder();
        builder.setSource(source);
        builder.setSequence(sequence);
        return builder.build();
    }
}
