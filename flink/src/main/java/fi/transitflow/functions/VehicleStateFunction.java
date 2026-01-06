package fi.transitflow.functions;

import fi.transitflow.models.EnrichedEvent;
import fi.transitflow.models.StopArrival;
import fi.transitflow.models.VehiclePosition;
import fi.transitflow.models.VehicleState;
import fi.transitflow.utils.ConfigLoader;
import fi.transitflow.utils.GeoUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VehicleStateFunction 
        extends KeyedProcessFunction<Integer, VehiclePosition, EnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(VehicleStateFunction.class);

    public static final OutputTag<StopArrival> STOP_ARRIVAL_TAG = 
            new OutputTag<StopArrival>("stop-arrivals") {};

    private final double stoppedSpeedThreshold;

    private transient ValueState<VehicleState> vehicleState;

    public VehicleStateFunction() {
        this.stoppedSpeedThreshold = ConfigLoader.stoppedSpeedThreshold();
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(
                        Time.minutes(ConfigLoader.stateTtlMinutes()))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<VehicleState> descriptor = 
                new ValueStateDescriptor<>("vehicle-state", VehicleState.class);
        descriptor.enableTimeToLive(ttlConfig);

        vehicleState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            VehiclePosition pos,
            Context ctx,
            Collector<EnrichedEvent> out) throws Exception {

        VehicleState state = vehicleState.value();
        if (state == null) {
            state = new VehicleState(ConfigLoader.delayHistorySize());
        }

        boolean isStopped = pos.getSpeedMs() < stoppedSpeedThreshold;

        double delayTrend = state.getDelayTrend(pos.getDelaySeconds());
        double speedTrend = state.isFirstEvent() ? 0.0 : state.getSpeedTrend(pos.getSpeedMs());

        double distanceSinceLastM = 0.0;
        long timeSinceLastMs = 0;

        if (!state.isFirstEvent()) {
            distanceSinceLastM = GeoUtils.haversineDistance(
                    state.getLastLatitude(), state.getLastLongitude(),
                    pos.getLatitude(), pos.getLongitude());
            timeSinceLastMs = pos.getEventTimeMs() - state.getLastEventTimeMs();
        }

        long stoppedDurationMs = state.getStoppedDurationMs(pos.getEventTimeMs());

        // Detect stop arrival logic - Consistent with boolean door status
        if (state.hasStopChanged(pos.getNextStopId()) && pos.getNextStopId() != null) {
            StopArrival arrival = new StopArrival(
                    pos.getVehicleId(),
                    pos.getNextStopId(),
                    pos.getLineId(),
                    pos.getDirectionId(),
                    pos.getEventTimeMs(),
                    pos.getDelaySeconds(),
                    pos.isDoorOpen(), // Boolean from VehiclePosition
                    pos.getLatitude(),
                    pos.getLongitude()
            );
            ctx.output(STOP_ARRIVAL_TAG, arrival);
        }

        state.update(pos, isStopped);
        vehicleState.update(state);

        EnrichedEvent enriched = EnrichedEvent.builder()
                .fromPosition(pos)
                .delayTrend(delayTrend)
                .speedTrend(speedTrend)
                .distanceSinceLastM(distanceSinceLastM)
                .timeSinceLastMs(timeSinceLastMs)
                .isStopped(isStopped)
                .stoppedDurationMs(stoppedDurationMs)
                .processingTime(System.currentTimeMillis())
                .build();

        out.collect(enriched);
    }
}