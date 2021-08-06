package org.jackyzeng.demos.generators;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple3;

public class PunctuatedGenerator implements WatermarkGenerator<Tuple3<String, Long, Boolean>> {
    @Override
    public void onEvent(Tuple3<String, Long, Boolean> event, long timestamp, WatermarkOutput output) {
        // Base on specific field to emit watermark
        if (event.f2) {
            output.emitWatermark(new Watermark(event.f1));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // do nothing when emitting punctuated watermark
    }
}
