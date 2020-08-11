package com.github.streaming.state.generator;

import com.github.streaming.state.event.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static org.apache.flink.util.Preconditions.checkArgument;

@SuppressWarnings("serial")
public class EventsGeneratorSource extends RichParallelSourceFunction<Event> {

    private final double errorProbability;

    private final int delayPerRecordMillis;

    private volatile boolean running = true;

    public EventsGeneratorSource(double errorProbability, int delayPerRecordMillis) {
        checkArgument(errorProbability >= 0.0 && errorProbability <= 1.0, "error probability must be in [0.0, 1.0]");
        checkArgument(delayPerRecordMillis >= 0, "delay must be >= 0");

        this.errorProbability = errorProbability;
        this.delayPerRecordMillis = delayPerRecordMillis;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final EventsGenerator generator = new EventsGenerator(errorProbability);

        final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        final int min = range * getRuntimeContext().getIndexOfThisSubtask();
        final int max = min + range;

        while (running) {
            ctx.collect(generator.next(min, max));

            if (delayPerRecordMillis > 0) {
                Thread.sleep(delayPerRecordMillis);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
