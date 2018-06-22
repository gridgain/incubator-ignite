package org.apache.ignite.yardstick.streamer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/** */
public class IgniteCustomStreamerBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static final String CACHE_NAME = "streamer-atomic";

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite().cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) {
        Map<CustomKey, CustomValue> entries = new HashMap<>();
        for (int i = 0; i < args.batch(); i++)
            entries.put(DataGenerator.randomKey(args.range()), DataGenerator.randomValue());

        try (IgniteDataStreamer<CustomKey, CustomValue> dataStreamer = ignite().dataStreamer(CACHE_NAME)) {
            dataStreamer.allowOverwrite(true);
            dataStreamer.receiver(new CustomEntryResolver());
            entries.entrySet().parallelStream().forEach(new Consumer<Map.Entry<CustomKey, CustomValue>>() {
                @Override public void accept(Map.Entry<CustomKey, CustomValue> entry) {
                    dataStreamer.addData(entry);
                }
            });
        }

        return true;
    }
}
