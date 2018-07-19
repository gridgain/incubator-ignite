package org.apache.ignite.yardstick.streamer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/** */
public class IgniteCustomStreamerBenchmark extends IgniteAbstractBenchmark {
    private static final String CACHE_NAME = "default";
    private final ArrayList<CustomKey> keys = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite().cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) {
        Map<CustomKey, CustomValue> entries = new HashMap<>();
        for (int i = 0; i < args.batch(); i++) {
            boolean useNewKey = ThreadLocalRandom.current().nextDouble() > args.repeatingKeysPercent() || keys.isEmpty();
            CustomKey key;
            if (useNewKey) {
                key = DataGenerator.randomKey();
                keys.add(key);
            }
            else
                key = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));
            CustomValue val = DataGenerator.randomValue();
            entries.put(key, val);
        }

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
