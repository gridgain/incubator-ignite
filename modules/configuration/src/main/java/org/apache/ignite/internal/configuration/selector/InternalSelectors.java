package org.apache.ignite.internal.configuration.selector;

import java.util.List;
import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.AutoAdjust;
import org.apache.ignite.internal.configuration.getpojo.Baseline;
import org.apache.ignite.internal.configuration.getpojo.Local;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.initpojo.InitAutoAdjust;
import org.apache.ignite.internal.configuration.initpojo.InitBaseline;
import org.apache.ignite.internal.configuration.initpojo.InitLocal;
import org.apache.ignite.internal.configuration.initpojo.InitNode;
import org.apache.ignite.internal.configuration.internalconfig.AutoAdjustConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.BaselineConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.DynamicProperty;
import org.apache.ignite.internal.configuration.internalconfig.LocalConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.NodeConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust;
import org.apache.ignite.internal.configuration.setpojo.ChangeBaseline;
import org.apache.ignite.internal.configuration.setpojo.ChangeLocal;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface InternalSelectors {
    Selector<Local, ChangeLocal, InitLocal, LocalConfiguration> LOCAL = new Selector<>(Keys.LOCAL);
    Selector<Baseline, ChangeBaseline, InitBaseline, BaselineConfiguration> LOCAL_BASELINE = new Selector<>(Keys.LOCAL_BASELINE);
    Selector<AutoAdjust, ChangeAutoAdjust, InitAutoAdjust, AutoAdjustConfiguration> LOCAL_BASELINE_AUTO_ADJUST = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST);
    Selector<Node, List<ChangeNode>, List<InitNode>, NodeConfiguration> LOCAL_BASELINE_NODES = new Selector<>(Keys.LOCAL_BASELINE_NODES);

    Selector<Integer, Integer, Integer, DynamicProperty<Integer>> LOCAL_BASELINE_NODES_PORT = new Selector<>(Keys.LOCAL_BASELINE_NODES_PORT);
    Selector<String, String, String, DynamicProperty<String>> LOCAL_BASELINE_NODES_CONSISTENT_ID = new Selector<>(Keys.LOCAL_BASELINE_NODES_CONSISTENT_ID);

    Selector<Long, Long, Long, DynamicProperty<Long>> LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT);
    Selector<Boolean, Boolean, Boolean, DynamicProperty<Boolean>> LOCAL_BASELINE_AUTO_ADJUST_ENABLED = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_ENABLED);
}
