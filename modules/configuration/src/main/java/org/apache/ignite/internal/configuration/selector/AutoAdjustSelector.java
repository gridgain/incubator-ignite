package org.apache.ignite.internal.configuration.selector;

import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.AutoAdjust;
import org.apache.ignite.internal.configuration.initpojo.InitAutoAdjust;
import org.apache.ignite.internal.configuration.internalconfig.AutoAdjustConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.DynamicProperty;
import org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust;

import static org.apache.ignite.internal.configuration.Keys.LOCAL_BASELINE_AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.Keys.LOCAL_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class AutoAdjustSelector extends Selector<AutoAdjust, ChangeAutoAdjust, InitAutoAdjust, AutoAdjustConfiguration> {
    public static final SameSelector<Long> TIMEOUT = new SameSelector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT);
    public static final Selector<Long, Long, Long, DynamicProperty<Long>> LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT = new Selector<>(Keys.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT);
    public static final SameSelector<Boolean> ENABLED = new SameSelector<>(LOCAL_BASELINE_AUTO_ADJUST_ENABLED);

    public AutoAdjustSelector() {
        super(LOCAL_BASELINE_AUTO_ADJUST);
    }
}
