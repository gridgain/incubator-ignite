package org.apache.ignite.internal.configuration.selector;

import org.apache.ignite.internal.configuration.getpojo.Baseline;
import org.apache.ignite.internal.configuration.initpojo.InitBaseline;
import org.apache.ignite.internal.configuration.internalconfig.BaselineConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeBaseline;

import static org.apache.ignite.internal.configuration.Keys.LOCAL_BASELINE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class BaselineSelector extends Selector<Baseline, ChangeBaseline, InitBaseline, BaselineConfiguration>{
    public final static AutoAdjustSelector AUTO_ADJUST = new AutoAdjustSelector();
    public final static AutoAdjustSelector LOCAL_BASELINE_AUTO_ADJUST = new AutoAdjustSelector();
    public final static NodeSelector NODES = new NodeSelector();
    public final static NodeSelector LOCAL_BASELINE_NODES = new NodeSelector();

    public BaselineSelector() {
        super(LOCAL_BASELINE);
    }
}
