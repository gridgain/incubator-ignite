package org.apache.ignite.internal.configuration.selector;

import org.apache.ignite.internal.configuration.getpojo.Local;
import org.apache.ignite.internal.configuration.initpojo.InitLocal;
import org.apache.ignite.internal.configuration.internalconfig.LocalConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeLocal;

import static org.apache.ignite.internal.configuration.Keys.LOCAL;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class LocalSelector extends Selector<Local, ChangeLocal, InitLocal, LocalConfiguration> {
    public final static BaselineSelector BASELINE = new BaselineSelector();
    public final static BaselineSelector LOCAL_BASELINE = new BaselineSelector();

    public LocalSelector() {
        super(LOCAL);
    }
}
