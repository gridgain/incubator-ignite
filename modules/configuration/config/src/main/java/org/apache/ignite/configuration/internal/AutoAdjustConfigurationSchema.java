package org.apache.ignite.configuration.internal;

import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.Value;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
@Config
public class AutoAdjustConfigurationSchema {
    @Value
    private long timeout;
    @Value
    private boolean enabled;
}
