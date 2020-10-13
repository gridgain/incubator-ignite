package org.apache.ignite.configuration.internal;

import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.NamedConfig;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
@Config
public class BaselineConfigurationSchema {
    @Config
    private AutoAdjustConfigurationSchema autoAdjustConfiguration;

    @NamedConfig
    private NodeConfigurationSchema nodes;

}
