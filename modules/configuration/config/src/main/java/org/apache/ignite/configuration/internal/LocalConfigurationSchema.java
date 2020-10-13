package org.apache.ignite.configuration.internal;

import org.apache.ignite.configuration.internal.annotation.Config;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
@Config("local")
public class LocalConfigurationSchema {

    @Config
    private BaselineConfigurationSchema baselineConfiguration;

}
