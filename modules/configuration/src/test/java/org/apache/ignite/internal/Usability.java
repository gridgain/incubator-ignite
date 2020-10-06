package org.apache.ignite.internal;

import org.apache.ignite.internal.configuration.Configurator;
import org.apache.ignite.internal.configuration.selector.AutoAdjustSelector;
import org.apache.ignite.internal.configuration.selector.BaselineSelector;
import org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.configuration.initpojo.InitAutoAdjust.initAutoAdjust;
import static org.apache.ignite.internal.configuration.selector.AutoAdjustSelector.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT;
import static org.apache.ignite.internal.configuration.selector.BaselineSelector.LOCAL_BASELINE_AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.selector.BaselineSelector.LOCAL_BASELINE_NODES;
import static org.apache.ignite.internal.configuration.selector.Selectors.LOCAL;
import static org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust.changeAutoAdjust;
import static org.apache.ignite.internal.configuration.setpojo.ChangeBaseline.changeBaseline;
import static org.apache.ignite.internal.configuration.setpojo.ChangeLocal.changeLocal;
import static org.apache.ignite.internal.configuration.setpojo.ChangeNode.changeNode;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Usability {

    @Test
    public void api() {
        Configurator conf = new Configurator();

        conf.init(LOCAL_BASELINE_AUTO_ADJUST, initAutoAdjust()
            .enabled(false)
            .timeout(1000)
        );

        Long aPublic = conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT);

        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + aPublic);
        System.out.println("NODE.BASELINE.NODES = " + conf.getPublic(LOCAL.BASELINE.NODES));

        conf.set(LOCAL, changeLocal()
            .with(
                changeBaseline()
                    .with(
                        changeAutoAdjust()
                            .enabled(false)
                            .timeout(988)
                    )
                    .with(
                        asList(
                            changeNode("test1")
                                .port(222),

                            changeNode("test2")
                                .port(777)
                        )
                    )
            )
        );

        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT));
        System.out.println("NODE.BASELINE.AUTO_ADJUST.ENABLED = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.ENABLED));

        System.out.println("LOCAL_BASELINE_NODES.NODE1.METRIC" + conf.getPublic(LOCAL.BASELINE.NODES));

        conf.set(LOCAL_BASELINE_NODES, asList(
            changeNode("test1")
                .port(3456),
            changeNode("test2")
                .port(7433)
            )
        );

        System.out.println("NODE.BASELINE.NODES = " + conf.getPublic(LOCAL.BASELINE.NODES));

        conf.set(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT, 4555L);

        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));
        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT Internal = " + conf.getInternal(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT).value());

    }

    @Test
    public void test() {
        Configurator conf = new Configurator();

        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT));
        System.out.println("AutoAdjustSelector.TIMEOUT = " + conf.getPublic(AutoAdjustSelector.TIMEOUT));

        conf.set(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT, 4555L);

        System.out.println("or");

        System.out.println("NODE_BASELINE_AUTO_ADJUST_TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));
        conf.set(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT, 4555L);

        System.out.println("--------------------------");

        System.out.println("NODE.BASELINE.AUTO_ADJUST = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST));
        System.out.println("BaselineSelector.AUTO_ADJUST = " + conf.getPublic(BaselineSelector.AUTO_ADJUST));

        conf.set(LOCAL.BASELINE.AUTO_ADJUST, new ChangeAutoAdjust().timeout(5959).enabled(true));

        System.out.println("or");

        System.out.println("NODE_BASELINE_AUTO_ADJUST = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST));

        conf.set(LOCAL_BASELINE_AUTO_ADJUST, new ChangeAutoAdjust().timeout(7777).enabled(false));

        System.out.println("--------------------------");

        System.out.println("NODE.BASELINE = " + conf.getPublic(LOCAL.BASELINE));

        System.out.println("--------------------------");

        System.out.println("NODE =" + conf.getPublic(LOCAL));

        System.out.println("==================================================");

        conf.set(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT, 4555L);
        System.out.println(conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT));

        System.out.println("--------------------------");

        conf.set(LOCAL.BASELINE.AUTO_ADJUST.ENABLED, true);
        System.out.println(conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.ENABLED));

        System.out.println("--------------------------");

        System.out.println(conf.getPublic(LOCAL));

    }
}
