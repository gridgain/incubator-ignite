package org.apache.ignite.internal;

import org.apache.ignite.internal.configuration.Configurator;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;
import org.apache.ignite.internal.configuration.setpojo.NList;
import org.junit.Test;

import static java.lang.System.out;
import static org.apache.ignite.internal.configuration.initpojo.InitAutoAdjust.initAutoAdjust;
import static org.apache.ignite.internal.configuration.initpojo.InitBaseline.initBaseline;
import static org.apache.ignite.internal.configuration.initpojo.InitLocal.initLocal;
import static org.apache.ignite.internal.configuration.initpojo.InitNode.initNode;
import static org.apache.ignite.internal.configuration.initpojo.InitNode.initNodes;
import static org.apache.ignite.internal.configuration.selector.Selectors.LOCAL;
import static org.apache.ignite.internal.configuration.selector.Selectors.LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT;
import static org.apache.ignite.internal.configuration.selector.Selectors.LOCAL_BASELINE_NODES;
import static org.apache.ignite.internal.configuration.selector.Selectors.localBaselineNode;
import static org.apache.ignite.internal.configuration.setpojo.ChangeAutoAdjust.changeAutoAdjust;
import static org.apache.ignite.internal.configuration.setpojo.ChangeBaseline.changeBaseline;
import static org.apache.ignite.internal.configuration.setpojo.ChangeLocal.changeLocal;
import static org.apache.ignite.internal.configuration.setpojo.ChangeNode.changeNode;
import static org.apache.ignite.internal.configuration.setpojo.ChangeNode.changeNodes;

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

        conf.init(LOCAL, initLocal()
            .with(
                initBaseline()
                    .with(
                        initAutoAdjust()
                            .enabled(false)
                            .timeout(1000)
                    )
                    .with(
                        initNodes()
                            .add("test1", initNode()
                                .consistentId("constId1")
                                .port(444)
                            )
                            .add("test2", initNode()
                                .consistentId("constId2")
                                .port(776))
                        )
                    )
        );

        out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));
        out.println("NODE.BASELINE.NODES = " + conf.getPublic(LOCAL_BASELINE_NODES));
        out.println("NODE.BASELINE.NODES.test1 = " + conf.getPublic(localBaselineNode("test1")));
        out.println("NODE.BASELINE.NODES.test1.PORT = " + conf.getPublic(localBaselineNode("test1").port()));

        conf.set(LOCAL, changeLocal()
            .with(
                changeBaseline()
                    .with(
                        changeAutoAdjust()
                            .enabled(false)
                            .timeout(988)
                    )
                    .with(
                        NList.<ChangeNode>list()
                            .add("test1", changeNode()
                                .port(222)
                            )
                            .add("test2", changeNode()
                                .port(777)
                            )
                    )
            )
        );

        out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));

        out.println("NODE.BASELINE.NODES = " + conf.getPublic(LOCAL_BASELINE_NODES));
        out.println("NODE.BASELINE.NODES.test1.PORT = " + conf.getPublic(localBaselineNode("test1").port()));
        out.println("NODE.BASELINE.NODES.test2.consistentId = " + conf.getPublic(localBaselineNode("test2").consistentId()));

        conf.set(LOCAL_BASELINE_NODES, changeNodes()
            .add("test1", changeNode()
                .port(5567)
            )
            .add("test2", changeNode()
                .port(554)
            )
        );

        out.println("NODE.BASELINE.NODES = " + conf.getPublic(LOCAL_BASELINE_NODES));

        conf.set(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT, 44L);

        out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));
        out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT Internal = " + conf.getInternal(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT).value());

    }

//    @Test
//    public void test() {
//        Configurator conf = new Configurator();
//
//        System.out.println("NODE.BASELINE.AUTO_ADJUST.TIMEOUT = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT));
//        System.out.println("AutoAdjustSelector.TIMEOUT = " + conf.getPublic(AutoAdjustSelector.TIMEOUT));
//
//        conf.set(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT, 4555L);
//
//        System.out.println("or");
//
//        System.out.println("NODE_BASELINE_AUTO_ADJUST_TIMEOUT = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT));
//        conf.set(LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT, 4555L);
//
//        System.out.println("--------------------------");
//
//        System.out.println("NODE.BASELINE.AUTO_ADJUST = " + conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST));
//        System.out.println("BaselineSelector.AUTO_ADJUST = " + conf.getPublic(BaselineSelector.AUTO_ADJUST));
//
//        conf.set(LOCAL.BASELINE.AUTO_ADJUST, new ChangeAutoAdjust().timeout(5959).enabled(true));
//
//        System.out.println("or");
//
//        System.out.println("NODE_BASELINE_AUTO_ADJUST = " + conf.getPublic(LOCAL_BASELINE_AUTO_ADJUST));
//
//        conf.set(LOCAL_BASELINE_AUTO_ADJUST, new ChangeAutoAdjust().timeout(7777).enabled(false));
//
//        System.out.println("--------------------------");
//
//        System.out.println("NODE.BASELINE = " + conf.getPublic(LOCAL.BASELINE));
//
//        System.out.println("--------------------------");
//
//        System.out.println("NODE =" + conf.getPublic(LOCAL));
//
//        System.out.println("==================================================");
//
//        conf.set(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT, 4555L);
//        System.out.println(conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.TIMEOUT));
//
//        System.out.println("--------------------------");
//
//        conf.set(LOCAL.BASELINE.AUTO_ADJUST.ENABLED, true);
//        System.out.println(conf.getPublic(LOCAL.BASELINE.AUTO_ADJUST.ENABLED));
//
//        System.out.println("--------------------------");
//
//        System.out.println(conf.getPublic(LOCAL));
//
//    }
}
