package org.apache.ignite.entitymanager;

import java.util.Map;
import org.apache.ignite.lang.IgniteBiClosure;

/** */
public class GenericEntityManagerSelftTest extends AbstractEntityManagerSelfTest {
    /** {@inheritDoc} */
    @Override protected EntityManager<Long, TestUser> mgr(int parts, String name,
        Map<String, IgniteBiClosure<StringBuilder, Object, String>> indices, IdGenerator<Long> gen) {
        return new EntityManager<>(parts, name, indices, gen);
    }
}
