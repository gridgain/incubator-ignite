package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

public class ComplexTypeHandlerService implements Service, ComplexTypeHandler {
    @Override public ComplexType handle(ComplexType obj) {
        return new ComplexType()
            .setI(obj.getI() * 2);
    }

    @Override public void cancel(ServiceContext ctx) {
    }

    @Override public void init(ServiceContext ctx) throws Exception {
    }

    @Override public void execute(ServiceContext ctx) throws Exception {
    }
}
