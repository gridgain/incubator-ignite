package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

public class CalculatorService implements Service, Calculator {
    @IgniteInstanceResource Ignite ignite;

    @Override public Model calculate(Model mdl) {
        mdl.setResults(new Result[] {new Result("apr", 1.2), new Result("sum", 1000)});

        return mdl;
    }

    @Override public void cancel(ServiceContext ctx) {
    }

    @Override public void init(ServiceContext ctx) throws Exception {
    }

    @Override public void execute(ServiceContext ctx) throws Exception {
    }
}
