package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.Ignition;

public class Server {
    public static void main(String[] args) {
        Ignition.start("examples/config/example-service-interop.xml");
    }
}
