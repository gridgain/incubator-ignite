package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.binary.BinaryObject;

public interface Calculator {
    BinaryObject calculate(BinaryObject obj);
}
