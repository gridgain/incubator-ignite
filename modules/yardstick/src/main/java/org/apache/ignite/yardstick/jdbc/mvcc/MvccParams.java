package org.apache.ignite.yardstick.jdbc.mvcc;

import com.beust.jcommander.Parameter;

public class MvccParams {
    /** */
    @Parameter (names = {"--run-vacuum"})
    private boolean runVacuum;

    @Parameter(names = {"--table-template"})
    private String tabTpl;


    /**
     * What cache to use as template for test table.
     */
    public String tableTemplate() {
        return tabTpl;
    }

    /**
     * Whether or not to run vacuum job when data upload is done.
     */
    public boolean runVacuum() {
        return runVacuum;
    }
}
