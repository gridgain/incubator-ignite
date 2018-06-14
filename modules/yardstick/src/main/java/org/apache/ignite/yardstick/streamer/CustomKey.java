package org.apache.ignite.yardstick.streamer;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/** */
class CustomKey {
    /** */

    private String str1;

    /** */
    private String str2;

    /** */
    private String str3;

    /** */
    @AffinityKeyMapped
    private int int1;

    /** */
    public CustomKey() { }

    /** */
    public CustomKey(String str1, String str2, String str3, int int1) {
        this.str1 = str1;
        this.str2 = str2;
        this.str3 = str3;
        this.int1 = int1;
    }
}
