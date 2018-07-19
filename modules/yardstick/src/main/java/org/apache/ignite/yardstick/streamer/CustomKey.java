package org.apache.ignite.yardstick.streamer;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/** */
class CustomKey {
    @AffinityKeyMapped
    @QuerySqlField(index = true)
    private String str1;
    @QuerySqlField
    private String str2;
    @QuerySqlField
    private String str3;

    /** */
    public CustomKey() { }

    /** */
    public CustomKey(String str1, String str2, String str3) {
        this.str1 = str1;
        this.str2 = str2;
        this.str3 = str3;
    }
}
