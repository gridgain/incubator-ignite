package org.apache.ignite.yardstick.streamer;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

class CustomValue {
    private String payload;
    private List<CustomNestedValue> nestedValueList;
    @QuerySqlField
    private LocalDateTime time;

    public CustomValue() { }

    public CustomValue(String payload, List<CustomNestedValue> nestedValueList, LocalDateTime time) {
        this.payload = payload;
        this.nestedValueList = nestedValueList;
        this.time = time;
    }

    public LocalDateTime getTime() {
        return time;
    }
}
