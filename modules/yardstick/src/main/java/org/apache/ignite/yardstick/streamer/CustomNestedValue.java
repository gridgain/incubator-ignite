package org.apache.ignite.yardstick.streamer;

import java.math.BigDecimal;
import java.time.LocalDate;

class CustomNestedValue {
    private LocalDate date;
    private BigDecimal bigDecimal;
    private String nestedStr;
    private long long1;

    public CustomNestedValue() { }

    public CustomNestedValue(LocalDate date, BigDecimal bigDecimal, String nestedStr, long long1) {
        this.date = date;
        this.bigDecimal = bigDecimal;
        this.nestedStr = nestedStr;
        this.long1 = long1;
    }
}
