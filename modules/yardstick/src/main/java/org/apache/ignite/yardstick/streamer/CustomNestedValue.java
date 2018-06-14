package org.apache.ignite.yardstick.streamer;

import java.math.BigDecimal;
import java.time.LocalDate;

class CustomNestedValue {
    private LocalDate date;
    private BigDecimal bigDecimal;
    private String str1;
    private long long1;
    private long long2;
    private int int1;
    private String str2;

    public CustomNestedValue() { }

    public CustomNestedValue(LocalDate date, BigDecimal bigDecimal, String str1, long long1, long long2, int int1,
        String str2) {
        this.date = date;
        this.bigDecimal = bigDecimal;
        this.str1 = str1;
        this.long1 = long1;
        this.long2 = long2;
        this.int1 = int1;
        this.str2 = str2;
    }
}
