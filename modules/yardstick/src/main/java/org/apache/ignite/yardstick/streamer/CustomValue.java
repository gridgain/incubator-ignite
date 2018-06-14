package org.apache.ignite.yardstick.streamer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

class CustomValue {
    private String str1;
    private String str2;
    private String str3;
    private String str4;
    private String str5;
    private BigDecimal bigDecimal;
    private int int1;
    private String str6;
    private String str7;
    private String str8;
    private List<CustomNestedValue> nestedValueList;
    private LocalDateTime time;

    public CustomValue() { }

    public CustomValue(String str1, String str2, String str3, String str4, String str5,
        BigDecimal bigDecimal, int int1, String str6, String str7, String str8,
        List<CustomNestedValue> nestedValueList, LocalDateTime time) {
        this.str1 = str1;
        this.str2 = str2;
        this.str3 = str3;
        this.str4 = str4;
        this.str5 = str5;
        this.bigDecimal = bigDecimal;
        this.int1 = int1;
        this.str6 = str6;
        this.str7 = str7;
        this.str8 = str8;
        this.nestedValueList = nestedValueList;
        this.time = time;
    }

    public LocalDateTime getTime() {
        return time;
    }
}
