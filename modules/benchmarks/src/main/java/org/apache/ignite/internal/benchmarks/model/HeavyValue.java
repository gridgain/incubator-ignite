package org.apache.ignite.internal.benchmarks.model;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class HeavyValue {
    /**
     *
     */
    static public HeavyValue generate() {
        Integer i = ThreadLocalRandom.current().nextInt();
        Double d = ThreadLocalRandom.current().nextDouble();
        String s = i.toString();

        return new HeavyValue(
            s, // Indexed
            s+"a", // Indexed
            s+"b", // Indexed
            s+"c", // Indexed
            s+"d", // Indexed
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            s,
            d,
            s,
            d,
            d,
            s,
            s,
            s,
            d,
            s
        );
    }

    String field1;
    String field2;
    String field3;
    String field4;
    String field5;
    String field6;
    String field7;
    String field8;
    String field9;
    String field10;
    String field11;
    Double field12;
    String field13;
    String field14;
    String field15;
    String field16;
    String field17;
    String field18;
    String field19;
    String field20;
    String field21;
    String field22;
    Double field23;
    String field24;
    String field25;
    String field26;
    String field27;
    String field28;
    String field29;
    String field30;
    String field31;
    String field32;
    String field33;
    String field34;
    String field35;
    String field36;
    String field37;
    String field38;
    String field39;
    String field40;
    Double field41;
    String field42;
    Double field43;
    Double field44;
    String field45;
    String field46;
    String field47;
    Double field48;
    String field49;

    /**
     */
    private HeavyValue(String field1, String field2, String field3, String field4,
        String field5, String field6, String field7,
        String field8, String field9, String field10, String field11, Double field12,
        String field13, String field14, String field15, String field16,
        String field17, String field18, String field19, String field20,
        String field21, String field22, Double field23, String field24, String field25,
        String field26, String field27, String field28,
        String field29, String field30, String field31,
        String field32, String field33,
        String field34, String field35, String field36,
        String field37, String field38, String field39, String field40, Double field41,
        String field42, Double field43, Double field44, String field45,
        String field46, String field47, Double field48, String field49) {
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
        this.field5 = field5;
        this.field6 = field6;
        this.field7 = field7;
        this.field8 = field8;
        this.field9 = field9;
        this.field10 = field10;
        this.field11 = field11;
        this.field12 = field12;
        this.field13 = field13;
        this.field14 = field14;
        this.field15 = field15;
        this.field16 = field16;
        this.field17 = field17;
        this.field18 = field18;
        this.field19 = field19;
        this.field20 = field20;
        this.field21 = field21;
        this.field22 = field22;
        this.field23 = field23;
        this.field24 = field24;
        this.field25 = field25;
        this.field26 = field26;
        this.field27 = field27;
        this.field28 = field28;
        this.field29 = field29;
        this.field30 = field30;
        this.field31 = field31;
        this.field32 = field32;
        this.field33 = field33;
        this.field34 = field34;
        this.field35 = field35;
        this.field36 = field36;
        this.field37 = field37;
        this.field38 = field38;
        this.field39 = field39;
        this.field40 = field40;
        this.field41 = field41;
        this.field42 = field42;
        this.field43 = field43;
        this.field44 = field44;
        this.field45 = field45;
        this.field46 = field46;
        this.field47 = field47;
        this.field48 = field48;
        this.field49 = field49;
    }
}