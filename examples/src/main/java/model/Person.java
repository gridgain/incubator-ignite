package model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Person {

    @QuerySqlField
    private Long orgId;

    @QuerySqlField
    public String firstName;

    @QuerySqlField
    public String lastName;

    @QuerySqlField
    public String resume;

    @QuerySqlField
    public Double salary;
}
