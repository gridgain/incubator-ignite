import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.resources.SpringResource;

public class PersonCacheStoreFactory implements Factory<PersonStore>{

//    @SpringResource(resourceName = "ds")
    private transient DataSource dataSource;

    @Override public PersonStore create() {
        return new PersonStore(dataSource);
    }
}
