import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import model.Person;
import org.apache.ignite.cache.store.CacheStoreAdapter;

public class PersonStore extends CacheStoreAdapter<Long, Person> {

    private DataSource ds;

    public PersonStore(DataSource ds) {
        this.ds = ds;
    }

    @Override public Person load(Long key) throws CacheLoaderException {
        System.out.println("Loading by key:"+key);
        //ds.getConnection().prepareStatement("")
        return null;
    }

    @Override public void write(Cache.Entry<? extends Long, ? extends Person> entry) throws CacheWriterException {
        System.out.println("Write entry:"+entry);
        //ds.getConnection().prepareStatement("")
    }

    @Override public void delete(Object key) throws CacheWriterException {
        System.out.println("Delete entry:"+key);
        //ds.getConnection().prepareStatement("")
    }
}
