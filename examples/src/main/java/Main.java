import model.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException {
        Ignite ignite = Ignition.start("ignite.xml");
        ignite.active(true);
        IgniteCache<Long, Person> cache = ignite.cache("Person");
        cache.put(1L, new Person());

    }
}
