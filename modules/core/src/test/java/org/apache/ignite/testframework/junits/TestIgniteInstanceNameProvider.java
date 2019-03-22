package org.apache.ignite.testframework.junits;

public class TestIgniteInstanceNameProvider {

    private final Class<?> testClass;

    public TestIgniteInstanceNameProvider(Class<?> aClass) {
        testClass = aClass;
    }

    /**
     * @return Generated unique test Ignite instance name.
     */
    public String getTestIgniteInstanceName() {
        String[] parts = testClass.getName().split("\\.");

        return parts[parts.length - 2] + '.' + parts[parts.length - 1];
    }


    /**
     * @param idx Index of the Ignite instance.
     * @return Indexed Ignite instance name.
     */
    public String getTestIgniteInstanceName(int idx) {
        return getTestIgniteInstanceName() + idx;
    }

    public boolean instanceNameWasGeneratedByProvider(String instanceName) {
        return instanceName.startsWith(getTestIgniteInstanceName());
    }

    /**
     * Parses test Ignite instance index from test Ignite instance name.
     *
     * @param testIgniteInstanceName Test Ignite instance name, returned by {@link #getTestIgniteInstanceName(int)}.
     * @return Test Ignite instance index.
     */
    public int getTestIgniteInstanceIndex(String testIgniteInstanceName) {
        return Integer.parseInt(testIgniteInstanceName.substring(getTestIgniteInstanceName().length()));
    }


}
