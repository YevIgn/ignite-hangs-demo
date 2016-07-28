import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.net.InetSocketAddress;
import java.util.Collections;

public class AtomicLongHangsInCacheStoreTest {
    private static final int NODES = 3;
    private static final String FIRST_CACHE = "firstCache";
    private static final String SECOND_CACHE = "secondCache";

    private Ignite root;
    private Ignite[] nodes = new Ignite[NODES];

    @Before
    public void setUp() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = start(cfg, "node-" + i);
        }
        root = nodes[0];

        CacheConfiguration<Object, Object> base = new CacheConfiguration<Object, Object>() {{
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            setCacheMode(CacheMode.PARTITIONED);
            setReadThrough(true);
            setWriteThrough(true);
        }};

        root.createCache(getCache(base, FIRST_CACHE));
        root.createCache(getCache(base, SECOND_CACHE));
    }

    private static <K, V> CacheConfiguration<K, V> getCache(CacheConfiguration<K, V> configuration, String cacheName) {
        CacheConfiguration<K, V> cacheConfiguration = new CacheConfiguration<>(configuration);
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheStoreFactory(new HangingStoreFactory<>());
        return cacheConfiguration;
    }

    @After
    public void tearDown() {
        for (Ignite node : nodes) {
            node.close();
        }
    }

    @Test
    public void atomicLongGetHangsInCacheStoreSessionEndTest() {
        root.compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite localIgnite;

            @Override
            public void run() {
                IgniteCache<Object, Object> firstCache = localIgnite.cache(FIRST_CACHE);
                IgniteCache<Object, Object> secondCache = localIgnite.cache(SECOND_CACHE);

                try (Transaction transaction = localIgnite.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ
                )) {
                    firstCache.put(1, 1);
                    secondCache.put(2, 2);
                    transaction.commit();
                }
            }
        });
    }

    private static Ignite start(IgniteConfiguration cfg, final String gridName) {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.registerAddresses(Collections.singleton(InetSocketAddress.createUnresolved("127.0.0.1", 47500)));
        final TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(finder);

        return Ignition.start(new IgniteConfiguration(cfg) {{
            setGridName(gridName);
            setDiscoverySpi(discoverySpi);
        }});
    }

    private static class HangingStoreFactory<K, V> implements Factory<HangingCacheStore<K, V>> {
        @IgniteInstanceResource
        private transient Ignite ignite;

        @Override
        public HangingCacheStore<K, V> create() {
             return new HangingCacheStore<>(ignite.atomicLong("atomicLong", 0, true));
        }
    }

    private static class HangingCacheStore<K, V> extends CacheStoreAdapter<K, V> {
        private final IgniteAtomicLong atomicLong;

        HangingCacheStore(IgniteAtomicLong atomicLong) {
            this.atomicLong = atomicLong;
        }

        @Override
        public V load(K key) throws CacheLoaderException {
            return null;
        }

        @Override
        public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
        }

        @Override
        public void sessionEnd(boolean commit) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
            }
            atomicLong.get();
        }
    }
}
