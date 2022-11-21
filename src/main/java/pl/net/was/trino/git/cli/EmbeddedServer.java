/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.net.was.trino.git.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.discovery.server.testing.TestingDiscoveryServer;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.metadata.AllNodes;
import io.trino.metadata.FunctionBundle;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.security.SystemAccessControl;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.log.Level.DEBUG;
import static io.airlift.log.Level.ERROR;
import static io.airlift.log.Level.WARN;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EmbeddedServer
        implements Closeable
{
    private static final Logger log = Logger.get(EmbeddedServer.class);
    private static final String ENVIRONMENT = "embedded";
    private final TestingDiscoveryServer discoveryServer;
    private final TestingTrinoServer coordinator;
    private final Runnable registerNewWorker;
    private final List<TestingTrinoServer> servers = new CopyOnWriteArrayList<>();
    private final List<FunctionBundle> functionBundles = new CopyOnWriteArrayList<>(ImmutableList.of());
    private final List<Plugin> plugins = new CopyOnWriteArrayList<>();
    private final Closer closer = Closer.create();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static Builder<?> builder(Session defaultSession)
    {
        return new Builder<>(defaultSession);
    }

    public EmbeddedServer(
            Session defaultSession,
            int nodeCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            String environment,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        setupLogging();

        try {
            long start = System.nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            closer.register(() -> closeUnchecked(discoveryServer));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

            registerNewWorker = () -> createServer(false, extraProperties, environment, baseDataDir, ImmutableList.of(), ImmutableList.of());

            for (int i = 1; i < nodeCount; i++) {
                registerNewWorker.run();
            }

            Map<String, String> extraCoordinatorProperties = new HashMap<>();
            extraCoordinatorProperties.putAll(extraProperties);
            extraCoordinatorProperties.putAll(coordinatorProperties);

            if (!extraCoordinatorProperties.containsKey("web-ui.authentication.type")) {
                // Make it possible to use Trino UI when running multiple tests (or tests and SomeQueryRunner.main) at once.
                // This is necessary since cookies are shared (don't discern port number) and logging into one instance logs you out from others.
                extraCoordinatorProperties.put("web-ui.authentication.type", "fixed");
                extraCoordinatorProperties.put("web-ui.user", "admin");
            }

            coordinator = createServer(true, extraCoordinatorProperties, environment, baseDataDir, systemAccessControls, eventListeners);
        }
        catch (Exception e) {
            try {
                throw closer.rethrow(e, Exception.class);
            }
            finally {
                closer.close();
            }
        }

        waitForAllNodesGloballyVisible();
    }

    private TestingTrinoServer createServer(
            boolean coordinator,
            Map<String, String> extraCoordinatorProperties,
            String environment,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
    {
        TestingTrinoServer server = closer.register(createTestingTrinoServer(
                discoveryServer.getBaseUrl(),
                coordinator,
                extraCoordinatorProperties,
                environment,
                baseDataDir,
                systemAccessControls,
                eventListeners));
        servers.add(server);
        functionBundles.forEach(server::addFunctions);
        plugins.forEach(server::installPlugin);
        return server;
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("Bootstrap", WARN);
        logging.setLevel("org.glassfish", ERROR);
        logging.setLevel("org.eclipse.jetty.server", WARN);
        logging.setLevel("io.trino.plugin.hive.util.RetryDriver", DEBUG);
    }

    private static TestingTrinoServer createTestingTrinoServer(
            URI discoveryUri,
            boolean coordinator,
            Map<String, String> extraProperties,
            String environment,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
    {
        long start = System.nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                // Use few threads in tests to preserve resources on CI
                .put("discovery.http-client.min-threads", "1") // default 8
                .put("exchange.http-client.min-threads", "1") // default 8
                .put("node-manager.http-client.min-threads", "1") // default 8
                .put("exchange.page-buffer-client.max-callback-threads", "5") // default 25
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("distributed-index-joins-enabled", "true");
        if (coordinator) {
            propertiesBuilder.put("node-scheduler.include-coordinator", "true");
            propertiesBuilder.put("join-distribution-type", "PARTITIONED");

            // Use few threads in tests to preserve resources on CI
            propertiesBuilder.put("failure-detector.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("memoryManager.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("scheduler.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("workerInfo.http-client.min-threads", "1"); // default 8
        }
        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.buildOrThrow());
        properties.putAll(extraProperties);

        TestingTrinoServer server = TestingTrinoServer.builder()
                .setCoordinator(coordinator)
                .setProperties(properties)
                .setEnvironment(environment)
                .setDiscoveryUri(discoveryUri)
                .setBaseDataDir(baseDataDir)
                .setSystemAccessControls(systemAccessControls)
                .setEventListeners(eventListeners)
                .build();

        String nodeRole = coordinator ? "coordinator" : "worker";
        log.info("Created %s TestingTrinoServer in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    public void addServers(int nodeCount)
            throws Exception
    {
        for (int i = 0; i < nodeCount; i++) {
            registerNewWorker.run();
        }
        waitForAllNodesGloballyVisible();
    }

    private void waitForAllNodesGloballyVisible()
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            MILLISECONDS.sleep(10);
        }
        log.info("Announced servers in %s", nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private boolean allNodesGloballyVisible()
    {
        for (TestingTrinoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != servers.size())) {
                return false;
            }
        }
        return true;
    }

    public final void close()
    {
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cancelAllQueries()
    {
        QueryManager queryManager = coordinator.getQueryManager();
        for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
            if (queryInfo.getState().isDone()) {
                continue;
            }
            try {
                queryManager.cancelQuery(queryInfo.getQueryId());
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to cancel query");
            }
        }
    }

    private static void closeUnchecked(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public TestingTrinoServer getCoordinator()
    {
        return coordinator;
    }

    public void installPlugin(Plugin plugin)
    {
        plugins.add(plugin);
        long start = System.nanoTime();
        servers.forEach(server -> server.installPlugin(plugin));
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = System.nanoTime();
        coordinator.createCatalog(catalogName, connectorName, properties);
        log.info("Created catalog %s in %s", catalogName, nanosSince(start));
    }

    public static class Builder<SELF extends Builder<?>>
    {
        private Session defaultSession;
        private int nodeCount = 1;
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private String environment = ENVIRONMENT;
        private Optional<Path> baseDataDir = Optional.empty();
        private List<SystemAccessControl> systemAccessControls = ImmutableList.of();
        private List<EventListener> eventListeners = ImmutableList.of();

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public SELF setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = new HashMap<>(extraProperties);
            return self();
        }

        public SELF addExtraProperty(String key, String value)
        {
            this.extraProperties.put(key, value);
            return self();
        }

        public SELF setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = coordinatorProperties;
            return self();
        }

        /**
         * Sets coordinator properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one coordinator property needs to be set.
         */
        public SELF setSingleCoordinatorProperty(String key, String value)
        {
            return setCoordinatorProperties(ImmutableMap.of(key, value));
        }

        public SELF setEnvironment(String environment)
        {
            this.environment = environment;
            return self();
        }

        public SELF setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return self();
        }

        @SuppressWarnings("unchecked")
        protected SELF self()
        {
            return (SELF) this;
        }

        public EmbeddedServer build()
                throws Exception
        {

            return new EmbeddedServer(
                    defaultSession,
                    nodeCount,
                    extraProperties,
                    coordinatorProperties,
                    environment,
                    baseDataDir,
                    systemAccessControls,
                    eventListeners);
        }
    }
}
