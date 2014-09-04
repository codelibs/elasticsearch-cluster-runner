package org.codelibs.elasticsearch.runner;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * ElasticsearchClusterRunner manages multiple Elasticsearch instances.
 *
 * @author shinsuke
 *
 */
public class ElasticsearchClusterRunner {
    private static final ESLogger logger = Loggers
            .getLogger(ElasticsearchClusterRunner.class);

    protected static final String LOGGING_YAML = "logging.yml";

    protected static final String ELASTICSEARCH_YAML = "elasticsearch.yml";

    protected static final String WORK_DIR = "work";

    protected static final String DATA_DIR = "data";

    protected static final String LOGS_DIR = "logs";

    protected static final String PLUGINS_DIR = "plugins";

    protected static final String CONFIG_DIR = "config";

    protected List<Node> nodeList = new ArrayList<>();

    protected int maxHttpPort = 9299;

    protected int maxTransportPort = 9399;

    @Option(name = "-basePath", usage = "Base path for Elasticsearch.")
    protected String basePath;

    @Option(name = "-numOfNode", usage = "The number of Elasticsearch node.")
    protected int numOfNode = 3;

    @Option(name = "-baseTransportPort", usage = "Base transport port.")
    protected int baseTransportPort = 9300;

    @Option(name = "-baseHttpPort", usage = "Base http port.")
    protected int baseHttpPort = 9200;

    @Option(name = "-clusterName", usage = "Cluster name.")
    protected String clusterName = "elasticsearch-cluster-runner";

    @Option(name = "-indexStoreType", usage = "Index store type.")
    protected String indexStoreType = "default";

    @Option(name = "-useStdOut", usage = "Print logs to stdout.")
    protected boolean useStdOut = true;

    @Option(name = "-throwOnFailure", usage = "Throw an exception on a failure.")
    protected boolean throwOnFailure = true;

    protected Builder builder;

    public static void main(final String[] args) {
        final ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                runner.close();
            }
        });

        runner.build(args);

        while (true) {
            if (runner.isClosed()) {
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (final InterruptedException e) {
                // no-op
            }
        }
    }

    public ElasticsearchClusterRunner() {
    }

    public boolean isClosed() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return false;
            }
        }
        return true;
    }

    public void close() {
        for (final Node node : nodeList) {
            node.close();
        }
        print("Closed all nodes.");
    }

    public void clean() {
        final Path bPath = FileSystems.getDefault().getPath(basePath);
        try {
            Files.walkFileTree(bPath, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir,
                        final BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file,
                        final BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return checkIfExist(file);
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file,
                        final IOException exc) throws IOException {
                    throw exc;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir,
                        final IOException exc) throws IOException {
                    if (exc == null) {
                        Files.delete(dir);
                        if (!Files.exists(dir)) {
                            return FileVisitResult.CONTINUE;
                        } else {
                            throw new IOException();
                        }
                    } else {
                        throw exc;
                    }
                }

                private FileVisitResult checkIfExist(final Path path)
                        throws IOException {
                    if (Files.exists(path)) {
                        throw new IOException("Failed to delete " + path);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            print("Deleted " + basePath);
        } catch (final IOException e) {
            print("Failed to delete " + basePath);
        }
    }

    public ElasticsearchClusterRunner onBuild(final Builder builder) {
        this.builder = builder;
        return this;
    }

    public void build(final String... args) {
        if (args != null) {
            final CmdLineParser parser = new CmdLineParser(this);
            parser.setUsageWidth(80);

            try {
                parser.parseArgument(args);
            } catch (final CmdLineException e) {
                throw new ClusterRunnerException("Failed to parse args: "
                        + Strings.arrayToDelimitedString(args, " "));
            }
        }

        if (basePath == null) {
            try {
                basePath = Files.createTempDirectory("es-cluster")
                        .toAbsolutePath().toString();
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create $ES_HOME.",
                        e);
            }
        }

        final Path esBasePath = Paths.get(basePath);
        createDir(esBasePath);

        final Path confPath = Paths.get(basePath, CONFIG_DIR);
        createDir(confPath);

        final Path esConfPath = confPath.resolve(ELASTICSEARCH_YAML);
        if (!Files.exists(esConfPath)) {
            try (InputStream is = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(CONFIG_DIR + "/" + ELASTICSEARCH_YAML)) {
                Files.copy(is, esConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create: "
                        + esConfPath, e);
            }
        }

        final Path logConfPath = confPath.resolve(LOGGING_YAML);
        if (!Files.exists(logConfPath)) {
            try (InputStream is = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(CONFIG_DIR + "/" + LOGGING_YAML)) {
                Files.copy(is, logConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create: "
                        + logConfPath, e);
            }
        }

        print("----------------------------------------");
        print("Cluster Name: " + clusterName);
        print("Base Path:    " + basePath);
        print("Num Of Node:  " + numOfNode);
        print("----------------------------------------");

        for (int i = 0; i < numOfNode; i++) {
            nodeList.add(buildNode(i + 1));
        }
    }

    protected Node buildNode(final int number) {
        final Path confPath = Paths.get(basePath, CONFIG_DIR);
        final Path pluginsPath = Paths.get(basePath, PLUGINS_DIR);
        final Path logsPath = Paths.get(basePath, LOGS_DIR, "node_" + number);
        final Path dataPath = Paths.get(basePath, DATA_DIR, "node_" + number);
        final Path workPath = Paths.get(basePath, WORK_DIR, "node_" + number);

        createDir(logsPath);
        createDir(dataPath);
        createDir(workPath);

        final ImmutableSettings.Builder settingsBuilder = settingsBuilder();

        if (builder != null) {
            builder.build(number, settingsBuilder);
        }

        putIfAbsent(settingsBuilder, "path.conf", confPath.toAbsolutePath()
                .toString());
        putIfAbsent(settingsBuilder, "path.data", dataPath.toAbsolutePath()
                .toString());
        putIfAbsent(settingsBuilder, "path.work", workPath.toAbsolutePath()
                .toString());
        putIfAbsent(settingsBuilder, "path.logs", logsPath.toAbsolutePath()
                .toString());
        putIfAbsent(settingsBuilder, "path.plugins", pluginsPath
                .toAbsolutePath().toString());

        final String nodeName = "Node " + number;
        final int transportPort = getTransportPort(number);
        final int httpPort = getHttpPort(number);
        putIfAbsent(settingsBuilder, "cluster.name", clusterName);
        putIfAbsent(settingsBuilder, "node.name", nodeName);
        putIfAbsent(settingsBuilder, "node.master", String.valueOf(true));
        putIfAbsent(settingsBuilder, "node.data", String.valueOf(true));
        putIfAbsent(settingsBuilder, "http.enabled", String.valueOf(true));
        putIfAbsent(settingsBuilder, "transport.tcp.port",
                String.valueOf(transportPort));
        putIfAbsent(settingsBuilder, "http.port", String.valueOf(httpPort));
        putIfAbsent(settingsBuilder, "index.store.type", indexStoreType);

        print("Node Name:      " + nodeName);
        print("HTTP Port:      " + httpPort);
        print("Transport Port: " + transportPort);
        print("Data Directory: " + dataPath);
        print("Log Directory:  " + logsPath);
        print("----------------------------------------");

        final Settings settings = settingsBuilder.build();
        LogConfigurator.configure(settings);
        final Node node = new InternalNode(settings, true);
        node.start();
        return node;
    }

    private int getHttpPort(final int number) {
        int httpPort = baseHttpPort + number;
        if (maxHttpPort < 0) {
            return httpPort;
        }
        while (httpPort <= maxHttpPort) {
            try (Socket socket = new Socket("localhost", httpPort)) {
                httpPort++;
            } catch (final ConnectException e) {
                return httpPort;
            } catch (final IOException e) {
                print(e.getMessage());
                httpPort++;
            }
        }
        throw new ClusterRunnerException("The http port " + httpPort
                + " is unavailable.");
    }

    protected int getTransportPort(final int number) {
        int transportPort = baseTransportPort + number;
        if (maxTransportPort < 0) {
            return transportPort;
        }
        while (transportPort <= maxTransportPort) {
            try (Socket socket = new Socket("localhost", transportPort)) {
                transportPort++;
            } catch (final ConnectException e) {
                return transportPort;
            } catch (final IOException e) {
                print(e.getMessage());
                transportPort++;
            }
        }
        throw new ClusterRunnerException("The transport port " + transportPort
                + " is unavailable.");
    }

    protected void putIfAbsent(final ImmutableSettings.Builder settingsBuilder,
            final String key, final String value) {
        if (settingsBuilder.get(key) == null && value != null) {
            settingsBuilder.put(key, value);
        }
    }

    public void setMaxHttpPort(final int maxHttpPort) {
        this.maxHttpPort = maxHttpPort;
    }

    public void setMaxTransportPort(final int maxTransportPort) {
        this.maxTransportPort = maxTransportPort;
    }

    public Node getNode(final int i) {
        return nodeList.get(i);
    }

    public Node getNode(final String name) {
        if (name == null) {
            return null;
        }
        for (final Node node : nodeList) {
            if (name.equals(node.settings().get("name"))) {
                return node;
            }
        }
        return null;
    }

    public int getNodeSize() {
        return nodeList.size();
    }

    protected void print(final String line) {
        if (useStdOut) {
            System.out.println(line);
        } else {
            logger.info(line);
        }
    }

    protected void createDir(final Path path) {
        if (!Files.exists(path)) {
            print("Creating " + path);
            try {
                Files.createDirectories(path);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Failed to create " + path, e);
            }
        }
    }

    public Node node() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return node;
            }
        }
        throw new ClusterRunnerException("All nodes are closed.");
    }

    public synchronized Node masterNode() {
        final ClusterState state = client().admin().cluster().prepareState()
                .execute().actionGet().getState();
        final String name = state.nodes().masterNode().name();
        return getNode(name);
    }

    public synchronized Node nonMasterNode() {
        final ClusterState state = client().admin().cluster().prepareState()
                .execute().actionGet().getState();
        final String name = state.nodes().masterNode().name();
        for (final Node node : nodeList) {
            if (!node.isClosed() && !name.equals(node.settings().get("name"))) {
                return node;
            }
        }
        return null;
    }

    public Client client() {
        return node().client();
    }

    public AdminClient admin() {
        return client().admin();
    }

    public ClusterHealthStatus ensureGreen(final String... indices) {
        final ClusterHealthResponse actionGet = client()
                .admin()
                .cluster()
                .health(Requests.clusterHealthRequest(indices)
                        .waitForGreenStatus().waitForEvents(Priority.LANGUID)
                        .waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            onFailure("ensureGreen timed out, cluster state:\n"
                    + client().admin().cluster().prepareState().get()
                            .getState().prettyPrint()
                    + "\n"
                    + client().admin().cluster().preparePendingClusterTasks()
                            .get().prettyPrint(), actionGet);
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus ensureYellow(final String... indices) {
        final ClusterHealthResponse actionGet = client()
                .admin()
                .cluster()
                .health(Requests.clusterHealthRequest(indices)
                        .waitForRelocatingShards(0).waitForYellowStatus()
                        .waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            onFailure("ensureYellow timed out, cluster state:\n"
                    + "\n"
                    + client().admin().cluster().prepareState().get()
                            .getState().prettyPrint()
                    + "\n"
                    + client().admin().cluster().preparePendingClusterTasks()
                            .get().prettyPrint(), actionGet);
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        final ClusterHealthRequest request = Requests.clusterHealthRequest()
                .waitForRelocatingShards(0);
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            onFailure("waitForRelocation timed out, cluster state:\n"
                    + "\n"
                    + client().admin().cluster().prepareState().get()
                            .getState().prettyPrint()
                    + "\n"
                    + client().admin().cluster().preparePendingClusterTasks()
                            .get().prettyPrint(), actionGet);
        }
        return actionGet.getStatus();
    }

    public FlushResponse flush() {
        waitForRelocation();
        final FlushResponse actionGet = client().admin().indices()
                .prepareFlush().execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            onFailure(shardFailures.toString(), actionGet);
        }
        return actionGet;
    }

    public RefreshResponse refresh() {
        waitForRelocation();
        final RefreshResponse actionGet = client().admin().indices()
                .prepareRefresh().execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            onFailure(shardFailures.toString(), actionGet);
        }
        return actionGet;
    }

    public OptimizeResponse optimize(final boolean force) {
        waitForRelocation();
        final OptimizeResponse actionGet = client().admin().indices()
                .prepareOptimize().setForce(force).execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet
                .getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            onFailure(shardFailures.toString(), actionGet);
        }
        return actionGet;
    }

    public CreateIndexResponse createIndex(final String index,
            final Settings settings) {
        final CreateIndexResponse actionGet = client()
                .admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                        settings != null ? settings
                                : ImmutableSettings.Builder.EMPTY_SETTINGS)
                .execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create " + index + ".", actionGet);
        }
        return actionGet;
    }

    public boolean indexExists(final String index) {
        final IndicesExistsResponse actionGet = client().admin().indices()
                .prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    public PutMappingResponse createMapping(final String index,
            final String type, final XContentBuilder source) {

        final PutMappingResponse actionGet = client().admin().indices()
                .preparePutMapping(index).setType(type).setSource(source)
                .execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create a mapping for " + index + ".",
                    actionGet);
        }
        return actionGet;
    }

    public IndexResponse insert(final String index, final String type,
            final String id, final String source) {
        final IndexResponse actionGet = client().prepareIndex(index, type, id)
                .setSource(source).setRefresh(true).execute().actionGet();
        if (!actionGet.isCreated()) {
            onFailure("Failed to insert " + id + " into " + index + "/" + type
                    + ".", actionGet);
        }
        return actionGet;
    }

    public DeleteResponse delete(final String index, final String type,
            final String id) {
        final DeleteResponse actionGet = client()
                .prepareDelete(index, type, id).setRefresh(true).execute()
                .actionGet();
        if (!actionGet.isFound()) {
            onFailure("Failed to delete " + id + " from " + index + "/" + type
                    + ".", actionGet);
        }
        return actionGet;
    }

    public SearchResponse search(final String index, final String type,
            final QueryBuilder queryBuilder, final SortBuilder sort,
            final int from, final int size) {
        final SearchResponse actionGet = client()
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(
                        queryBuilder != null ? queryBuilder : QueryBuilders
                                .matchAllQuery())
                .addSort(sort != null ? sort : SortBuilders.scoreSort())
                .setFrom(from).setSize(size).execute().actionGet();
        return actionGet;
    }

    public ClusterService clusterService() {
        return getInstance(ClusterService.class);
    }

    public synchronized <T> T getInstance(final Class<T> clazz) {
        final Node node = masterNode();
        if (node instanceof InternalNode) {
            return ((InternalNode) node).injector().getInstance(clazz);
        }
        return null;
    }

    public String getClusterName() {
        return clusterName;
    }

    private void onFailure(final String message, final ActionResponse response) {
        if (throwOnFailure) {
            throw new ClusterRunnerException(message, response);
        } else {
            print(message);
        }
    }

    /**
     * This builder sets parameters to create a node
     *
     * @author shinsuke
     */
    public interface Builder {

        /**
         * @param index an index of nodes
         * @param settingsBuilder a builder instance to create a node
         */
        void build(int index, ImmutableSettings.Builder settingsBuilder);
    }
}
