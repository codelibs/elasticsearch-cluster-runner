/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.codelibs.elasticsearch.runner;

import static org.elasticsearch.common.settings.Settings.builder;

import java.io.Closeable;
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
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.runner.node.ClusterRunnerNode;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.ClusterRunnerPluginsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

/**
 * ElasticsearchClusterRunner manages multiple Elasticsearch instances.
 *
 * @author shinsuke
 *
 */
public class ElasticsearchClusterRunner implements Closeable {

    private static final Logger logger = LogManager.getLogger("codelibs.cluster.runner");

    public static final String NODE_NAME = "node.name";

    public static final String HTTP_PORT = "http.port";

    public static final String LOG4J2_PROPERTIES = "log4j2.properties";

    public static final String ELASTICSEARCH_YAML = "elasticsearch.yml";

    public static final String[] MODULE_TYPES = new String[] { //
            "org.elasticsearch.aggregations.AggregationsPlugin", //
            "org.elasticsearch.analysis.common.CommonAnalysisPlugin", //
            "org.elasticsearch.telemetry.apm.APM", //
            "org.elasticsearch.datastreams.DataStreamsPlugin", //
            "org.elasticsearch.ingest.attachment.IngestAttachmentPlugin", //
            "org.elasticsearch.ingest.common.IngestCommonPlugin", //
            // "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin", //
            "org.elasticsearch.ingest.useragent.IngestUserAgentPlugin", //
            "org.elasticsearch.kibana.KibanaPlugin", //
            "org.elasticsearch.script.expression.ExpressionPlugin", //
            "org.elasticsearch.script.mustache.MustachePlugin", //
            "org.elasticsearch.painless.PainlessPlugin", //
            "org.elasticsearch.legacygeo.LegacyGeoPlugin", //
            "org.elasticsearch.index.mapper.extras.MapperExtrasPlugin", //
            "org.elasticsearch.join.ParentJoinPlugin", //
            "org.elasticsearch.percolator.PercolatorPlugin", //
            "org.elasticsearch.index.rankeval.RankEvalPlugin", //
            "org.elasticsearch.reindex.ReindexPlugin", //
            // "org.elasticsearch.repositories.azure.AzureRepositoryPlugin", //
            "org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin", //
            "org.elasticsearch.repositories.s3.S3RepositoryPlugin", //
            "org.elasticsearch.plugin.repository.url.URLRepositoryPlugin", //
            "org.elasticsearch.rest.root.MainRestPlugin", //
            "org.elasticsearch.runtimefields.RuntimeFieldsCommonPlugin", //
            // "org.elasticsearch.systemd.SystemdPlugin", //
            "org.elasticsearch.transport.netty4.Netty4Plugin", //

    };

    public static final String DATA_DIR = "data";

    public static final String LOGS_DIR = "logs";

    public static final String CONFIG_DIR = "config";

    protected List<Node> nodeList = new ArrayList<>();

    protected List<Environment> envList = new ArrayList<>();

    protected Collection<Class<? extends Plugin>> pluginList = new ArrayList<>();

    protected int maxHttpPort = 9299;

    @Option(name = "-basePath", usage = "Base path for Elasticsearch.")
    protected String basePath;

    @Option(name = "-confPath", usage = "Config path for Elasticsearch.")
    protected String confPath;

    @Option(name = "-dataPath", usage = "Data path for Elasticsearch.")
    protected String dataPath;

    @Option(name = "-logsPath", usage = "Log path for Elasticsearch.")
    protected String logsPath;

    @Option(name = "-numOfNode", usage = "The number of Elasticsearch node.")
    protected int numOfNode = 3;

    @Option(name = "-baseHttpPort", usage = "Base http port.")
    protected int baseHttpPort = 9200;

    @Option(name = "-clusterName", usage = "Cluster name.")
    protected String clusterName = "elasticsearch-cluster-runner";

    @Option(name = "-indexStoreType", usage = "Index store type.")
    protected String indexStoreType = "fs";

    @Option(name = "-useLogger", usage = "Print logs to a logger.")
    protected boolean useLogger = false;

    @Option(name = "-disableESLogger", usage = "Disable ESLogger.")
    protected boolean disableESLogger = false;

    @Option(name = "-useConsoleAppender", usage = "Use a console appender.")
    protected boolean useConsoleAppender = true;

    @Option(name = "-printOnFailure", usage = "Print an exception on a failure.")
    protected boolean printOnFailure = false;

    @Option(name = "-moduleTypes", usage = "Module types.")
    protected String moduleTypes;

    @Option(name = "-pluginTypes", usage = "Plugin types.")
    protected String pluginTypes;

    protected Builder settingsBuilder;

    public static void main(final String[] args) {
        try (final ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner()) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        runner.close();
                    } catch (final IOException e) {
                        runner.print(e.getLocalizedMessage());
                    }
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
        } catch (final IOException e) {
            System.exit(1);
        }
    }

    public ElasticsearchClusterRunner() {
        // nothing
    }

    /**
     * Check if a cluster runner is closed.
     *
     * @return true if a runner is closed.
     */
    public boolean isClosed() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Close a cluster runner.
     * @throws IOException i/o exception
     */
    @Override
    public void close() throws IOException {
        final List<IOException> exceptionList = new ArrayList<>();
        for (final Node node : nodeList) {
            try {
                node.close();
            } catch (final IOException e) {
                exceptionList.add(e);
            }
        }
        if (exceptionList.isEmpty()) {
            print("Closed all nodes.");
        } else {
            if (useLogger && logger.isDebugEnabled()) {
                for (final Exception e : exceptionList) {
                    logger.debug("Failed to close a node.", e);
                }
            }
            throw new IOException(exceptionList.toString());
        }
    }

    /**
     * Delete all configuration files and directories.
     */
    public void clean() {
        LogManager.shutdown();
        final Path bPath = FileSystems.getDefault().getPath(basePath);
        final CleanUpFileVisitor visitor = new CleanUpFileVisitor();
        try {
            Files.walkFileTree(bPath, visitor);
            if (visitor.hasErrors()) {
                throw new ClusterRunnerException(visitor.getErrors().stream()
                        .map(e -> e.getLocalizedMessage())
                        .collect(Collectors.joining("\n")));
            }
        } catch (IOException e) {
            throw new ClusterRunnerException("Failed to delete " + bPath, e);
        }
    }

    /**
     * Configure each Elasticsearch instance by builder.
     *
     * @param builder builder to create a cluster
     * @return this instance
     */
    public ElasticsearchClusterRunner onBuild(final Builder builder) {
        this.settingsBuilder = builder;
        return this;
    }

    /**
     * Create and start Elasticsearch cluster with Configs instance.
     *
     * @param configs configuration
     */
    public void build(final Configs configs) {
        build(configs.build());
    }

    /**
     * Create and start Elasticsearch cluster with arguments.
     *
     * @param args artuments for starting a cluster
     */
    public void build(final String... args) {
        if (args != null) {
            final CmdLineParser parser = new CmdLineParser(this, ParserProperties.defaults().withUsageWidth(80));

            try {
                parser.parseArgument(args);
            } catch (final CmdLineException e) {
                throw new ClusterRunnerException("Failed to parse args: " + Strings.arrayToDelimitedString(args, " "));
            }
        }

        if (basePath == null) {
            try {
                basePath = Files.createTempDirectory("es-cluster").toAbsolutePath().toString();
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create $ES_HOME.", e);
            }
        }

        final Path esBasePath = Paths.get(basePath);
        createDir(esBasePath);

        final String[] types = moduleTypes == null ? MODULE_TYPES : moduleTypes.split(",");
        for (final String moduleType : types) {
            Class<? extends Plugin> clazz;
            try {
                clazz = Class.forName(moduleType).asSubclass(Plugin.class);
                pluginList.add(clazz);
            } catch (final ClassNotFoundException e) {
                logger.debug("{} is not found.", moduleType, e);
            }
        }
        if (pluginTypes != null) {
            for (final String value : pluginTypes.split(",")) {
                final String pluginType = value.trim();
                if (pluginType.length() > 0) {
                    Class<? extends Plugin> clazz;
                    try {
                        clazz = Class.forName(pluginType).asSubclass(Plugin.class);
                        pluginList.add(clazz);
                    } catch (final ClassNotFoundException e) {
                        throw new ClusterRunnerException(pluginType + " is not found.", e);
                    }
                }
            }
        }

        print("Base Path:    " + basePath);
        print("Num Of Node:  " + numOfNode);

        for (int i = 0; i < numOfNode; i++) {
            execute(i + 1);
            System.setProperty("es.set.netty.runtime.available.processors", "false");
        }
    }

    protected void execute(final int id) {
        final String nodeName = "Node " + id;
        final Path homePath = Paths.get(basePath, nodeName.replace(' ', '_').toLowerCase(Locale.ROOT));
        final Path confPath = this.confPath == null ? homePath.resolve(CONFIG_DIR) : Paths.get(this.confPath);
        final Path logsPath = this.logsPath == null ? homePath.resolve(LOGS_DIR) : Paths.get(this.logsPath);
        final Path dataPath = this.dataPath == null ? homePath.resolve(DATA_DIR) : Paths.get(this.dataPath);

        createDir(homePath);
        createDir(confPath);
        createDir(logsPath);
        createDir(dataPath);

        final Settings.Builder builder = builder();

        if (settingsBuilder != null) {
            settingsBuilder.build(id, builder);
        }

        putIfAbsent(builder, "path.home", homePath.toAbsolutePath().toString());
        putIfAbsent(builder, "path.data", dataPath.toAbsolutePath().toString());
        putIfAbsent(builder, "path.logs", logsPath.toAbsolutePath().toString());

        final Path esConfPath = confPath.resolve(ELASTICSEARCH_YAML);
        if (!esConfPath.toFile().exists()) {
            try (InputStream is =
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_DIR + "/" + ELASTICSEARCH_YAML)) {
                Files.copy(is, esConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create: " + esConfPath, e);
            }
        }

        if (!disableESLogger) {
            final Path logConfPath = confPath.resolve(LOG4J2_PROPERTIES);
            if (!logConfPath.toFile().exists()) {
                try (InputStream is =
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_DIR + "/" + LOG4J2_PROPERTIES)) {
                    Files.copy(is, logConfPath, StandardCopyOption.REPLACE_EXISTING);
                } catch (final IOException e) {
                    throw new ClusterRunnerException("Could not create: " + logConfPath, e);
                }
            }
        }

        try {
            final String pluginPath = builder.get("path.plugins");
            if (pluginPath != null) {
                final Path sourcePath = Paths.get(pluginPath);
                final Path targetPath = homePath.resolve("plugins");
                Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
                        Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                        Files.copy(file, targetPath.resolve(sourcePath.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
                        return FileVisitResult.CONTINUE;
                    }
                });
                builder.remove("path.plugins");
            }

            final int httpPort = getAvailableHttpPort(id);
            putIfAbsent(builder, "cluster.name", clusterName);
            putIfAbsent(builder, NODE_NAME, nodeName);
            putIfAbsent(builder, HTTP_PORT, String.valueOf(httpPort));
            putIfAbsent(builder, "index.store.type", indexStoreType);
            if (!builder.keys().contains("node.roles")) {
                builder.putList("node.roles", "master", "data");
            }

            print("Node Name:      " + builder.get(NODE_NAME));
            print("HTTP Port:      " + builder.get(HTTP_PORT));
            print("Data Directory: " + dataPath);
            print("Log Directory:  " + logsPath);

            final Settings settings = builder.build();
            final Environment environment =
                    InternalSettingsPreparer.prepareEnvironment(settings, Collections.emptyMap(), confPath, () -> nodeName);
            if (!disableESLogger) {
                LogConfigurator.registerErrorListener();
                final String envNodeName = Node.NODE_NAME_SETTING
                        .get(environment.settings());
                try {
                    LogConfigurator.setNodeName(envNodeName);
                } catch (final IllegalStateException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to set {} to a log configuration.",
                                envNodeName, e);
                    }
                }
                LogConfigurator.configure(environment, useConsoleAppender);
            }
            createDir(environment.modulesFile());
            createDir(environment.pluginsFile());

            final Node node = new ClusterRunnerNode(environment,
                    s -> new ClusterRunnerPluginsService(s, environment,
                            pluginList));
            node.start();
            nodeList.add(node);
            envList.add(environment);
        } catch (final Exception e) {
            throw new ClusterRunnerException("Failed to start node " + id, e);
        }
    }

    protected int getAvailableHttpPort(final int number) {
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
        throw new ClusterRunnerException("The http port " + httpPort + " is unavailable.");
    }

    protected void putIfAbsent(final Settings.Builder builder, final String key, final String value) {
        if (builder.get(key) == null && value != null) {
            builder.put(key, value);
        }
    }

    public void setMaxHttpPort(final int maxHttpPort) {
        this.maxHttpPort = maxHttpPort;
    }

    /**
     * Return a node by the node index.
     *
     * @param i A node index
     * @return null if the node is not found
     */
    public Node getNode(final int i) {
        if (i < 0 || i >= nodeList.size()) {
            return null;
        }
        return nodeList.get(i);
    }

    /**
     * Start a closed node.
     *
     * @param i the number of nodes
     * @return true if the node is started.
     */
    public boolean startNode(final int i) {
        if (i >= nodeList.size()) {
            return false;
        }
        if (!nodeList.get(i).isClosed()) {
            return false;
        }
        final Environment environment = envList.get(i);
        final Node node = new ClusterRunnerNode(environment,
                s -> new ClusterRunnerPluginsService(s, environment,
                        pluginList));
        try {
            node.start();
            nodeList.set(i, node);
            return true;
        } catch (final NodeValidationException e) {
            print(e.getLocalizedMessage());
        }
        return false;
    }

    /**
     * Return a node by the name.
     *
     * @param name A node name
     * @return null if the node is not found by the name
     */
    public Node getNode(final String name) {
        if (name == null) {
            return null;
        }
        for (final Node node : nodeList) {
            if (name.equals(node.settings().get(NODE_NAME))) {
                return node;
            }
        }
        return null;
    }

    /**
     * Return a node index.
     *
     * @param node node to check an index
     * @return -1 if the node does not exist.
     */
    public int getNodeIndex(final Node node) {
        for (int i = 0; i < nodeList.size(); i++) {
            if (nodeList.get(i).equals(node)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Return the number of nodes.
     *
     * @return the number of nodes
     */
    public int getNodeSize() {
        return nodeList.size();
    }

    public void print(final String line) {
        if (useLogger) {
            logger.info(line);
        } else {
            System.out.println(line);
        }
    }

    protected void createDir(final Path path) {
        if (!path.toFile().exists()) {
            print("Creating " + path);
            try {
                Files.createDirectories(path);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Failed to create " + path, e);
            }
        }
    }

    /**
     * Return an available node.
     *
     * @return node
     */
    public Node node() {
        for (final Node node : nodeList) {
            if (!node.isClosed()) {
                return node;
            }
        }
        throw new ClusterRunnerException("All nodes are closed.");
    }

    /**
     * Return a master node.
     *
     * @return master node
     */
    public synchronized Node masterNode() {
        final ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        final String name = state.nodes().getMasterNode().getName();
        return getNode(name);
    }

    /**
     * Return a non-master node.
     *
     * @return non-master node
     */
    public synchronized Node nonMasterNode() {
        final ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        final String name = state.nodes().getMasterNode().getName();
        for (final Node node : nodeList) {
            if (!node.isClosed() && !name.equals(node.settings().get(NODE_NAME))) {
                return node;
            }
        }
        return null;
    }

    /**
     * Return an elasticsearch client.
     *
     * @return client
     */
    public Client client() {
        return node().client();
    }

    /**
     * Return an elasticsearch admin client.
     *
     * @return admin client
     */
    public AdminClient admin() {
        return client().admin();
    }

    /**
     * Wait for green state of a cluster.
     *
     * @param indices indices to check status
     * @return cluster health status
     */
    public ClusterHealthStatus ensureGreen(final String... indices) {
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .prepareHealth(indices).setWaitForGreenStatus()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true).execute().actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "ensureGreen timed out, cluster state:\n" + client()
                            .admin().cluster().prepareState().get().getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    /**
     * Wait for yellow state of a cluster.
     *
     * @param indices indices to check status
     * @return cluster health status
     */
    public ClusterHealthStatus ensureYellow(final String... indices) {
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .prepareHealth(indices).setWaitForYellowStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID).execute().actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "ensureYellow timed out, cluster state:\n" + "\n" + client()
                            .admin().cluster().prepareState().get().getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        final ClusterHealthResponse actionGet = client().admin().cluster()
                .prepareHealth().setWaitForNoRelocatingShards(true).execute()
                .actionGet();
        if (actionGet.isTimedOut()) {
            onFailure(
                    "waitForRelocation timed out, cluster state:\n" + "\n"
                            + client().admin().cluster().prepareState().get()
                                    .getState()
                            + "\n"
                            + client().admin().cluster()
                                    .preparePendingClusterTasks().get(),
                    actionGet);
        }
        return actionGet.getStatus();
    }

    public FlushResponse flush() {
        return flush(true);
    }

    public FlushResponse flush(final boolean force) {
        return flush(builder -> builder.setWaitIfOngoing(true).setForce(force));
    }

    public FlushResponse flush(final BuilderCallback<FlushRequestBuilder> builder) {
        waitForRelocation();
        final FlushResponse actionGet = builder.apply(client().admin().indices().prepareFlush()).execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet.getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    public RefreshResponse refresh() {
        return refresh(builder -> builder);
    }

    public RefreshResponse refresh(final BuilderCallback<RefreshRequestBuilder> builder) {
        waitForRelocation();
        final RefreshResponse actionGet = builder.apply(client().admin().indices().prepareRefresh()).execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet.getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    public ForceMergeResponse forceMerge() {
        return forceMerge(-1, false, true);
    }

    public ForceMergeResponse forceMerge(final int maxNumSegments, final boolean onlyExpungeDeletes, final boolean flush) {
        return forceMerge(builder -> builder.setMaxNumSegments(maxNumSegments).setOnlyExpungeDeletes(onlyExpungeDeletes).setFlush(flush));
    }

    public ForceMergeResponse forceMerge(final BuilderCallback<ForceMergeRequestBuilder> builder) {
        waitForRelocation();
        final ForceMergeResponse actionGet = builder.apply(client().admin().indices().prepareForceMerge()).execute().actionGet();
        final ShardOperationFailedException[] shardFailures = actionGet.getShardFailures();
        if (shardFailures != null && shardFailures.length != 0) {
            final StringBuilder buf = new StringBuilder(100);
            for (final ShardOperationFailedException shardFailure : shardFailures) {
                buf.append(shardFailure.toString()).append('\n');
            }
            onFailure(buf.toString(), actionGet);
        }
        return actionGet;
    }

    public OpenIndexResponse openIndex(final String index) {
        return openIndex(index, builder -> builder);
    }

    public OpenIndexResponse openIndex(final String index, final BuilderCallback<OpenIndexRequestBuilder> builder) {
        final OpenIndexResponse actionGet = builder.apply(client().admin().indices().prepareOpen(index)).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to open " + index + ".", actionGet);
        }
        return actionGet;
    }

    public AcknowledgedResponse closeIndex(final String index) {
        return closeIndex(index, builder -> builder);
    }

    public AcknowledgedResponse closeIndex(final String index, final BuilderCallback<CloseIndexRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder.apply(client().admin().indices().prepareClose(index)).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to close " + index + ".", actionGet);
        }
        return actionGet;
    }

    public CreateIndexResponse createIndex(final String index, final Settings settings) {
        return createIndex(index, builder -> builder.setSettings(settings != null ? settings : Settings.EMPTY));
    }

    public CreateIndexResponse createIndex(final String index, final BuilderCallback<CreateIndexRequestBuilder> builder) {
        final CreateIndexResponse actionGet = builder.apply(client().admin().indices().prepareCreate(index)).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create " + index + ".", actionGet);
        }
        return actionGet;
    }

    public boolean indexExists(final String index) {
        return indexExists(index, builder -> builder);
    }

    public boolean indexExists(final String index,
            final BuilderCallback<GetIndexRequestBuilder> builder) {
        try {
            final GetIndexResponse actionGet = builder.apply(client().admin()
                    .indices().prepareGetIndex().setIndices(index)).execute()
                    .actionGet();
            return actionGet.indices().length > 0;
        } catch (final IndexNotFoundException e) {
            return false;
        }
    }

    public AcknowledgedResponse deleteIndex(final String index) {
        return deleteIndex(index, builder -> builder);
    }

    public AcknowledgedResponse deleteIndex(final String index, final BuilderCallback<DeleteIndexRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder.apply(client().admin().indices().prepareDelete(index)).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create " + index + ".", actionGet);
        }
        return actionGet;
    }

    public AcknowledgedResponse createMapping(final String index, final String mappingSource) {
        return createMapping(index, builder -> builder.setSource(mappingSource, xContentType(mappingSource)));
    }

    public AcknowledgedResponse createMapping(final String index, final XContentBuilder source) {
        return createMapping(index, builder -> builder.setSource(source));
    }

    public AcknowledgedResponse createMapping(final String index, final BuilderCallback<PutMappingRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder.apply(client().admin().indices().preparePutMapping(index)).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to create a mapping for " + index + ".", actionGet);
        }
        return actionGet;
    }

    public DocWriteResponse insert(final String index, final String id, final String source) {
        return insert(index, id,
                builder -> builder.setSource(source, xContentType(source)).setRefreshPolicy(RefreshPolicy.IMMEDIATE));
    }

    public DocWriteResponse insert(final String index, final String id,
            final BuilderCallback<IndexRequestBuilder> builder) {
        final DocWriteResponse actionGet = builder.apply(client().prepareIndex().setIndex(index).setId(id)).execute().actionGet();
        if (actionGet.getResult() != Result.CREATED) {
            onFailure("Failed to insert " + id + " into " + index + ".", actionGet);
        }
        return actionGet;
    }

    public DeleteResponse delete(final String index, final String id) {
        return delete(index, id, builder -> builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE));
    }

    public DeleteResponse delete(final String index, final String id,
            final BuilderCallback<DeleteRequestBuilder> builder) {
        final DeleteResponse actionGet = builder.apply(client().prepareDelete().setIndex(index).setId(id)).execute().actionGet();
        if (actionGet.getResult() != Result.DELETED) {
            onFailure("Failed to delete " + id + " from " + index + ".", actionGet);
        }
        return actionGet;
    }

    public SearchResponse count(final String index) {
        return count(index, builder -> builder);
    }

    public SearchResponse count(final String index, final BuilderCallback<SearchRequestBuilder> builder) {
        return builder.apply(client().prepareSearch(index).setSize(0)).execute().actionGet();
    }

    public SearchResponse search(final String index, final QueryBuilder queryBuilder, final SortBuilder<?> sort, final int from,
            final int size) {
        return search(index, builder -> builder.setQuery(queryBuilder != null ? queryBuilder : QueryBuilders.matchAllQuery())
                .addSort(sort != null ? sort : SortBuilders.scoreSort()).setFrom(from).setSize(size));
    }

    public SearchResponse search(final String index, final BuilderCallback<SearchRequestBuilder> builder) {
        return builder.apply(client().prepareSearch(index)).execute().actionGet();
    }

    public GetAliasesResponse getAlias(final String alias) {
        return getAlias(alias, builder -> builder);
    }

    public GetAliasesResponse getAlias(final String alias, final BuilderCallback<GetAliasesRequestBuilder> builder) {
        return builder.apply(client().admin().indices().prepareGetAliases(alias)).execute().actionGet();
    }

    public AcknowledgedResponse updateAlias(final String alias, final String[] addedIndices, final String[] deletedIndices) {
        return updateAlias(builder -> {
            if (addedIndices != null && addedIndices.length > 0) {
                builder.addAlias(addedIndices, alias);
            }
            if (deletedIndices != null && deletedIndices.length > 0) {
                builder.removeAlias(deletedIndices, alias);
            }
            return builder;
        });
    }

    public AcknowledgedResponse updateAlias(final BuilderCallback<IndicesAliasesRequestBuilder> builder) {
        final AcknowledgedResponse actionGet = builder.apply(client().admin().indices().prepareAliases()).execute().actionGet();
        if (!actionGet.isAcknowledged()) {
            onFailure("Failed to update aliases.", actionGet);
        }
        return actionGet;
    }

    public ClusterService clusterService() {
        return getInstance(ClusterService.class);
    }

    public synchronized <T> T getInstance(final Class<T> clazz) {
        final Node node = masterNode();
        return node.injector().getInstance(clazz);
    }

    public String getClusterName() {
        return clusterName;
    }

    private void onFailure(final String message, final ActionResponse response) {
        if (printOnFailure) {
            print(message);
        } else {
            throw new ClusterRunnerException(message, response);
        }
    }

    private static final class CleanUpFileVisitor implements FileVisitor<Path> {
        private final List<Throwable> errorList = new ArrayList<>();

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        public boolean hasErrors() {
            return !errorList.isEmpty();
        }

        public List<Throwable> getErrors() {
            return errorList;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return checkIfExist(file);
        }

        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            throw exc;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc == null) {
                Files.delete(dir);
                if (dir.toFile().exists()) {
                    errorList.add(new IOException("Failed to delete " + dir));
                    dir.toFile().deleteOnExit();
                }
                return FileVisitResult.CONTINUE;
            } else {
                throw exc;
            }
        }

        private FileVisitResult checkIfExist(final Path path) {
            if (path.toFile().exists()) {
                errorList.add(new IOException("Failed to delete " + path));
                path.toFile().deleteOnExit();
            }
            return FileVisitResult.CONTINUE;
        }
    }

    /**
     * This builder sets parameters to create a node
     *
     */
    public interface Builder {

        /**
         * @param index an index of nodes
         * @param builder a builder instance to create a node
         */
        void build(int index, Settings.Builder builder);
    }

    public static Configs newConfigs() {
        return new Configs();
    }

    /**
     * ElasticsearchClusterRunner configuration.
     *
     */
    public static class Configs {
        List<String> configList = new ArrayList<>();

        public Configs basePath(final String basePath) {
            configList.add("-basePath");
            configList.add(basePath);
            return this;
        }

        public Configs numOfNode(final int numOfNode) {
            configList.add("-numOfNode");
            configList.add(String.valueOf(numOfNode));
            return this;
        }

        public Configs baseHttpPort(final int baseHttpPort) {
            configList.add("-baseHttpPort");
            configList.add(String.valueOf(baseHttpPort));
            return this;
        }

        public Configs clusterName(final String clusterName) {
            configList.add("-clusterName");
            configList.add(clusterName);
            return this;
        }

        public Configs indexStoreType(final String indexStoreType) {
            configList.add("-indexStoreType");
            configList.add(indexStoreType);
            return this;
        }

        public Configs useLogger() {
            configList.add("-useLogger");
            return this;
        }

        public Configs disableESLogger() {
            configList.add("-disableESLogger");
            return this;
        }

        public Configs useConsoleAppender() {
            configList.add("-useConsoleAppender");
            return this;
        }

        public Configs printOnFailure() {
            configList.add("-printOnFailure");
            return this;
        }

        public Configs moduleTypes(final String moduleTypes) {
            configList.add("-moduleTypes");
            configList.add(moduleTypes);
            return this;
        }

        public Configs pluginTypes(final String pluginTypes) {
            configList.add("-pluginTypes");
            configList.add(pluginTypes);
            return this;
        }

        public String[] build() {
            return configList.toArray(new String[configList.size()]);
        }

    }

    private static XContentType xContentType(final CharSequence content) {
        final int length = content.length() < 20 ? content.length() : 20;
        if (length == 0) {
            return null;
        }
        final char first = content.charAt(0);
        if (first == '{' || JsonXContent.jsonXContent.detectContent(content)) {
            return XContentType.JSON;
        }
        // Should we throw a failure here? Smile idea is to use it in bytes....
        if (SmileXContent.smileXContent.detectContent(content)) {
            return XContentType.SMILE;
        }
        if (YamlXContent.yamlXContent.detectContent(content)) {
            return XContentType.YAML;
        }

        // CBOR is not supported

        for (int i = 0; i < length; i++) {
            final char c = content.charAt(i);
            if (c == '{') {
                return XContentType.JSON;
            }
            if (!Character.isWhitespace(c)) {
                break;
            }
        }
        return null;
    }

    /**
     * Callback function.
     */
    public interface BuilderCallback<T> {
        T apply(T builder);
    }
}
