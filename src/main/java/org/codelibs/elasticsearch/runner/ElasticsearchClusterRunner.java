package org.codelibs.elasticsearch.runner;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ElasticsearchClusterRunner {
    private static final String LOGGING_YAML = "logging.yml";

    private static final String ELASTICSEARCH_YAML = "elasticsearch.yml";

    private static final String WORK_DIR = "work";

    private static final String DATA_DIR = "data";

    private static final String LOGS_DIR = "logs";

    private static final String PLUGINS_DIR = "plugins";

    private static final String CONFIG_DIR = "config";

    private List<Node> nodeList = new ArrayList<>();

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
    }

    public void build(final String[] args) {
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

    private Node buildNode(final int number) {
        final Path confPath = Paths.get(basePath, CONFIG_DIR);
        final Path pluginsPath = Paths.get(basePath, PLUGINS_DIR);
        final Path logsPath = Paths.get(basePath, LOGS_DIR, "node_" + number);
        final Path dataPath = Paths.get(basePath, DATA_DIR, "node_" + number);
        final Path workPath = Paths.get(basePath, WORK_DIR, "node_" + number);

        createDir(logsPath);
        createDir(dataPath);
        createDir(workPath);

        final ImmutableSettings.Builder settingsBuilder = settingsBuilder();

        settingsBuilder.put("path.conf", confPath.toAbsolutePath().toString());
        settingsBuilder.put("path.data", dataPath.toAbsolutePath().toString());
        settingsBuilder.put("path.work", workPath.toAbsolutePath().toString());
        settingsBuilder.put("path.logs", logsPath.toAbsolutePath().toString());
        settingsBuilder.put("path.plugins", pluginsPath.toAbsolutePath()
                .toString());

        final String nodeName = "Node " + number;
        final int transportPort = baseTransportPort + number;
        final int httpPort = baseHttpPort + number;
        settingsBuilder.put("cluster.name", clusterName);
        settingsBuilder.put("node.name", nodeName);
        settingsBuilder.put("node.master", true);
        settingsBuilder.put("node.data", true);
        settingsBuilder.put("http.enabled", true);
        settingsBuilder.put("transport.tcp.port", transportPort);
        settingsBuilder.put("http.port", httpPort);

        print("Node Name:      " + nodeName);
        print("HTTP Port:      " + httpPort);
        print("Transport Port: " + transportPort);
        print("Data Directory: " + dataPath);
        print("Log Directory:  " + logsPath);
        print("----------------------------------------");

        final Node node = new InternalNode(settingsBuilder.build(), false);
        node.start();
        return node;
    }

    public Node getNode(final int i) {
        return nodeList.get(i);
    }

    public int getNodeSize() {
        return nodeList.size();
    }

    private void print(final String line) {
        System.out.println(line);
    }

    private void createDir(final Path path) {
        if (!Files.exists(path)) {
            print("Creating " + path);
            try {
                Files.createDirectories(path);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Failed to create " + path, e);
            }
        }
    }
}
