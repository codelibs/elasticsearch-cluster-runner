Elasticsearch Cluster Runner
============================

This project runs Elasticsearch cluster on one JVM instance for your development/testing easily.
You can use Elasticsearch Cluster Runner as Embedded Elasticsearch in your application.

## Version

[Versions in Maven Repository](http://central.maven.org/maven2/org/codelibs/elasticsearch-cluster-runner/)

## Run on Your Application

Put elasticsearch-cluster-runner if using Maven:

    <dependency>
        <groupId>org.codelibs</groupId>
        <artifactId>elasticsearch-cluster-runner</artifactId>
        <version>5.5.2.0</version>
    </dependency>

### Start Cluster Runner

    import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
    ...
    // create runner instance
    ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
    // create ES nodes
    runner.onBuild(new ElasticsearchClusterRunner.Builder() {
        @Override
        public void build(final int number, final Builder settingsBuilder) {
            // put elasticsearch settings
            // settingsBuilder.put("index.number_of_replicas", 0);
        }
    }).build(newConfigs());

build(Configs) method configures/starts Clsuter Runner.

### Stop Cluster Runner

    // close runner
    runner.close();

### Clean up 

    // delete all files(config and index)
    runner.clean();

## Run on JUnit

Put elasticsearch-cluster-runner as test scope:

    <dependency>
        <groupId>org.codelibs</groupId>
        <artifactId>elasticsearch-cluster-runner</artifactId>
        <version>5.5.2.0</version>
        <scope>test</scope>
    </dependency>

and see [ElasticsearchClusterRunnerTest](https://github.com/codelibs/elasticsearch-cluster-runner/blob/master/src/test/java/org/codelibs/elasticsearch/runner/ElasticsearchClusterRunnerTest.java "ElasticsearchClusterRunnerTest").

## Run as Standalone

### Install Maven

Download and install Maven 3 from http://maven.apache.org/.

### Clone This Project

    git clone https://github.com/codelibs/elasticsearch-cluster-runner.git

### Build This Project

    mvn compile

## Run/Stop Elasticsearch Cluster

### Run Cluster

Run:

    mvn exec:java 

The default cluster has 3 nodes and the root directory for Elasticsearch is es\_home.
Nodes use 9201-9203 port for HTTP and 9301-9303 port for Transport.
If you want to change the number of node, Run:

    mvn exec:java -Dexec.args="-basePath es_home -numOfNode 4"

### Stop Cluster

Type Ctrl-c or kill the process.

## Others

### Install Plugins

This project does not have plugin command to install plugins.
Therefore, you need to put plugins manually.
For example, installing solr-api plugin:

    $ mkdir -p es_home/plugins/solr-api
    $ wget http://repo1.maven.org/maven2/org/codelibs/elasticsearch-solr-api/1.4.0/elasticsearch-solr-api-1.4.0.zip
    $ unzip elasticsearch-solr-api-1.4.0.zip 
    $ rm elasticsearch-solr-api-1.4.0.zip 


### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-cluster-runner/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

