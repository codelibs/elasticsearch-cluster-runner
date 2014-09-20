package org.codelibs.elasticsearch.runner;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.net.Curl;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.sort.SortBuilders;

public class ElasticsearchClusterRunnerTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                // settingsBuilder.put("discovery.zen.minimum_master_nodes",
                // "3");
            }
        }).build(newConfigs().ramIndexStore());

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {

        // check if runner has nodes
        assertEquals(3, runner.getNodeSize());
        assertNotNull(runner.getNode(0));
        assertNotNull(runner.getNode(1));
        assertNotNull(runner.getNode(2));
        assertNotNull(runner.getNode("Node 1"));
        assertNotNull(runner.getNode("Node 2"));
        assertNotNull(runner.getNode("Node 3"));
        assertNull(runner.getNode("Node 4"));
        assertNotNull(runner.node());

        assertNotNull(runner.client());

        // check if a master node exists
        assertNotNull(runner.masterNode());
        assertNotNull(runner.nonMasterNode());
        assertFalse(runner.masterNode() == runner.nonMasterNode());

        // check if a cluster service exists
        assertNotNull(runner.clusterService());

        final String index = "test_index";
        final String type = "test_type";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // msg
                .startObject("msg")//
                .field("type", "string")//
                .endObject()//
                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.refresh();

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        {
            final SearchResponse searchResponse = runner.search(index, type,
                    QueryBuilders.matchAllQuery(),
                    SortBuilders.fieldSort("id"), 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        {
            final CountResponse countResponse = runner.count(index, type);
            assertEquals(1000, countResponse.getCount());
        }

        // delete 1 document
        runner.delete(index, type, String.valueOf(1));
        runner.flush();

        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(999, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        // optimize
        runner.optimize(false);

        String content = Curl.get(runner.masterNode(), "/_search")
                .param("q", "*:*").execute().getContentAsString();
        assertNotNull(content);

        // transport client
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", runner.getClusterName()).build();
        final int port = runner.node().settings()
                .getAsInt("transport.tcp.port", 9300);
        try (TransportClient client = new TransportClient(settings)) {
            client.addTransportAddress(new InetSocketTransportAddress(
                    "localhost", port));
            final SearchResponse searchResponse = client.prepareSearch(index)
                    .setTypes(type).setQuery(QueryBuilders.matchAllQuery())
                    .execute().actionGet();
            assertEquals(999, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        // node client
        try (Client client = NodeBuilder.nodeBuilder().settings(settings)
                .client(true).clusterName(runner.getClusterName()).node()
                .client()) {
            final SearchResponse searchResponse = client.prepareSearch(index)
                    .setTypes(type).setQuery(QueryBuilders.matchAllQuery())
                    .execute().actionGet();
            assertEquals(999, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        // close 1 node
        final Node node1 = runner.node();
        node1.close();
        final Node node2 = runner.node();
        assertTrue(node1 != node2);
        assertTrue(runner.getNode(0).isClosed());
        assertFalse(runner.getNode(1).isClosed());
        assertFalse(runner.getNode(2).isClosed());

        // restart a node
        assertTrue(runner.startNode(0));
        assertFalse(runner.startNode(1));
        assertFalse(runner.startNode(2));

        runner.ensureGreen();
    }
}
