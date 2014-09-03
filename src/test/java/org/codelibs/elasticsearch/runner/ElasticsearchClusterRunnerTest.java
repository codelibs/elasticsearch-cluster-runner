package org.codelibs.elasticsearch.runner;

import junit.framework.TestCase;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
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
                // settingsBuilder.put("discovery.zen.minimum_master_nodes", "3");
            }
        }).build();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {

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

        // wait for yellow status
        runner.ensureYellow();

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

        // close 1 node
        final Node node1 = runner.node();
        node1.close();
        final Node node2 = runner.node();
        assertTrue(node1 != node2);
        assertTrue(runner.getNode(0).isClosed());
        assertFalse(runner.getNode(1).isClosed());
        assertFalse(runner.getNode(2).isClosed());

    }

}
