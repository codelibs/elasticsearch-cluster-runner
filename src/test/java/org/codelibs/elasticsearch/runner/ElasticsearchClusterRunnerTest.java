package org.codelibs.elasticsearch.runner;

import junit.framework.TestCase;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.sort.SortBuilders;

public class ElasticsearchClusterRunnerTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        runner = new ElasticsearchClusterRunner();
        runner.build();
    }

    @Override
    protected void tearDown() throws Exception {
        runner.close();
        runner.clean();
    }

    public void test_runCluster() throws Exception {

        assertEquals(3, runner.getNodeSize());
        final Node node1 = runner.getNode(0);
        assertNotNull(node1);
        final Node node2 = runner.getNode(1);
        assertNotNull(node2);
        final Node node3 = runner.getNode(2);
        assertNotNull(node3);

        assertNotNull(runner.client());

        runner.ensureYellow();

        final String index = "test_index";
        final String type = "test_type";
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }

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

        runner.delete(index, type, String.valueOf(1));

    }

}
