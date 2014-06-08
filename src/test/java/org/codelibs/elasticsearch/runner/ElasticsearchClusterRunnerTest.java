package org.codelibs.elasticsearch.runner;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

public class ElasticsearchClusterRunnerTest extends TestCase {
    public ElasticsearchClusterRunnerTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(ElasticsearchClusterRunnerTest.class);
    }

    public void test_runCluster() throws Exception {
        ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
        runner.build(null);

        assertEquals(3, runner.getNodeSize());
        Node node1 = runner.getNode(0);
        assertNotNull(node1);
        Node node2 = runner.getNode(1);
        assertNotNull(node2);
        Node node3 = runner.getNode(2);
        assertNotNull(node3);

        Client client1 = node1.client();
        client1.admin().cluster().prepareHealth().setWaitForYellowStatus()
                .execute().actionGet();

        IndexResponse indexResponse = client1
                .prepareIndex("test_index", "test_type", "1")
                .setSource("{\"id\":\"1\",\"message\":\"Hello 1\"}").execute()
                .actionGet();
        assertTrue(indexResponse.isCreated());

        GetResponse getResponse = client1
                .prepareGet("test_index", "test_type", "1").execute()
                .actionGet();
        assertTrue(getResponse.isExists());

        runner.close();

        Thread.sleep(5000);

        new File(runner.basePath).delete();
    }

}
