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

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;

import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import junit.framework.TestCase;

public class ElasticsearchClusterRunnerTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    private static final int NUM_OF_NODES = 3;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-cl-run-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301", "127.0.0.1:9302");
                settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1:9301");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(NUM_OF_NODES));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
        assertFalse("Check if " + runner.basePath + " is deleted", Files
                .exists(FileSystems.getDefault().getPath(runner.basePath)));
    }

    public void test_runCluster() throws Exception {

        // check if runner has nodes
        assertEquals(NUM_OF_NODES, runner.getNodeSize());
        assertNotNull(runner.getNode(0));
        assertNotNull(runner.getNode(1));
        assertNotNull(runner.getNode(2));
        assertNotNull(runner.getNode("Node 1"));
        assertNotNull(runner.getNode("Node 2"));
        assertNotNull(runner.getNode("Node 3"));
        assertNull(runner.getNode(NUM_OF_NODES));
        assertNotNull(runner.node());

        assertNotNull(runner.client());

        // check if a master node exists
        assertNotNull(runner.masterNode());
        assertNotNull(runner.nonMasterNode());
        assertFalse(runner.masterNode() == runner.nonMasterNode());

        // check if a cluster service exists
        assertNotNull(runner.clusterService());

        final String index = "test_index";

        // create an index
        runner.createIndex(index, (Settings) null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "keyword")//
                .endObject()//

                // msg
                .startObject("msg")//
                .field("type", "text")//
                .endObject()//

                // order
                .startObject("order")//
                .field("type", "long")//
                .endObject()//

                // @timestamp
                .startObject("@timestamp")//
                .field("type", "date")//
                .endObject()//

                .endObject()//
                .endObject();
        runner.createMapping(index, mappingBuilder);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final DocWriteResponse indexResponse1 = runner.insert(index, String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"order\":" + i + ",\"@timestamp\":\"2000-01-01T00:00:00\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        // update alias
        final String alias = index + "_alias";
        {
            final GetAliasesResponse aliasesResponse = runner.getAlias(alias);
            assertNull(aliasesResponse.getAliases().get(alias));
        }

        {
            runner.updateAlias(alias, new String[] { index }, null);
            runner.flush();
            final GetAliasesResponse aliasesResponse = runner.getAlias(alias);
            assertEquals(1, aliasesResponse.getAliases().size());
            assertEquals(1, aliasesResponse.getAliases().get(index).size());
            assertEquals(alias, aliasesResponse.getAliases().get(index).get(0).alias());
        }

        {
            runner.updateAlias(alias, null, new String[] { index });
            final GetAliasesResponse aliasesResponse = runner.getAlias(alias);
            assertNull(aliasesResponse.getAliases().get(alias));
        }

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(index, null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits().value);
            assertEquals(10, searchResponse.getHits().getHits().length);
        }

        {
            final SearchResponse searchResponse = runner.search(index, QueryBuilders.matchAllQuery(), SortBuilders.fieldSort("id"), 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits().value);
            assertEquals(10, searchResponse.getHits().getHits().length);
        }

        {
            final SearchResponse searchResponse = runner.count(index);
            assertEquals(1000, searchResponse.getHits().getTotalHits().value);
        }

        // delete 1 document
        runner.delete(index, String.valueOf(1));
        runner.flush();

        {
            final SearchResponse searchResponse = runner.search(index, null, null, 0, 10);
            assertEquals(999, searchResponse.getHits().getTotalHits().value);
            assertEquals(10, searchResponse.getHits().getHits().length);
        }

        // optimize
        runner.forceMerge();

        final Node node = runner.node();

        // http access
        // get
        try (CurlResponse curlResponse =
                EcrCurl.get(node, "/_search").header("Content-Type", "application/json").param("q", "*:*").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            assertTrue(content.contains("total"));
            final Map<String, Object> map = curlResponse.getContent(EcrCurl.jsonParser());
            assertNotNull(map);
            assertEquals("false", map.get("timed_out").toString());
        }

        // post
        try (CurlResponse curlResponse = EcrCurl.post(node, "/" + index + "/_doc/").header("Content-Type", "application/json")
                .body("{\"id\":\"2000\",\"msg\":\"test 2000\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContent(EcrCurl.jsonParser());
            assertNotNull(map);
            assertEquals("created", map.get("result"));
        }

        // put
        try (CurlResponse curlResponse = EcrCurl.put(node, "/" + index + "/_doc/2001").header("Content-Type", "application/json")
                .body("{\"id\":\"2001\",\"msg\":\"test 2001\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContent(EcrCurl.jsonParser());
            assertNotNull(map);
            assertEquals("created", map.get("result"));
        }

        // delete
        try (CurlResponse curlResponse =
                EcrCurl.delete(node, "/" + index + "/_doc/2001").header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContent(EcrCurl.jsonParser());
            assertNotNull(map);
            assertEquals("deleted", map.get("result"));
        }

        // post
        try (CurlResponse curlResponse = EcrCurl.post(node, "/" + index + "/_doc/").header("Content-Type", "application/json")
                .onConnect((curlRequest, connection) -> {
                    connection.setDoOutput(true);
                    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"))) {
                        writer.write("{\"id\":\"2002\",\"msg\":\"test 2002\"}");
                        writer.flush();
                    } catch (IOException e) {
                        throw new CurlException("Failed to write data.", e);
                    }
                }).execute()) {
            final Map<String, Object> map = curlResponse.getContent(EcrCurl.jsonParser());
            assertNotNull(map);
            assertEquals("created", map.get("result"));
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
