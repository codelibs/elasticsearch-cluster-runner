/*
 * Copyright 2012-2020 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.elasticsearch.node;

import java.util.function.Function;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginsService;

public class ClusterRunnerNode extends Node {

    public ClusterRunnerNode(final Environment initialEnvironment,
            final Function<Settings, PluginsService> pluginServiceCtor) {
        super(NodeConstruction.prepareConstruction(initialEnvironment,
                new NodeServiceProvider() {
                    @Override
                    PluginsService newPluginService(Environment environment,
                            Settings settings) {
                        return pluginServiceCtor.apply(settings);
                    }
                }, true));
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(
            final ClusterService clusterService) {
        // nothing
    }
}
