/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
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
package org.codelibs.elasticsearch.runner.node;

import java.util.Collection;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

public class ClusterRunnerNode extends Node {

    private final Collection<Class<? extends Plugin>> plugins;

    public ClusterRunnerNode(final Environment tmpEnv, final Collection<Class<? extends Plugin>> classpathPlugins) {
        super(tmpEnv, classpathPlugins, true);
        this.plugins = classpathPlugins;
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        return plugins;
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        // nothing
    }
}
