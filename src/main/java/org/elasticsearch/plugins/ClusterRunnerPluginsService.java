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
package org.elasticsearch.plugins;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;

public class ClusterRunnerPluginsService extends PluginsService {

    private static final Logger logger = LogManager
            .getLogger(ClusterRunnerPluginsService.class);

    private final List<LoadedPlugin> overridePlugins;

    private final PluginsAndModules overrideInfo;

    public ClusterRunnerPluginsService(Settings settings,
            Environment environment,
            Collection<Class<? extends Plugin>> classpathPlugins) {
        super(settings, environment.configFile(), environment.modulesFile(),
                environment.pluginsFile());

        final Path configPath = environment.configFile();

        List<LoadedPlugin> pluginsLoaded = new ArrayList<>();

        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginDescriptor pluginInfo = new PluginDescriptor(
                    pluginClass.getName(), "classpath plugin", "NA",
                    Version.CURRENT.toString(),
                    Integer.toString(Runtime.version().feature()),
                    pluginClass.getName(), null, Collections.emptyList(), false,
                    false, false, false);
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new LoadedPlugin(pluginInfo, plugin));
        }

        List<PluginRuntimeInfo> pluginInfos = new ArrayList<>();
        pluginInfos.addAll(super.info().getPluginInfos());
        pluginsLoaded.stream().map(LoadedPlugin::descriptor)
                .map(PluginRuntimeInfo::new).forEach(pluginInfos::add);

        pluginsLoaded.addAll(super.plugins());
        this.overridePlugins = List.copyOf(pluginsLoaded);
        this.overrideInfo = new PluginsAndModules(pluginInfos,
                super.info().getModuleInfos());

    }

    @Override
    protected final List<LoadedPlugin> plugins() {
        return this.overridePlugins;
    }

    @Override
    public PluginsAndModules info() {
        return this.overrideInfo;
    }

    @Override
    protected void addServerExportsService(
            Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        // no-op
    }
}
