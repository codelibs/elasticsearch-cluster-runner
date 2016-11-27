package org.codelibs.elasticsearch.runner.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

public class ClusterRunnerNode extends Node {

	private Collection<Class<? extends Plugin>> plugins;

    public ClusterRunnerNode(Environment tmpEnv,
            Collection<Class<? extends Plugin>> classpathPlugins) {
        super(tmpEnv, classpathPlugins);
        this.plugins = classpathPlugins;
    }

    public ClusterRunnerNode(final Settings preparedSettings) {
        this(InternalSettingsPreparer.prepareEnvironment(preparedSettings,
                null), getDefaultPlugins(preparedSettings));
    }

	public Collection<Class<? extends Plugin>> getPlugins() {
		return plugins;
	}

	private static Collection<Class<? extends Plugin>> getDefaultPlugins(final Settings preparedSettings) {
		final String pluginTypes = preparedSettings.get("plugin.types");
		if (pluginTypes != null) {
			Collection<Class<? extends Plugin>> pluginList = new ArrayList<>();
			for (String pluginType : pluginTypes.split(",")) {
				Class<? extends Plugin> clazz;
				try {
					clazz = Class.forName(pluginType).asSubclass(Plugin.class);
					pluginList.add(clazz);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Failed to load " + pluginType, e);
				}
			}
			return pluginList;
		}
		return Collections.<Class<? extends Plugin>>emptyList();
	}
}
