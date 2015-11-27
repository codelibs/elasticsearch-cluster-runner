package org.elasticsearch.node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

public class ClusterRunnerNode extends Node {

	private Version version;

	private Collection<Class<? extends Plugin>> plugins;

	public ClusterRunnerNode(Settings settings, Version version, Collection<Class<? extends Plugin>> classpathPlugins) {
		super(settings, version, classpathPlugins);
		this.version = version;
		this.plugins = classpathPlugins;
	}

	public ClusterRunnerNode(final Settings preparedSettings) {
		this(preparedSettings, Version.CURRENT, getDefaultPlugins(preparedSettings));
	}

	public Collection<Class<? extends Plugin>> getPlugins() {
		return plugins;
	}

	public Version getVersion() {
		return version;
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
