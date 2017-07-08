package org.codelibs.elasticsearch.runner.node;

import java.util.Collection;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

public class ClusterRunnerNode extends Node {

	private final Collection<Class<? extends Plugin>> plugins;

    public ClusterRunnerNode(final Environment tmpEnv,
            final Collection<Class<? extends Plugin>> classpathPlugins) {
        super(tmpEnv, classpathPlugins);
        this.plugins = classpathPlugins;
    }

    public ClusterRunnerNode(final Settings preparedSettings,
            final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(InternalSettingsPreparer.prepareEnvironment(preparedSettings,
                null), classpathPlugins);
    }

	public Collection<Class<? extends Plugin>> getPlugins() {
		return plugins;
	}
}
