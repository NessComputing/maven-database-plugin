package trumpet.maven;

import io.trumpet.migratory.loader.FileLoader;
import io.trumpet.migratory.loader.JarLoader;
import io.trumpet.migratory.loader.LoaderManager;

import java.io.StringReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.skife.config.CommonsConfigSource;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pyx4j.log4j.MavenLogAppender;

public abstract class AbstractDatabaseMojo extends AbstractMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseMojo.class);

    protected DBIConfig rootDBIConfig;

    protected Configuration config;

    protected ConfigurationObjectFactory factory;

    protected final LoaderManager loaderManager = new LoaderManager();

    /**
     * @parameter expression="${manifest.url}"
     * @required
     */
    private String manifestUrl;

    /**
     * @parameter expression="${manifest.name}"
     */
    private String manifestName = "development";

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException
    {
        MavenLogAppender.startPluginLog(this);

        try {
            LOG.debug("Manifest URL: {}", manifestUrl);
            LOG.debug("Manifest Name: {}", manifestName);

            loaderManager.addLoader(new FileLoader(Charsets.UTF_8));
            loaderManager.addLoader(new JarLoader(Charsets.UTF_8));

            final StringBuilder location = new StringBuilder(manifestUrl);
            if (!manifestUrl.endsWith("/")) {
                location.append("/");
            }
            location.append(manifestName);
            location.append(".manifest");

            LOG.debug("Manifest location: {}", location);

            final String contents = loaderManager.loadFile(URI.create(location.toString()));

            final CombinedConfiguration config = new CombinedConfiguration(new OverrideCombiner());
            config.addConfiguration(new SystemConfiguration(), "systemProperties");
            final PropertiesConfiguration pc = new PropertiesConfiguration();
            pc.load(new StringReader(contents));
            config.addConfiguration(pc);

            this.config = config;
            LOG.debug("Configuration now: {}", this.config);

            factory = new ConfigurationObjectFactory(new CommonsConfigSource(config));
            rootDBIConfig = getDBIConfig("trumpet.default.root_");

            doExecute();
        }
        catch (Exception e) {
            LOG.error("While executing Mojo {}: {}", this.getClass().getSimpleName(), e);
            throw new MojoExecutionException("Failure:" ,e);
        }
        finally {
            MavenLogAppender.endPluginLog(this);
        }
    }

    protected DBIConfig getDBIConfig(final String prefix)
    {
        return factory.buildWithReplacements(DBIConfig.class, ImmutableMap.of("_dbi_name", prefix));
    }

    protected DBIConfig getDBIConfigFor(final String database)
    {
        final DBIConfig baseConfig = getDBIConfig("trumpet.db." + database + ".");
        final String dbUrl = (baseConfig.getDBUrl() != null)
            ? baseConfig.getDBUrl()
            : String.format(config.getString("trumpet.default.base"), database);

        return new DBIConfig() {
            @Override
            public String getDBDriverClass() {
                return baseConfig.getDBDriverClass();
            }

            @Override
            public String getDBUser() {
                return baseConfig.getDBUser();
            }

            @Override
            public String getDBPassword() {
                return baseConfig.getDBPassword();
            }

            @Override
            public String getDBUrl() {
                return dbUrl;
            }
        };
    }

    protected DBI getDBIFor(final String database) throws Exception
    {
        return getDBIFor(getDBIConfigFor(database));
    }

    protected DBI getDBIFor(final DBIConfig dbiConfig) throws Exception
    {
        if (dbiConfig.getDBDriverClass() != null) {
            Class.forName(dbiConfig.getDBDriverClass());
        }

        return new DBI(dbiConfig.getDBUrl(), dbiConfig.getDBUser(), dbiConfig.getDBPassword());
    }

    protected List<String> expandDatabaseList(final String databases)
    {
        final String [] databaseNames = StringUtils.stripAll(StringUtils.split(databases, ","));
        if (databaseNames == null) {
            return  Collections.<String>emptyList();
        }
        if (databaseNames.length == 1 && databaseNames[0].equalsIgnoreCase("all")) {
            final List<String> databaseList = Lists.newArrayList();

            Configuration dbConfig = config.subset("trumpet.db");
            for (Iterator<?> it = dbConfig.getKeys(); it.hasNext(); ) {
                final String key = (String) it.next();
                if (key.contains(".")) {
                    continue;
                }
                databaseList.add(key);
            }
            return databaseList;
        }
        else {
            return Arrays.asList(databaseNames);
        }
    }

    /**
     * Executes this mojo.
     */
    protected abstract void doExecute() throws Exception;
}
