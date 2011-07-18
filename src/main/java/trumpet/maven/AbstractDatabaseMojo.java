package trumpet.maven;

import io.trumpet.migratory.MigratoryConfig;
import io.trumpet.migratory.MigratoryOption;
import io.trumpet.migratory.loader.FileLoader;
import io.trumpet.migratory.loader.HttpLoader;
import io.trumpet.migratory.loader.JarLoader;
import io.trumpet.migratory.loader.LoaderManager;

import java.io.StringReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
import org.skife.config.SimplePropertyConfigSource;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pyx4j.log4j.MavenLogAppender;

public abstract class AbstractDatabaseMojo extends AbstractMojo
{
    private static final String [] MUST_EXIST = new String [] { "trumpet.default.base", "trumpet.default.root_url", "trumpet.default.root_user", "trumpet.default.root_password", "trumpet.default.user", "trumpet.default.password" };
    private static final String [] MUST_NOT_BE_EMPTY = new String [] { "trumpet.default.base", "trumpet.default.root_url", "trumpet.default.root_user", "trumpet.default.user" };


    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseMojo.class);

    protected DBIConfig rootDBIConfig;

    protected Configuration config;

    private ConfigurationObjectFactory factory;

    protected final LoaderManager loaderManager = new LoaderManager();

    protected MigratoryOption [] optionList;

    /**
     * @parameter expression="${manifest.url}" default-value="https://depot.trumpet.io/database/default"
     */
    protected String manifestUrl = "https://depot.trumpet.io/database/default";

    /**
     * @parameter expression="${manifest.name}" default-value="development"
     */
    private String manifestName = "development";

    /**
     * @parameter expression="${options}"
     */
    private String options;

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException
    {
        MavenLogAppender.startPluginLog(this);

        try {
            LOG.debug("Manifest URL: {}", manifestUrl);
            LOG.debug("Manifest Name: {}", manifestName);

            // This config object is only used for the Http Loader, so that login and pw
            // can be provided using plugin parameters.
            final ConfigurationObjectFactory systemFactory = new ConfigurationObjectFactory(new SimplePropertyConfigSource(System.getProperties()));

            final MigratoryConfig migratoryConfig = new TrumpetSpecificDelegatingMigratoryConfig(systemFactory.build(MigratoryConfig.class));

            loaderManager.addLoader(new FileLoader(Charsets.UTF_8));
            loaderManager.addLoader(new JarLoader(Charsets.UTF_8));
            loaderManager.addLoader(new HttpLoader(migratoryConfig));

            this.optionList = parseOptions(options);

            final StringBuilder location = new StringBuilder(manifestUrl);
            if (!manifestUrl.endsWith("/")) {
                location.append("/");
            }
            manifestUrl = location.toString();

            // After here, the manifestUrl is guaranteed to have a / at the end!

            location.append(manifestName);
            location.append(".manifest");

            LOG.debug("Manifest location: {}", location);

            final String contents = loaderManager.loadFile(URI.create(location.toString()));

            if (contents == null) {
                throw new MojoExecutionException("Could not load manifest '" + manifestName + "' from '" + manifestUrl + "'");
            }

            final CombinedConfiguration config = new CombinedConfiguration(new OverrideCombiner());
            config.addConfiguration(new SystemConfiguration(), "systemProperties");
            final PropertiesConfiguration pc = new PropertiesConfiguration();
            pc.load(new StringReader(contents));
            config.addConfiguration(pc);

            if (!validateConfiguration(config)) {
                throw new MojoExecutionException("Manifest '" + manifestName + "' is not valid. Refusing to execute!");
            }

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



    /**
     * Executes this mojo.
     */
    protected abstract void doExecute() throws Exception;

    protected DBIConfig getDBIConfig(final String prefix)
    {
        return factory.buildWithReplacements(DBIConfig.class, ImmutableMap.of("_dbi_name", prefix));
    }

    protected MigratoryConfig getMigratoryConfig()
    {
        return new TrumpetSpecificDelegatingMigratoryConfig(factory.build(MigratoryConfig.class));
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

            @Override
            public String getDBTablespace() {
                return baseConfig.getDBTablespace();
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

    protected List<String> expandDatabaseList(final String databases) throws MojoExecutionException
    {
        final String [] databaseNames = StringUtils.stripAll(StringUtils.split(databases, ","));
        if (databaseNames == null) {
            return  Collections.<String>emptyList();
        }

        final List<String> availableDatabases = getAvailableDatabases();

        if (databaseNames.length == 1 && databaseNames[0].equalsIgnoreCase("all")) {
            return availableDatabases;
        }
        else {
            for (String database : databaseNames) {
                if (!availableDatabases.contains(database)) {
                    throw new MojoExecutionException("Database " + database + " is unknown!");
                }
            }

            return Arrays.asList(databaseNames);
        }
    }

    protected List<String> getAvailableDatabases()
    {
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

    protected MigratoryOption [] parseOptions(final String options)
    {
        final String [] optionList = StringUtils.stripAll(StringUtils.split(options, ","));

        if (optionList == null) {
            return new MigratoryOption[0];
        }

        final MigratoryOption [] migratoryOptions = new MigratoryOption[optionList.length];
        for (int i = 0 ; i < optionList.length; i++) {
            migratoryOptions[i] = MigratoryOption.valueOf(optionList[i].toUpperCase(Locale.ENGLISH));
        }

        LOG.debug("Parsed {} into {}", options, migratoryOptions);
        return migratoryOptions;
    }


    protected Map<String, MigrationInformation> getAvailableMigrations(final String database) throws MojoExecutionException
    {
        final Map<String, MigrationInformation> availableMigrations = Maps.newHashMap();

        addMigrations("trumpet.db." + database, availableMigrations);
        addMigrations("trumpet.default.personalities", availableMigrations);

        return availableMigrations;
    }

    protected void addMigrations(final String property, final Map<String, MigrationInformation> availableMigrations) throws MojoExecutionException
    {
        final String [] personalities = StringUtils.stripAll(config.getStringArray(property));
        for (String personality : personalities) {
            final String [] personalityParts = StringUtils.stripAll(StringUtils.split(personality, ":"));

            if (personalityParts == null || personalityParts.length < 1 || personalityParts.length > 2) {
                throw new MojoExecutionException("Personality " + personality + " is invalid.");
            }

            if (personalityParts.length == 1) {
                availableMigrations.put(personalityParts[0], new MigrationInformation(personalityParts[0], 0));
            }
            else {
                availableMigrations.put(personalityParts[0], new MigrationInformation(personalityParts[0], Integer.parseInt(personalityParts[1], 10)));
            }
        }
    }

    protected static class MigrationInformation
    {
        private final String name;
        private final int priority;

        public MigrationInformation(final String name, final int priority)
        {
            this.name = name;
            this.priority = priority;
        }

        public String getName()
        {
            return name;
        }

        public int getPriority()
        {
            return priority;
        }
    }

    private boolean validateConfiguration(Configuration config) throws MojoExecutionException
    {
        boolean valid = true;
        for (String mustExist : MUST_EXIST) {
            if (!config.containsKey(mustExist)) {
                LOG.error("The required property '{}' does not exist in the manifest.", mustExist);
                valid = false;
}
        }

        for (String mustNotBeEmpty : MUST_NOT_BE_EMPTY) {
            if (StringUtils.isBlank(config.getString(mustNotBeEmpty, null))) {
                LOG.error("The property '{}' must not be empty.", mustNotBeEmpty);
                valid = false;
            }
        }

        final String defaultBase = config.getString("trumpet.default.base", "");
        if (defaultBase.indexOf("%s") == -1 || defaultBase.indexOf("%s") != defaultBase.lastIndexOf("%s")) {
            LOG.error("The 'config.default.base' property must contain exactly one '%s' place holder!");
            valid = false;
        }

        return valid;
    }
}
