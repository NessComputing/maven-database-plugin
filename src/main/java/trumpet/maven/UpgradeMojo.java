package trumpet.maven;

import io.trumpet.migratory.Migratory;
import io.trumpet.migratory.MigratoryConfig;
import io.trumpet.migratory.MigratoryException;
import io.trumpet.migratory.migration.MigrationPlan;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;
import trumpet.maven.util.MojoLocator;

import com.google.common.collect.Maps;


/**
 * Maven goal that upgrades all databases.
 *
 * @goal upgrade
 */
public class UpgradeMojo extends AbstractDatabaseMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(UpgradeMojo.class);

    /**
     * Describes the migrations for this database.
     *
     * all -> all databases, latest version. Default.
     * follow Migrate follow database to latest version.
     * oauth=oauth@4  : explicit version for a personality on a given db (all others are ignored)
     * trumpet_test=oauth@4/prefs@2 Migrate two personalities of a given database.
     * follow=follow@4,prefs=prefs@7  Migrate two different databases.
     * follow=follow Migrate follow personality in the follow database to latest version
     *
     * @parameter expression="${migrations}"
     * @required
     */
    private String migrations = "all";

    @Override
    protected void doExecute() throws Exception
    {
        final boolean permission = config.getBoolean("trumpet.permission.upgrade-db", false);
        if (!permission) {
            throw new MojoExecutionException("No permission to run this task!");
        }

        final Map<String, String> databases = extractDatabases(migrations);
        final MigratoryConfig config = factory.build(MigratoryConfig.class);



        for (Map.Entry<String, String> database : databases.entrySet()) {
            final String databaseName = database.getKey();

            final DBIConfig databaseConfig = getDBIConfigFor(databaseName);
            final DBI rootDbDbi = new DBI(databaseConfig.getDBUrl(), rootDBIConfig.getDBUser(), rootDBIConfig.getDBPassword());

            try {
                final MigrationPlan rootMigrationPlan  = createMigrationPlan(database, true);
                if (!rootMigrationPlan.isEmpty()) {
                    LOG.info("Migrating {} as root user ...", databaseName);

                    Migratory migratory = new Migratory(config, rootDbDbi);
                    migratory.addLocator(new MojoLocator(migratory, manifestUrl));
                    migratory.dbMigrate(rootMigrationPlan);
                }

                final MigrationPlan migrationPlan  = createMigrationPlan(database, false);
                if (!migrationPlan.isEmpty()) {
                    LOG.info("Migrating {} as schema owner ...", databaseName);
                    final DBI dbi = getDBIFor(databaseName);

                    Migratory migratory = new Migratory(config, dbi);
                    migratory.addLocator(new MojoLocator(migratory, manifestUrl));
                    migratory.dbMigrate(migrationPlan);
                }
            }
            catch (MigratoryException me) {
                LOG.warn("While creating {}: {}", databaseName, me);
            }
        }
        LOG.info("... done");
    }

    protected Map<String, String> extractDatabases(final String migrations)throws MojoExecutionException
    {
        String [] migrationNames = StringUtils.stripAll(StringUtils.split(migrations, ","));

        final List<String> availableDatabases = getAvailableDatabases();

        if (migrationNames == null) {
            return  Collections.<String, String>emptyMap();
        }

        final Map<String, String> databases = Maps.newHashMap();

        if (migrationNames.length == 1 && migrationNames[0].equalsIgnoreCase("all")) {
            migrationNames = availableDatabases.toArray(new String[availableDatabases.size()]);
        }

        for (String migration : migrationNames) {
            final String [] migrationFields = StringUtils.stripAll(StringUtils.split(migration, "="));

            if (migrationFields == null || migrationFields.length < 1 || migrationFields.length > 2) {
                throw new MojoExecutionException("Migration " + migration + " is invalid.");
            }

            if (!availableDatabases.contains(migrationFields[0])) {
                throw new MojoExecutionException("Database " + migrationFields[0] + " is unknown!");
            }

            databases.put(migrationFields[0], (migrationFields.length == 1 ? null : migrationFields[1]));
        }

        return databases;
    }

    protected MigrationPlan createMigrationPlan(final Map.Entry<String, String> database, boolean rootPlan) throws MojoExecutionException
    {
        final Map<String, MigrationInformation> availableMigrations = getAvailableMigrations(database.getKey());

        final MigrationPlan migrationPlan = new MigrationPlan();

        // Do we have any special migrations given?
        final String migrations = database.getValue();
        if (StringUtils.isEmpty(migrations)) {
            for (MigrationInformation availableMigration : availableMigrations.values()) {
                if (rootPlan == availableMigration.isRootMigration()) {
                    migrationPlan.addMigration(availableMigration.getName(), Integer.MAX_VALUE, availableMigration.getPriority());
                }
            }

            return migrationPlan; // No
        }

        final String [] migrationNames = StringUtils.stripAll(StringUtils.split(migrations, "/"));

        for (String migrationName : migrationNames) {
            final String [] migrationFields = StringUtils.stripAll(StringUtils.split(migrationName, "@"));

            if (migrationFields == null || migrationFields.length < 1 || migrationFields.length > 2) {
                throw new MojoExecutionException("Migration " + migrationName + " is invalid.");
            }

            int targetVersion = migrationFields.length == 2 ? Integer.parseInt(migrationFields[1], 10) : Integer.MAX_VALUE;

            MigrationInformation migrationInformation = availableMigrations.get(migrationFields[0]);

            if (migrationInformation == null) {
                throw new MojoExecutionException("Migration " + migrationName + " is unknown!");
            }

            if (rootPlan == migrationInformation.isRootMigration()) {
                migrationPlan.addMigration(migrationInformation.getName(), targetVersion, migrationInformation.getPriority());
            }
        }

        return migrationPlan;
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
                availableMigrations.put(personalityParts[0], new MigrationInformation(personalityParts[0], 0, false));
            }
            else {
                boolean rootMigration = personalityParts[1].startsWith("R");
                final String versionString = rootMigration ? personalityParts[1].substring(1) : personalityParts[1];

                int priority = versionString.isEmpty() ? 0 : Integer.parseInt(versionString, 10);
                availableMigrations.put(personalityParts[0], new MigrationInformation(personalityParts[0], priority, rootMigration));
            }
        }
    }

    private static class MigrationInformation
    {
        private final String name;
        private final int priority;
        private final boolean rootMigration;

        public MigrationInformation(final String name, final int priority, final boolean rootMigration)
        {
            this.name = name;
            this.priority = priority;
            this.rootMigration = rootMigration;
        }

        public String getName()
        {
            return name;
        }

        public int getPriority()
        {
            return priority;
        }

        public boolean isRootMigration()
        {
            return rootMigration;
        }
    }
}

