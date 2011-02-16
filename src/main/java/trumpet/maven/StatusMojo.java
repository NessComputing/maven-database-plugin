package trumpet.maven;

import io.trumpet.migratory.Migratory;
import io.trumpet.migratory.MigratoryConfig;
import io.trumpet.migratory.MigratoryException;
import io.trumpet.migratory.StatusResult;
import io.trumpet.migratory.migration.MigrationPlanner.MigrationDirection;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;
import trumpet.maven.util.HttpLoader;
import trumpet.maven.util.MojoLocator;

import com.google.common.base.Charsets;


/**
 * Maven goal to report the status of the databases.
 *
 * @aggregator true
 * @requiresProject false
 * @goal status
 */
public class StatusMojo extends AbstractDatabaseMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(StatusMojo.class);

    /**
     * @parameter expression="${databases}" default-value="all"
     */
    private String databases = "all";

    @Override
    protected void doExecute() throws Exception
    {
        final List<String> databaseList = expandDatabaseList(databases);

        final boolean permission = config.getBoolean("trumpet.permission.status-db", true);
        if (!permission) {
            throw new MojoExecutionException("No permission to run this task!");
        }

        LOG.info(FRAME);
        LOG.info(HEADER);
        LOG.info(FRAME);

        for (String database : databaseList) {

            final Map<String, MigrationInformation> availableMigrations = getAvailableMigrations(database);

            final DBIConfig databaseConfig = getDBIConfigFor(database);
            final DBI rootDbDbi = new DBI(databaseConfig.getDBUrl(), rootDBIConfig.getDBUser(), rootDBIConfig.getDBPassword());
            final DBI dbi = getDBIFor(database);
            final MigratoryConfig config = factory.build(MigratoryConfig.class);

            try {
                final Migratory migratory = new Migratory(config, dbi, rootDbDbi);
                migratory.addLoader(new HttpLoader(Charsets.UTF_8));
                migratory.addLocator(new MojoLocator(migratory, manifestUrl));
                final Map<String, StatusResult> results = migratory.dbStatus(availableMigrations.keySet(), optionList);

                dump(database, results.values());
                LOG.info(FRAME);
            }
            catch (MigratoryException me) {
                LOG.warn("While getting status for {}: {}", database, me);
            }
            catch (RuntimeException re) {
                LOG.warn("While getting status for {}: {}", database, re);
            }
        }
    }

    private static final String FRAME  = "+---------------------------+---------------------------+-------+------+-------+------+---------+-----+";
    private static final String HEADER = "|         Database          |        Personality        | State | Curr | First | Last | Migrate | Dir |";
    private static final String BODY   = "| %-25s | %-25s | %-5s | %4d |  %4s | %4s |    %1s    | %-3s |";

    public static void dump(final String database, final Collection<StatusResult> results)
    {
        if (results == null || results.isEmpty()) {
            return;
        }

        for (StatusResult result : results) {
            LOG.info(String.format(BODY,
                              database,
                              result.getPersonalityName(),
                              result.getLastState(),
                              result.getCurrentVersion(),
                              // If the status code has no access to the available migrations (because there is
                              // no locator or loader, those will be MAX_VALUE for first and MIN_VALUE for last.
                              // In that case, ignore the output.
                              result.getFirstVersion() != Integer.MAX_VALUE ? Integer.toString(result.getFirstVersion()) : "",
                              result.getLastVersion() != Integer.MIN_VALUE ? Integer.toString(result.getLastVersion()) : "",
                              result.isMigrationPossible() ? "Y" : "N",
                              shortDir(result.getDirection())
                         ));
        }
    }

    private static String shortDir(final MigrationDirection dir)
    {
        switch (dir) {
        case FORWARD:
            return "FWD";
        case BACK:
            return "BCK";

        default:
            return "";
        }
    }
}
