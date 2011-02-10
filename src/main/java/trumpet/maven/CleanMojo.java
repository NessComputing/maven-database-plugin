package trumpet.maven;

import io.trumpet.migratory.Migratory;
import io.trumpet.migratory.MigratoryConfig;
import io.trumpet.migratory.MigratoryException;

import java.util.List;

import org.apache.maven.plugin.MojoExecutionException;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;


/**
 * Maven goal that drops all database objects.
 *
 * @phase pre-integration-test
 * @goal clean
 */
public class CleanMojo extends AbstractDatabaseMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(CleanMojo.class);

    /**
     * @parameter expression="${databases}"
     * @required
     */
    private String databases;

    @Override
    protected void doExecute() throws Exception
    {
        final List<String> databaseList = expandDatabaseList(databases);

        final boolean permission = config.getBoolean("trumpet.permission.clean-db", false);
        if (!permission) {
            throw new MojoExecutionException("No permission to run this task!");
        }

        for (String database : databaseList) {
            LOG.info("Cleaning Database {}...", database);

            final DBIConfig databaseConfig = getDBIConfigFor(database);
            final DBI rootDbDbi = new DBI(databaseConfig.getDBUrl(), rootDBIConfig.getDBUser(), rootDBIConfig.getDBPassword());
            final DBI dbi = getDBIFor(database);
            final MigratoryConfig config = factory.build(MigratoryConfig.class);

            try {
                final Migratory migratory = new Migratory(config, dbi, rootDbDbi);
                migratory.dbClean(optionList);
            }
            catch (MigratoryException me) {
                LOG.warn("While cleaning {}: {}", database, me);
            }
            catch (RuntimeException re) {
                LOG.warn("While cleaning {}: {}", database, re);
            }

            LOG.info("... done");
        }
    }
}
