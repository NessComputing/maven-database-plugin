package trumpet.maven;

import java.net.URI;
import java.util.List;

import org.apache.maven.plugin.MojoExecutionException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.ClasspathLoader;


/**
 * Maven goal that drops all databases.
 *
 * @goal drop
 */
public class DropMojo extends AbstractDatabaseMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(DropMojo.class);

    /**
     * @parameter expression="${databases}"
     * @required
     */
    private String databases;

    @Override
    protected void doExecute() throws Exception
    {
        final List<String> databaseList = expandDatabaseList(databases);

        final boolean permission = config.getBoolean("trumpet.permission.drop-db", false);
        if (!permission) {
            throw new MojoExecutionException("No permission to run this task!");
        }

        for (final String database : databaseList) {
            LOG.info("Dropping Database {}...", database);

            final DBI rootDbi = getDBIFor(rootDBIConfig);

            loaderManager.addLoader(new ClasspathLoader(loaderManager));

            final String sqlToRun = loaderManager.loadFile(URI.create("classpath:/sql/drop-database.st"));

            rootDbi.withHandle(new HandleCallback<Void>() {
                @Override
                public Void withHandle(final Handle handle) {
                    handle.createStatement(sqlToRun)
                    .define("database", database)
                    .execute();
                return null;
                }

            });
            LOG.info("... done");
        }
    }
}
