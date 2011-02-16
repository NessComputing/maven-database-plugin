package trumpet.maven;

import io.trumpet.migratory.Migratory;
import io.trumpet.migratory.MigratoryConfig;
import io.trumpet.migratory.MigratoryException;
import io.trumpet.migratory.validation.ValidationResult;
import io.trumpet.migratory.validation.ValidationResult.ValidationResultProblem;

import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;
import trumpet.maven.util.MojoLocator;


/**
 * Maven goal to validate databases.
 *
 * @aggregator true
 * @requiresProject false
 * @goal validate
 */
public class ValidateMojo extends AbstractDatabaseMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(ValidateMojo.class);

    /**
     * @parameter expression="${databases}" default-value="all"
     */
    private String databases = "all";

    @Override
    protected void doExecute() throws Exception
    {
        final List<String> databaseList = expandDatabaseList(databases);

        final boolean permission = config.getBoolean("trumpet.permission.validate-db", true);
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
                migratory.addLoader(httpLoader);
                migratory.addLocator(new MojoLocator(migratory, manifestUrl));
                final Map<String, ValidationResult> results = migratory.dbValidate(availableMigrations.keySet(), optionList);

                dump(database, results);
                LOG.info(FRAME);
            }
            catch (MigratoryException me) {
                LOG.warn("While validaing for {}: {}", database, me);
            }
            catch (RuntimeException re) {
                LOG.warn("While validating for {}: {}", database, re);
            }
        }
    }

    private static final String FRAME  = "+---------------------------+---------------------------+----------------------+----------------------------------------------------+";
    private static final String HEADER = "|         Database          |        Personality        |     State/Problem    | Reason                                             |";
    private static final String BODY   = "| %-25s | %-25s | %-20s | %50s |";

    public static void dump(final String database, final Map<String, ValidationResult> results)
    {
        if (results == null || results.isEmpty()) {
            return;
        }

        for (final Map.Entry<String, ValidationResult> result : results.entrySet()) {
            final String personalityName = result.getKey();
            final ValidationResult validationResult = result.getValue();

            LOG.info(String.format(BODY,
                                   database,
                                   personalityName,
                                   validationResult.getValidationStatus(),
                                   ""));

            final List<ValidationResultProblem> problems = validationResult.getProblems();
            for (ValidationResultProblem problem: problems) {
                LOG.info(String.format(BODY,
                                       "",
                                       "",
                                       problem.getValidationStatus(),
                                       problem.getReason()
                             ));
            }
        }
    }
}
