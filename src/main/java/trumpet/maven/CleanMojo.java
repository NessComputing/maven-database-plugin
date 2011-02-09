package trumpet.maven;


/**
 * Maven goal that drops all database objects.
 *
 * @phase pre-integration-test
 * @goal clean
 */
public class CleanMojo extends AbstractDatabaseMojo
{
    @Override
    protected void doExecute() throws Exception
    {
    }
}
