package trumpet.maven;

import io.trumpet.migratory.loader.FileLoader;
import io.trumpet.migratory.loader.JarLoader;
import io.trumpet.migratory.loader.LoaderManager;

import java.io.StringReader;
import java.net.URI;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.skife.config.CommonsConfigSource;
import org.skife.config.ConfigurationObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import trumpet.maven.util.DBIConfig;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.pyx4j.log4j.MavenLogAppender;

public abstract class AbstractDatabaseMojo extends AbstractMojo
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseMojo.class);

    protected DBIConfig rootDBIConfig;

    /**
     * @parameter expression="${manifest.url}"
     * @required
     */
    private String manifestUrl;

    /**
     * @parameter expression="${manifest.name}"
     */
    private String manifestName = "development";

    /**
     * @parameter expression="${manifest.personality}"
     * @required
     */
    private String manifestPersonality;

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException
    {
        MavenLogAppender.startPluginLog(this);

        try {
            LOG.debug("Manifest URL: {}", manifestUrl);
            LOG.debug("Manifest Name: {}", manifestName);
            LOG.debug("Manifest Personality: {}", manifestPersonality);

            final LoaderManager loaderManager = new LoaderManager();
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

            final CombinedConfiguration cc = new CombinedConfiguration(new OverrideCombiner());
            cc.addConfiguration(new SystemConfiguration(), "systemProperties");
            final PropertiesConfiguration pc = new PropertiesConfiguration();
            pc.load(new StringReader(contents));
            cc.addConfiguration(pc);

            LOG.debug("Configuration now: {}", cc);

            final ConfigurationObjectFactory factory = new ConfigurationObjectFactory(new CommonsConfigSource(cc));
            rootDBIConfig = factory.buildWithReplacements(DBIConfig.class, ImmutableMap.of("_dbi_name", "trumpet.db.root"));

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
}
