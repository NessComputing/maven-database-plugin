package trumpet.maven.util;

import io.trumpet.migratory.MigratoryException;
import io.trumpet.migratory.MigratoryException.Reason;
import io.trumpet.migratory.loader.LoaderManager;
import io.trumpet.migratory.loader.MigrationLoader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Loads a file from the classpath.
 */
public class ClasspathLoader implements MigrationLoader
{
    private final LoaderManager loaderManager;

    public ClasspathLoader(final LoaderManager loaderManager)
    {
        this.loaderManager = loaderManager;
    }

    @Override
    public boolean accept(final URI uri)
    {
        return (uri != null) && "classpath".equals(uri.getScheme());
    }

    @Override
    public Collection<URI> loadFolder(final URI folderUri, final String pattern)
    {
        try {
            final URI uriLocation = Resources.getResource(this.getClass(), folderUri.getPath()).toURI();
            return loaderManager.loadFolder(uriLocation, pattern);
        }
        catch (URISyntaxException e) {
            throw new MigratoryException(Reason.INTERNAL, e);
        }
    }

    /**
     * This method loads a file from a classpath:/... URI.
     */
    @Override
    public String loadFile(final URI fileUri)
    {
        try {
            final URL urlLocation = Resources.getResource(this.getClass(), fileUri.getPath());
            return Resources.toString(urlLocation, Charsets.UTF_8);
        }
        catch (IOException e) {
            throw new MigratoryException(Reason.INTERNAL, e);
        }
    }
}
