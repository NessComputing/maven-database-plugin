package trumpet.maven.util;

import io.trumpet.httpclient.HttpClient;
import io.trumpet.httpclient.HttpClientDefaults;
import io.trumpet.httpclient.HttpClientRequest;
import io.trumpet.httpclient.HttpClientResponse;
import io.trumpet.httpclient.response.ContentConverter;
import io.trumpet.httpclient.response.ContentResponseHandler;
import io.trumpet.migratory.loader.MigrationLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.skife.config.ConfigurationObjectFactory;
import org.skife.config.SimplePropertyConfigSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

public class HttpLoader implements MigrationLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpLoader.class);

    private final Charset charset;
    private final HttpClientDefaults httpClientDefaults;
    private final HttpAuthCredentials authCredentials;

    private final ContentResponseHandler<String> responseHandler = new ContentResponseHandler<String>(new StringConverter());

    public HttpLoader(final Charset charset)
    {
        final ConfigurationObjectFactory objectFactory = new ConfigurationObjectFactory(new SimplePropertyConfigSource(System.getProperties()));

        this.charset = charset;
        this.httpClientDefaults = objectFactory.build(HttpClientDefaults.class);
        this.authCredentials = objectFactory.build(HttpAuthCredentials.class);
    }

    @Override
    public boolean accept(final URI uri)
    {
        return (uri != null) && ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()));
    }

    @Override
    public String loadFile(final URI fileUri) throws IOException
    {
        final HttpClient httpClient = new HttpClient(httpClientDefaults);

        try {
            LOG.trace("Trying to load '%s'...", fileUri);
            try {
                final HttpClientRequest.Builder<String> req = httpClient.get(fileUri, responseHandler);
                if (authCredentials.getLogin() != null && authCredentials.getPassword() != null) {
                    req.addBasicAuth(authCredentials.getLogin(), authCredentials.getPassword());
                }

                final String result = req.perform();
                if (result != null) {
                    LOG.trace("... succeeded");
                    return result;
                }
                else {
                    LOG.trace("... not found");
                }
            }
            catch (IOException ioe) {
                LOG.trace("... failed", ioe);
            }
            return null;
        }
        finally {
            httpClient.close();
        }
    }

    @Override
    public Collection<URI> loadFolder(final URI folderUri, final String searchPattern) throws IOException
    {
        final List<URI> results = Lists.newArrayList();
        final Pattern pattern = (searchPattern == null) ? null : Pattern.compile(searchPattern);

//        String folder = folderUri.toString();
//        if (!folder.endsWith("/")) {
//            folder += "/";
//        }
        final String path = folderUri.getPath();
        final URI baseUri = (path.endsWith("/") ? folderUri : folderUri.resolve(path + "/"));

        final String content = loadFile(baseUri);
        if (content == null) {
            // File not found
            return results;
        }

        // The folders are a list of file names in plain text. Split it up.
        final String [] filenames = StringUtils.split(content);

        for (String filename : filenames) {
            if (pattern.matcher(filename).matches()) {
                results.add(URI.create(baseUri + filename));
            }
        }

        return results;
    }


    private final class StringConverter implements ContentConverter<String>
    {
        @Override
        public String convert(final HttpClientResponse response, final InputStream inputStream)
            throws IOException
        {
            switch (response.getStatusCode())
            {
                case 404:
                    return null;
                case 401:
                    LOG.warn("Could not load content from '%s', not authorized!", response.getUri());
                    return null;
                case 200:
                    return CharStreams.toString(new InputStreamReader(inputStream, charset));
                default:
                    throw new IOException("Could not load content from " + response.getUri().toString() + " (" + response.getStatusCode() + ")");
            }
        }

        @Override
        public String handleError(HttpClientResponse response, IOException ex) throws IOException
        {
            throw new IOException("Could not load content from " + response.getUri().toString() + " (" + response.getStatusCode() + ")", ex.getCause());
        }
    }
}





