package trumpet.maven.util;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class HttpAuthCredentials
{
    @Config("http.login")
    @DefaultNull
    public String getLogin()
    {
        return null;
    }

    @Config("http.password")
    @DefaultNull
    public String getPassword()
    {
        return null;
    }
}
