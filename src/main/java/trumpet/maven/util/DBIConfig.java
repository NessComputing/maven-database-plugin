package trumpet.maven.util;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class DBIConfig
{
    @Config("${_dbi_name}.driver")
    @DefaultNull
    public String getDBDriverClass()
    {
        return null;
    }

    @Config("${_dbi_name}.url")
    @DefaultNull
    public String getDBUrl()
    {
        return null;
    }

    @Config("${_dbi_name}.user")
    @DefaultNull
    public String getDBUser()
    {
        return null;
    }

    @Config("${_dbi_name}.password")
    @DefaultNull
    public String getDBPassword()
    {
        return null;
    }
}
