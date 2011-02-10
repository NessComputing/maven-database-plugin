package trumpet.maven.util;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public interface DBIConfig
{
    @Config({"${_dbi_name}driver", "trumpet.default.driver"})
    @DefaultNull
    String getDBDriverClass();

    @Config({"${_dbi_name}url"})
    @DefaultNull
    String getDBUrl();

    @Config({"${_dbi_name}user", "trumpet.default.user"})
    @DefaultNull
    String getDBUser();

    @Config({"${_dbi_name}password", "trumpet.default.password"})
    @DefaultNull
    String getDBPassword();

    @Config({"${_dbi_name}tablespace", "trumpet.default.tablespace"})
    @DefaultNull
    String getDBTablespace();
}
