package com.nesscomputing.migratory.mojo.database.util;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class HttpAuthCredentials
{
    @Config("http.login")
    @Default("config")
    public String getLogin()
    {
        return "config";
    }

    @Config("http.password")
    @Default("verysecret")
    public String getPassword()
    {
        return "verysecret";
    }
}
