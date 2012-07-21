package com.nesscomputing.migratory.mojo.database;

import com.google.common.base.Objects;
import com.nesscomputing.migratory.MigratoryConfig;

/**
 * This is Ness specific. Remove before opensourcing!
 */
class TrumpetSpecificDelegatingMigratoryConfig extends MigratoryConfig
{
    private final MigratoryConfig delegate;

    TrumpetSpecificDelegatingMigratoryConfig(final MigratoryConfig delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public String getEncoding()
    {
        return delegate.getEncoding();
    }

    @Override
    public boolean isReadOnly()
    {
        return delegate.isReadOnly();
    }

    @Override
    public boolean isCreatePersonalities()
    {
        return delegate.isCreatePersonalities();
    }

    @Override
    public boolean isAllowRollForward()
    {
        return delegate.isAllowRollForward();
    }

    @Override
    public boolean isAllowRollBack()
    {
        return delegate.isAllowRollBack();
    }

    @Override
    public String getMetadataTableName()
    {
        return "migratory_metadata";
    }

    @Override
    public String getHttpLogin()
    {
        return Objects.firstNonNull(delegate.getHttpLogin(), "config");
    }

    @Override
    public String getHttpPassword()
    {
        return Objects.firstNonNull(delegate.getHttpLogin(), "verysecret");
    }
}
