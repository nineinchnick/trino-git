/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.net.was.trino.git;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.procedure.Procedure;
import jakarta.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Set;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

/**
 * Makes every node fetch new commits the next time it reads a repository, without waiting for git.fetch-interval.
 */
public class RefreshProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REFRESH;

    static {
        try {
            REFRESH = lookup().unreflect(RefreshProcedure.class.getMethod("refresh", String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final Set<String> schemaNames;
    private final RefreshGenerations refreshGenerations;

    @Inject
    public RefreshProcedure(GitConfig config, RefreshGenerations refreshGenerations)
    {
        this.schemaNames = config.getAllRepositories().keySet();
        this.refreshGenerations = requireNonNull(refreshGenerations, "refreshGenerations is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "refresh",
                ImmutableList.of(new Procedure.Argument("SCHEMA_NAME", VARCHAR, false, null)),
                REFRESH.bindTo(this));
    }

    public void refresh(String schemaName)
    {
        if (schemaName == null) {
            schemaNames.forEach(refreshGenerations::refresh);
            return;
        }
        checkProcedureArgument(schemaNames.contains(schemaName), "Unknown schema: %s", schemaName);
        refreshGenerations.refresh(schemaName);
    }
}
