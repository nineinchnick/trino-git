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

import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;

public final class GitQueryRunner
{
    private GitQueryRunner() {}

    public static final String GIT_CATALOG = "git";
    public static final String GIT_SCHEMA = "default";

    public static Builder builder(URI repositoryUri)
    {
        return new Builder(repositoryUri);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder(URI repositoryUri)
        {
            super(createSession());
            connectorProperties.put("git.url", repositoryUri.toString());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            if (!connectorProperties.containsKey("git.cache-directory")) {
                Path cacheDirectory = Files.createTempDirectory("trino-git-cache-");
                registerResource(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));
                connectorProperties.put("git.cache-directory", cacheDirectory.toString());
            }
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new MemoryPlugin());
                queryRunner.createCatalog("memory", "memory");

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new GitPlugin());
                queryRunner.createCatalog(GIT_CATALOG, "git", connectorProperties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GIT_CATALOG)
                .setSchema(GIT_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.plugin", Level.DEBUG);
        logging.setLevel("io.trino.spi", Level.DEBUG);
        logging.setLevel("pl.net.was", Level.DEBUG);

        String url = "https://github.com/nineinchnick/trino-git.git";
        if (args.length > 0) {
            url = args[0];
        }

        DistributedQueryRunner queryRunner = builder(URI.create(url))
                .addExtraProperty("http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8081"))
                .build();

        Logger log = Logger.get(GitQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
