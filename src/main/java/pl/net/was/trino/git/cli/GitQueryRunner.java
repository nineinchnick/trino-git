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
package pl.net.was.trino.git.cli;

import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.cli.Console;
import io.trino.plugin.memory.MemoryPlugin;
import pl.net.was.trino.git.GitPlugin;

import java.util.HashMap;
import java.util.Map;

import static io.trino.cli.Trino.createCommandLine;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public final class GitQueryRunner
{
    private GitQueryRunner() {}

    public static EmbeddedServer createServer(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        EmbeddedServer queryRunner = EmbeddedServer.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory");

            connectorProperties = new HashMap<>(Map.copyOf(connectorProperties));

            queryRunner.installPlugin(new GitPlugin());
            queryRunner.createCatalog("git", "git", connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);

            throw e;
        }
    }

    private static <T extends Throwable> void closeAllSuppress(T rootCause, AutoCloseable... closeables)
    {
        requireNonNull(rootCause, "rootCause is null");
        if (closeables == null) {
            return;
        }
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            }
            catch (Throwable e) {
                // Self-suppression not permitted
                if (rootCause != e) {
                    rootCause.addSuppressed(e);
                }
            }
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("git")
                .setSchema("default")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.plugin", Level.DEBUG);
        logging.setLevel("io.trino.spi", Level.DEBUG);
        logging.setLevel("pl.net.was", Level.DEBUG);

        EmbeddedServer server = createServer(
                Map.of("http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8080")),
                Map.of("metadata-uri", requireNonNull(System.getenv("TRINO_GIT_URL"))));

        Logger log = Logger.get(EmbeddedServer.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", server.getCoordinator().getBaseUrl());

        int exitCode = createCommandLine(new Console()).execute(args);
        server.close();
        System.exit(exitCode);
    }
}
