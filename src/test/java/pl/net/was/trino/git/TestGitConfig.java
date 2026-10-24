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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

public class TestGitConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GitConfig.class)
                .setUrl(null)
                .setRepositories(List.of())
                .setUsername(null)
                .setPassword(null)
                .setCacheDirectory(Path.of(System.getProperty("java.io.tmpdir"), "trino-git").toFile())
                .setFetchInterval(new Duration(1, MINUTES))
                .setTimeout(new Duration(5, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("git.url", "https://example.com/repo.git")
                .put("git.repositories", "trino=https://github.com/trinodb/trino.git, airlift=https://github.com/airlift/airlift.git?a=b")
                .put("git.username", "user")
                .put("git.password", "secret")
                .put("git.cache-directory", "/var/cache/trino-git")
                .put("git.fetch-interval", "10m")
                .put("git.timeout", "30s")
                .buildOrThrow();

        GitConfig expected = new GitConfig()
                .setUrl(URI.create("https://example.com/repo.git"))
                .setRepositories(List.of("trino=https://github.com/trinodb/trino.git", "airlift=https://github.com/airlift/airlift.git?a=b"))
                .setUsername("user")
                .setPassword("secret")
                .setCacheDirectory(Path.of("/var/cache/trino-git").toFile())
                .setFetchInterval(new Duration(10, MINUTES))
                .setTimeout(new Duration(30, SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyPropertyMappings()
    {
        assertDeprecatedEquivalence(
                GitConfig.class,
                Map.of("git.url", "https://example.com/repo.git"),
                Map.of("metadata-uri", "https://example.com/repo.git"));
    }

    @Test
    public void testCredentialsInUrl()
    {
        assertFailsValidation(
                new GitConfig().setUrl(URI.create("https://user:secret@example.com/repo.git")),
                "urlWithoutCredentials",
                "git.url and git.repositories must not contain credentials, use git.username and git.password instead",
                AssertTrue.class);
        assertFailsValidation(
                new GitConfig().setUrl(URI.create("https://token@example.com/repo.git")),
                "urlWithoutCredentials",
                "git.url and git.repositories must not contain credentials, use git.username and git.password instead",
                AssertTrue.class);
        assertValidates(new GitConfig().setUrl(URI.create("ssh://git@example.com/repo.git")));
        assertValidates(new GitConfig().setUrl(URI.create("https://example.com/repo.git")));
    }

    @Test
    public void testRepositories()
    {
        assertThat(new GitConfig()
                .setUrl(URI.create("https://example.com/repo.git"))
                .setRepositories(List.of("trino=https://github.com/trinodb/trino.git", "airlift=https://github.com/airlift/airlift.git?a=b"))
                .getAllRepositories())
                .containsExactly(
                        entry("default", URI.create("https://example.com/repo.git")),
                        entry("trino", URI.create("https://github.com/trinodb/trino.git")),
                        entry("airlift", URI.create("https://github.com/airlift/airlift.git?a=b")));
        assertThatThrownBy(() -> new GitConfig().setRepositories(List.of("https://github.com/trinodb/trino.git")))
                .hasMessage("Invalid repository, expected schema=url: https://github.com/trinodb/trino.git");
        assertThatThrownBy(() -> new GitConfig().setRepositories(List.of("Trino=https://github.com/trinodb/trino.git")))
                .hasMessage("Invalid schema name, it can contain only lowercase letters, digits and underscores: Trino");
    }

    @Test
    public void testRepositoryRequired()
    {
        assertFailsValidation(new GitConfig(), "anyRepositoryConfigured", "git.url or git.repositories must be set", AssertTrue.class);
        assertValidates(new GitConfig().setRepositories(List.of("trino=https://github.com/trinodb/trino.git")));
        assertFailsValidation(
                new GitConfig().setUrl(URI.create("https://example.com/repo.git")).setRepositories(List.of("default=https://github.com/trinodb/trino.git")),
                "defaultSchemaUnique",
                "git.repositories can't contain the default schema when git.url is set",
                AssertTrue.class);
        assertFailsValidation(
                new GitConfig().setRepositories(List.of("trino=https://user:secret@github.com/trinodb/trino.git")),
                "urlWithoutCredentials",
                "git.url and git.repositories must not contain credentials, use git.username and git.password instead",
                AssertTrue.class);
    }
}
