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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MINUTES;

public class GitConfig
{
    public static final String DEFAULT_SCHEMA = "default";

    private static final Pattern SCHEMA_NAME = Pattern.compile("[a-z0-9_]+");

    private URI url;
    private Map<String, URI> repositories = ImmutableMap.of();
    private String username;
    private String password;
    private File cacheDirectory = Path.of(System.getProperty("java.io.tmpdir"), "trino-git").toFile();
    private Duration fetchInterval = new Duration(1, MINUTES);
    private Duration timeout = new Duration(5, MINUTES);

    public URI getUrl()
    {
        return url;
    }

    @Config("git.url")
    @LegacyConfig("metadata-uri")
    @ConfigDescription("URL of the git repository, available as the default schema")
    public GitConfig setUrl(URI url)
    {
        this.url = url;
        return this;
    }

    public Map<String, URI> getRepositories()
    {
        return repositories;
    }

    @Config("git.repositories")
    @ConfigDescription("Comma separated schema=url pairs, for every git repository available as a schema")
    public GitConfig setRepositories(List<String> repositories)
    {
        ImmutableMap.Builder<String, URI> builder = ImmutableMap.builder();
        for (String entry : repositories) {
            // URLs can contain = too, so only the first one separates the schema name
            int separator = entry.indexOf('=');
            checkArgument(separator > 0, "Invalid repository, expected schema=url: %s", entry);
            String schemaName = entry.substring(0, separator).strip();
            checkArgument(SCHEMA_NAME.matcher(schemaName).matches(), "Invalid schema name, it can contain only lowercase letters, digits and underscores: %s", schemaName);
            builder.put(schemaName, URI.create(entry.substring(separator + 1).strip()));
        }
        this.repositories = builder.buildOrThrow();
        return this;
    }

    /**
     * Repositories by the name of the schema they are available as.
     */
    public Map<String, URI> getAllRepositories()
    {
        if (url == null) {
            return repositories;
        }
        return ImmutableMap.<String, URI>builder()
                .put(DEFAULT_SCHEMA, url)
                .putAll(repositories)
                .buildKeepingLast();
    }

    @AssertTrue(message = "git.url or git.repositories must be set")
    public boolean isAnyRepositoryConfigured()
    {
        return url != null || !repositories.isEmpty();
    }

    @AssertTrue(message = "git.repositories can't contain the default schema when git.url is set")
    public boolean isDefaultSchemaUnique()
    {
        return url == null || !repositories.containsKey(DEFAULT_SCHEMA);
    }

    @AssertTrue(message = "git.url and git.repositories must not contain credentials, use git.username and git.password instead")
    public boolean isUrlWithoutCredentials()
    {
        return getAllRepositories().values().stream().allMatch(GitConfig::isWithoutCredentials);
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("git.username")
    @ConfigDescription("Username used to authenticate to the git repository")
    public GitConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("git.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password or access token used to authenticate to the git repository")
    public GitConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public File getCacheDirectory()
    {
        return cacheDirectory;
    }

    @Config("git.cache-directory")
    @ConfigDescription("Directory where repositories are cloned, accessible only by the user running Trino")
    public GitConfig setCacheDirectory(File cacheDirectory)
    {
        this.cacheDirectory = cacheDirectory;
        return this;
    }

    @NotNull
    public Duration getFetchInterval()
    {
        return fetchInterval;
    }

    @Config("git.fetch-interval")
    @ConfigDescription("Minimum time between fetching new commits from a repository, on every node")
    public GitConfig setFetchInterval(Duration fetchInterval)
    {
        this.fetchInterval = fetchInterval;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("git.timeout")
    @ConfigDescription("Timeout for network operations when cloning or fetching a repository")
    public GitConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    private static boolean isWithoutCredentials(URI url)
    {
        // usernames in SSH URLs, like ssh://git@example.com/repo.git, are not secret
        if (url.getUserInfo() == null) {
            return true;
        }
        return !"http".equalsIgnoreCase(url.getScheme()) && !"https".equalsIgnoreCase(url.getScheme()) && !url.getUserInfo().contains(":");
    }
}
