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

import com.google.common.collect.ImmutableListMultimap;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.eclipse.jgit.lib.Constants.HEAD;

/**
 * Maps names and emails used in commits to the ones people want to be known by, like git does with a .mailmap file.
 *
 * @see <a href="https://git-scm.com/docs/gitmailmap">gitmailmap</a>
 */
public final class Mailmap
{
    public static final Mailmap EMPTY = new Mailmap(ImmutableListMultimap.of());

    // Proper Name <proper@email> Commit Name <commit@email>, where all but the last email are optional
    private static final Pattern ENTRY = Pattern.compile("([^<]*)<([^>]*)>\\s*(?:([^<]*)<([^>]*)>)?\\s*");

    private final ImmutableListMultimap<String, Entry> entries;

    private Mailmap(ImmutableListMultimap<String, Entry> entries)
    {
        this.entries = requireNonNull(entries, "entries is null");
    }

    public static Mailmap read(Repository repository)
            throws IOException
    {
        // like git in a bare repository, read the file from HEAD
        try (RevWalk walk = new RevWalk(repository)) {
            if (repository.resolve(HEAD) == null) {
                return EMPTY;
            }
            RevCommit head = walk.parseCommit(repository.resolve(HEAD));
            try (TreeWalk treeWalk = TreeWalk.forPath(repository, ".mailmap", head.getTree())) {
                if (treeWalk == null) {
                    return EMPTY;
                }
                ObjectLoader loader = repository.open(treeWalk.getObjectId(0));
                return parse(new String(loader.getCachedBytes(), UTF_8));
            }
        }
    }

    public static Mailmap parse(String contents)
    {
        ImmutableListMultimap.Builder<String, Entry> entries = ImmutableListMultimap.builder();
        for (String line : contents.lines().toList()) {
            int comment = line.indexOf('#');
            if (comment >= 0) {
                line = line.substring(0, comment);
            }
            Matcher matcher = ENTRY.matcher(line.strip());
            if (!matcher.matches()) {
                continue;
            }
            Optional<String> properName = nonEmpty(matcher.group(1));
            if (matcher.group(4) == null) {
                // Proper Name <commit@email>
                entries.put(matcher.group(2).strip().toLowerCase(ENGLISH), new Entry(properName, Optional.empty(), Optional.empty()));
                continue;
            }
            // [Proper Name] <proper@email> [Commit Name] <commit@email>
            entries.put(
                    matcher.group(4).strip().toLowerCase(ENGLISH),
                    new Entry(properName, nonEmpty(matcher.group(2)), nonEmpty(matcher.group(3)).map(name -> name.toLowerCase(ENGLISH))));
        }
        return new Mailmap(entries.build());
    }

    public Identity map(Identity identity)
    {
        List<Entry> candidates = entries.get(identity.email().toLowerCase(ENGLISH));
        String commitName = identity.name().toLowerCase(ENGLISH);
        // entries matching both the name and the email take precedence over the ones matching only the email
        Optional<Entry> entry = candidates.stream()
                .filter(candidate -> candidate.commitName().map(commitName::equals).orElse(false))
                .findFirst()
                .or(() -> candidates.stream().filter(candidate -> candidate.commitName().isEmpty()).findFirst());
        return entry
                .map(match -> new Identity(match.properName().orElse(identity.name()), match.properEmail().orElse(identity.email())))
                .orElse(identity);
    }

    private static Optional<String> nonEmpty(String value)
    {
        return Optional.ofNullable(value).map(String::strip).filter(stripped -> !stripped.isEmpty());
    }

    public record Identity(String name, String email)
    {
        public Identity
        {
            requireNonNull(name, "name is null");
            requireNonNull(email, "email is null");
        }
    }

    private record Entry(Optional<String> properName, Optional<String> properEmail, Optional<String> commitName) {}
}
