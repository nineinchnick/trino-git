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
import com.google.common.collect.ImmutableSortedSet;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import pl.net.was.trino.git.Mailmap.Identity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static pl.net.was.trino.git.CommitWalks.walkCommits;

/**
 * Groups names and emails of authors and committers that belong to the same person:
 * all names used with the same email, and all emails used with the same name, transitively.
 */
public final class Identities
{
    private Identities() {}

    public static List<Group> group(Repository repository, Mailmap mailmap)
    {
        UnionFind<Key> groups = new UnionFind<>();
        Set<Identity> mapped = new HashSet<>();
        try (RevWalk revWalk = new RevWalk(repository)) {
            Iterator<RevCommit> commits = walkCommits(repository, revWalk, Optional.empty());
            while (commits.hasNext()) {
                RevCommit commit = commits.next();
                for (PersonIdent person : List.of(commit.getAuthorIdent(), commit.getCommitterIdent())) {
                    Identity identity = new Identity(person.getName(), person.getEmailAddress());
                    Identity mappedIdentity = mailmap.map(identity);
                    groups.union(Key.name(identity.name()), Key.email(identity.email()));
                    if (!mappedIdentity.equals(identity)) {
                        mapped.add(mappedIdentity);
                        groups.union(Key.name(identity.name()), Key.name(mappedIdentity.name()));
                        groups.union(Key.name(mappedIdentity.name()), Key.email(mappedIdentity.email()));
                    }
                }
            }
        }

        Map<Key, List<Key>> members = groups.groups();
        ImmutableList.Builder<Group> result = ImmutableList.builder();
        for (List<Key> keys : members.values()) {
            ImmutableSortedSet<String> names = keys.stream().filter(Key::isName).map(Key::value).collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.naturalOrder()));
            ImmutableSortedSet<String> emails = keys.stream().filter(key -> !key.isName()).map(Key::value).collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.naturalOrder()));
            // prefer the identity set in the mailmap, and otherwise the first one in alphabetical order
            Identity primary = mapped.stream()
                    .filter(identity -> names.contains(identity.name()) && emails.contains(identity.email()))
                    .min(Comparator.comparing(Identity::email).thenComparing(Identity::name))
                    .orElseGet(() -> new Identity(names.first(), emails.first()));
            result.add(new Group(
                    primary,
                    emails.stream().filter(email -> !email.equals(primary.email())).collect(toImmutableList()),
                    names.stream().filter(name -> !name.equals(primary.name())).collect(toImmutableList())));
        }
        return result.build();
    }

    public record Group(Identity primary, List<String> extraEmails, List<String> extraNames)
    {
        public Group
        {
            requireNonNull(primary, "primary is null");
            extraEmails = ImmutableList.copyOf(extraEmails);
            extraNames = ImmutableList.copyOf(extraNames);
        }
    }

    private record Key(boolean isName, String value)
    {
        static Key name(String value)
        {
            return new Key(true, value);
        }

        static Key email(String value)
        {
            return new Key(false, value);
        }
    }

    private static class UnionFind<T>
    {
        private final Map<T, T> parents = new HashMap<>();

        void union(T first, T second)
        {
            T firstRoot = find(first);
            T secondRoot = find(second);
            if (!firstRoot.equals(secondRoot)) {
                parents.put(firstRoot, secondRoot);
            }
        }

        Map<T, List<T>> groups()
        {
            Map<T, List<T>> groups = new HashMap<>();
            for (T element : List.copyOf(parents.keySet())) {
                groups.computeIfAbsent(find(element), _ -> new ArrayList<>()).add(element);
            }
            return groups;
        }

        private T find(T element)
        {
            T parent = parents.computeIfAbsent(element, _ -> element);
            if (parent.equals(element)) {
                return element;
            }
            T root = find(parent);
            parents.put(element, root);
            return root;
        }
    }
}
