-- This can produce too many stages, see the queries in the examples dir
-- on how to break it down using temporary tables
WITH RECURSIVE
nodes (email, name) AS (
    SELECT DISTINCT author_email, author_name
    FROM commits
    UNION
    SELECT DISTINCT committer_email, committer_name
    FROM commits
),
edges (name1, name2) AS (
    SELECT n1.name, n2.name
    FROM nodes n1
    INNER JOIN nodes n2 USING (email)
),
walk (name1, name2, visited) AS (
    SELECT name1, name2, ARRAY[name1]
    FROM edges
    WHERE name1 = name2
    UNION ALL
    SELECT w.name1, e.name2, w.visited || e.name2
    FROM walk w
    INNER JOIN edges e ON e.name1 = w.name2
    WHERE NOT contains(w.visited, e.name2)
),
result (name1, name2s) AS (
    SELECT name1, array_agg(DISTINCT name2 ORDER BY name2)
    FROM walk
    GROUP BY name1
),
grouped (names, emails) AS (
    SELECT
        array_agg(DISTINCT n.name ORDER BY n.name) AS names,
        array_agg(DISTINCT n.email ORDER BY n.email) AS emails
    FROM result r
    INNER JOIN nodes n ON n.name = r.name1
    GROUP BY r.name2s;
)
SELECT
    emails[1] AS email,
    names[1] AS name,
    slice(emails, 2, cardinality(emails)) AS extra_emails,
    slice(names, 2, cardinality(emails)) AS extra_names
FROM grouped
ORDER BY name, names
