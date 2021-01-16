WITH pairs AS (
    SELECT
        DISTINCT lower(email) AS email,
        name
    FROM (
            SELECT
                author_name AS name,
                author_email AS email
            FROM commits
            GROUP BY 1, 2
        UNION
            SELECT
                committer_name AS name,
                committer_email AS email
            FROM commits
            GROUP BY 1, 2
        ORDER BY count(*) DESC
    ) a
),
-- incomplete name groups with complete (and duplicate) email groups
emails AS (
    SELECT p1.email,
        array_agg(p1.name ORDER BY p1.name) AS names,
        array_agg(DISTINCT p2.email ORDER BY p2.email) AS emails
    FROM pairs p1
    LEFT JOIN pairs p2 ON p1.name = p2.name
    GROUP BY p1.email
),
-- incomplete email groups with complete (and duplicate) name groups
names AS (
    SELECT p1.name,
        array_agg(p1.email ORDER BY p1.email) AS emails,
        array_agg(DISTINCT p2.name ORDER BY p2.name) AS names
    FROM pairs p1
    LEFT JOIN pairs p2 ON p1.email = p2.email
    GROUP BY p1.name
),
-- join all complete groups and remove duplicates
grouped AS (
    SELECT DISTINCT e.emails, n.names
    FROM emails e
    LEFT JOIN names n ON CONTAINS(e.names, n.name) OR CONTAINS(n.emails, e.email)
)

SELECT
    emails[1] AS email,
    names[1] AS name,
    slice(emails, 2, cardinality(emails)) AS extra_emails,
    slice(names, 2, cardinality(emails)) AS extra_names
FROM grouped
ORDER BY name, names