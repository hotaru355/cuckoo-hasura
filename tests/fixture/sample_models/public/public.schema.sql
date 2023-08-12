SET
    check_function_bodies = false;

CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at()
	RETURNS TRIGGER
	LANGUAGE plpgsql
    AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$;

DROP TABLE IF EXISTS authors CASCADE;
CREATE TABLE authors (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    age int,
	home_zone public.geometry NULL,
    jsonb_list jsonb,
    jsonb_dict jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by uuid NOT NULL,
    deleted_at timestamp with time zone,
    deleted_by uuid
);
ALTER TABLE ONLY authors
    DROP CONSTRAINT IF EXISTS authors_pkey;
ALTER TABLE ONLY authors
    ADD CONSTRAINT authors_pkey PRIMARY KEY (uuid);
CREATE TRIGGER set_author_updated_at 
    BEFORE UPDATE ON authors
    FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();

DROP TABLE IF EXISTS articles CASCADE;
CREATE TABLE articles (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    author_uuid uuid NOT NULL,
    title text NOT NULL,
    word_count int,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by uuid NOT NULL,
    deleted_at timestamp with time zone,
    deleted_by uuid
);
ALTER TABLE ONLY articles
    ADD CONSTRAINT articles_pk PRIMARY KEY (uuid);
ALTER TABLE ONLY articles
    ADD CONSTRAINT articles_authors_fk FOREIGN KEY (author_uuid) REFERENCES authors(uuid) ON UPDATE CASCADE ON DELETE CASCADE;

DROP TABLE IF EXISTS comments CASCADE;
CREATE TABLE comments (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    article_uuid uuid NOT NULL,
    content text NOT NULL,
    likes int DEFAULT 0,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by uuid NOT NULL,
    deleted_at timestamp with time zone,
    deleted_by uuid
);
ALTER TABLE ONLY comments
    ADD CONSTRAINT comments_pk PRIMARY KEY (uuid);
ALTER TABLE ONLY comments
    ADD CONSTRAINT comments_articles_fk FOREIGN KEY (article_uuid) REFERENCES articles(uuid) ON UPDATE CASCADE ON DELETE CASCADE;


DROP TABLE IF EXISTS addresses CASCADE;
CREATE TABLE addresses (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    street text NOT NULL,
    postal_code text NOT NULL,
    walk_score numeric,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by uuid NOT NULL,
    deleted_at timestamp with time zone,
    deleted_by uuid
);
ALTER TABLE ONLY addresses
    ADD CONSTRAINT addresses_pk PRIMARY KEY (uuid);


DROP TABLE IF EXISTS author_details CASCADE;
CREATE TABLE author_details (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    author_uuid uuid NOT NULL,
    primary_address_uuid uuid,
    secondary_address_uuid uuid,
    country text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by uuid NOT NULL,
    deleted_at timestamp with time zone,
    deleted_by uuid
);
ALTER TABLE ONLY author_details
    ADD CONSTRAINT author_details_pk PRIMARY KEY (uuid);
ALTER TABLE ONLY author_details
    ADD CONSTRAINT author_details_authors_uuid_uq UNIQUE (author_uuid);
ALTER TABLE ONLY author_details
    ADD CONSTRAINT author_details_authors_fk
    FOREIGN KEY (author_uuid)
    REFERENCES authors(uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE;
ALTER TABLE ONLY author_details
    ADD CONSTRAINT author_details_primary_address_fk
    FOREIGN KEY (primary_address_uuid)
    REFERENCES addresses(uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE;
ALTER TABLE ONLY author_details
    ADD CONSTRAINT author_details_secondary_address_fk
    FOREIGN KEY (secondary_address_uuid)
    REFERENCES addresses(uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE;

DROP TABLE IF EXISTS details_addresses CASCADE;
CREATE TABLE details_addresses (
    uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    author_detail_uuid uuid NOT NULL,
    address_uuid uuid NOT NULL,
    is_primary boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL
);
ALTER TABLE ONLY details_addresses
    ADD CONSTRAINT details_addresses_pk PRIMARY KEY (uuid);
ALTER TABLE ONLY details_addresses
    ADD CONSTRAINT details_addresses_author_details_fk
    FOREIGN KEY (author_detail_uuid)
    REFERENCES author_details(uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE;
ALTER TABLE ONLY details_addresses
    ADD CONSTRAINT details_addresses_address_fk
    FOREIGN KEY (address_uuid)
    REFERENCES addresses(uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE;

CREATE OR REPLACE FUNCTION find_most_commented_author(
    author_uuids uuid ARRAY default NULL
)
RETURNS authors
AS $$
    SELECT 
        au.*
    FROM authors AS au
    LEFT JOIN articles AS ar on au.uuid = ar.author_uuid
    LEFT JOIN comments AS co on co.article_uuid = ar.uuid
    WHERE author_uuids IS NULL OR au.uuid=ANY(author_uuids)
    GROUP BY au.uuid
    ORDER BY COUNT(co.uuid) desc, au.name ASC
    LIMIT 1
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION find_authors_with_articles(
    min_article_count numeric default 1
)
RETURNS SETOF authors
AS $$
    SELECT 
        au.*
    FROM authors AS au
    LEFT JOIN articles AS ar on au.uuid = ar.author_uuid
    GROUP BY au.uuid
    HAVING COUNT(ar.uuid) >= min_article_count
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION inc_author_age(
    author_uuid uuid,
    user_uuid uuid,
    only_if_older_than numeric default 1,
    has_articles boolean default true,
    updated_home_zone geometry default NULL
)
RETURNS authors
AS $$
    UPDATE authors
        SET
            age=authors.age + 1,
            home_zone=(
                CASE
                    WHEN updated_home_zone IS NULL THEN authors.home_zone
                    WHEN updated_home_zone IS NOT NULL THEN updated_home_zone
                END
            ),
            updated_by=user_uuid
    WHERE
        (
            (
                has_articles IS false
                AND NOT EXISTS(
                    SELECT 1
                    FROM articles ar
                    WHERE ar.author_uuid = authors.uuid
                )
            ) OR (
                has_articles IS true
                AND EXISTS(
                    SELECT 1
                    FROM articles ar
                    WHERE ar.author_uuid = authors.uuid
                )
            )
        )
        AND authors.uuid = author_uuid
        AND authors.age > only_if_older_than
    RETURNING *;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION inc_all_authors_age(
    author_uuids uuid ARRAY,
    user_uuid uuid
)
RETURNS SETOF authors
AS $$
    UPDATE authors
        SET
            age=authors.age + 1,
            updated_by=user_uuid
    WHERE authors.uuid = ANY(author_uuids)
    RETURNING *;
$$ LANGUAGE SQL;
