DROP database if exists analytics;
CREATE DATABASE analytics;
USE analytics;

CREATE TABLE entities (
    ts DATETIME(6) NOT NULL,
    id BIGINT NOT NULL,
    kind TEXT NOT NULL,

    SORT KEY (ts, id),
    SHARD (ts, id),
    KEY (kind) USING HASH
);

CREATE TABLE edges (
    ts DATETIME(6) NOT NULL,
    id BIGINT NOT NULL,

    k TEXT NOT NULL,

    i BIGINT,
    d DOUBLE,
    t TEXT,

    SORT KEY (ts, id, k),
    SHARD (ts, id)
);

delete from entities;
delete from edges;

insert into entities set
    ts = "2022-01-01 00:00:00",
    id = 1,
    kind = "visitor";

insert into edges set
    ts = "2022-01-01 00:00:00",
    id = 1,
    k = "email",
    t = "jesse@example";

insert into entities set
    ts = "2022-01-01 00:00:00",
    id = 2,
    kind = "visitor";

insert into edges set
    ts = "2022-01-01 00:00:00",
    id = 2,
    k = "email",
    t = "carl@example";

insert into entities values ("2022-01-01 01:00:00", 3, "purchase");
insert into edges values ("2022-01-01 01:00:00", 3, "item", null, null, "t-shirt");
insert into edges values ("2022-01-01 01:00:00", 3, "color", null, null, "red");
insert into edges values ("2022-01-01 01:00:00", 3, "visitor", 1, null, null);

insert into entities values ("2022-01-01 01:00:00", 4, "purchase");
insert into edges values ("2022-01-01 01:00:00", 4, "item", null, null, "t-shirt");
insert into edges values ("2022-01-01 01:00:00", 4, "color", null, null, "blue");
insert into edges values ("2022-01-01 01:00:00", 4, "visitor", 1, null, null);


with
    visitors as (
        SELECT entities.id
        FROM
            entities, edges
        WHERE
            entities.ts = edges.ts
            and entities.id = edges.id
            and entities.kind = "visitor"

            and edges.k = "email" and edges.t like "jesse%"
        group by entities.ts, entities.id
    ),
    events as (
        select
            entities.ts,
            entities.id,
            row_number() over (partition by edges.ts, edges.id) as rownum
        from entities, edges
        where
            entities.ts = edges.ts
            and entities.id = edges.id
            and entities.kind = "purchase"
            and (
                (edges.k = "item" and edges.t = "t-shirt")
                or (edges.k = "color" and edges.t = "red")
                or (edges.k = "visitor" and edges.i in (select id from visitors))
            )
    )
select * from events where rownum = 3;
