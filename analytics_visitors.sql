DROP database if exists analytics;
CREATE DATABASE analytics;
USE analytics;

create table visitors (
    visitor_id BIGINT NOT NULL,
    props JSON NOT NULL,

    PRIMARY KEY (visitor_id)
);

CREATE TABLE events (
    ts DATETIME(6) NOT NULL,
    visitor_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    kind TEXT NOT NULL,

    SORT KEY (ts, visitor_id, event_id),
    SHARD (visitor_id),
    KEY (kind) USING HASH
);

CREATE TABLE properties (
    ts DATETIME(6) NOT NULL,
    visitor_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,

    k TEXT NOT NULL,

    d DOUBLE,
    t TEXT,

    SORT KEY (ts, visitor_id, event_id, k),
    SHARD (visitor_id)
);

insert into visitors set
    visitor_id = 1,
    props = '{"name": "jesse", "email": "jesse@example"}';

insert into visitors set
    visitor_id = 2,
    props = '{"name": "carl", "email": "carl@example"}';

delete from events;
delete from properties;

insert into events values ("2022-01-01 01:00:00", 1, 1, "purchase");
insert into properties values ("2022-01-01 01:00:00", 1, 1, "item", null, "t-shirt");
insert into properties values ("2022-01-01 01:00:00", 1, 1, "color", null, "red");
insert into properties values ("2022-01-01 01:00:00", 1, 1, "count", 3, null);

insert into events values ("2022-01-01 01:00:00", 2, 2, "purchase");
insert into properties values ("2022-01-01 01:00:00", 2, 2, "item", null, "t-shirt");
insert into properties values ("2022-01-01 01:00:00", 2, 2, "color", null, "blue");
-- user messed up and used a string rather than a number
insert into properties values ("2022-01-01 01:00:00", 2, 2, "count", null, "5");

with
    visitors as (
        SELECT *
        FROM visitors
        WHERE props::$email like "jesse@%" or props::$name = "carl"
    ),
    events as (
        select
            events.ts,
            events.visitor_id,
            events.event_id,
            row_number() over (partition by properties.ts, properties.visitor_id, properties.event_id) as rownum
        from events, properties, visitors
        where
            events.ts = properties.ts
            and events.visitor_id = properties.visitor_id
            and events.visitor_id = visitors.visitor_id
            and events.event_id = properties.event_id
            and events.kind = "purchase"
            and (
                (properties.k = "item" and properties.t = "t-shirt")
                or (properties.k = "color" and properties.t in ("red", "blue"))
                or (properties.k = "count" and coalesce(properties.d, properties.t):>double > 2)
            )
    )
select ts, visitor_id, event_id from events where rownum = 3;
