DROP database if exists analytics_twocol;
CREATE DATABASE analytics_twocol;
USE analytics_twocol;

CREATE TABLE events (
    ts DATETIME(6) NOT NULL,
    id BINARY(36) NOT NULL,
    event_name TEXT NOT NULL,

    SORT KEY (ts, id),
    SHARD (ts, id),
    KEY (event_name) USING HASH
);

CREATE TABLE properties (
    ts DATETIME(6) NOT NULL,
    id BINARY(36) NOT NULL,
    k TEXT NOT NULL,
    v TEXT NOT NULL,

    SORT KEY (ts, id, k),
    SHARD (ts, id),
    KEY (k, v) USING HASH
);

SELECT event_name, events.ts
FROM events
LEFT JOIN properties ON
    events.ts = properties.ts
    AND events.id = properties.id
WHERE
    (
        properties.k = "item" and properties.v = "T-Shirt"
        or
        properties.k = "color" and properties.v = "red"
    )
    and events.ts between "2021-09-21 18:20:00" and "2021-09-21 18:25:00"
group by properties.ts, properties.id
having count(properties.id) = 2;