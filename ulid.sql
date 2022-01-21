DROP database if exists analytics_ulid;
CREATE DATABASE analytics_ulid;
USE analytics_ulid;

CREATE TABLE events (
    ts DATETIME(6) NOT NULL,
    ulid BINARY(128) NOT NULL,
    event_name TEXT NOT NULL,

    SORT KEY (ulid, event_name),
    SHARD (ulid),
    KEY (event_name) USING HASH
);

CREATE TABLE properties (
    ulid BINARY(128) NOT NULL,
    k TEXT NOT NULL,
    v TEXT NOT NULL,

    SORT KEY (ulid, k),
    SHARD (ulid),
    KEY (k) USING HASH
);

SELECT event_name, ts
FROM events
LEFT JOIN properties ON events.ulid = properties.ulid
WHERE
    (
        properties.k = "item" and properties.v = "T-Shirt"
        or
        properties.k = "color" and properties.v = "red"
    )
group by events.ulid
having count(properties.ulid) = 2;

-- and events.ulid >= "01FG2ZKJK30000000000000000" and events.ulid <= "01FG2ZKTZ60000000000000000"

select * from (
select ulid, item, color
from (select * from properties) as pivot_q
pivot (any_value(v) FOR k in ("item", "color"))
px) x
order by x.ulid asc;