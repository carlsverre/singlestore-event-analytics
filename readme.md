# Evaluation of sortable ID generation schemes

## ULID

**Repo:** https://github.com/ulid/spec
**Size:** 128 bits (16 bytes)
**Text Repr:** 26-character Base32
**Sorting:** binary (numeric) text (lexicographic)
**Time granularity:** 48 bit (unix epoch, milliseconds)

Schema:

    48 bit timestamp, big-endian
    80 bit random noise

## KSUID

**Repo:** https://github.com/segmentio/ksuid
**Size:** 160 bits (20 bytes)
**Text Repr:** 27-character Base62
**Sorting:** binary (numeric) text (lexicographic)
**Time granularity:** 32 bit (custom epoch, seconds)

KSUID is used by Segment and offers a higher amount of collision avoidance than ULID (due to a full 128 bits of random noise).

Schema:

    32 bit timestamp, big-endian
    128 bit random noise

## Sonyflake

**Repo:** https://github.com/sony/sonyflake
**Size:** 64 bits (8 bytes)
**Text Repr:** no canonical text repr
**Sorting:** binary (numeric)
**Time granularity:** 39 bit (custom epoch, 10 msec)

Sonyflake was inspired by Twitter's snowflake. Sonyflake uses the following schema:

    39 bits for time in units of 10 msec
    8 bits for a sequence number
    16 bits for a machine id

A major difference between sonyflake/snowflake and the other two ID generators is the need for coordinating machine ids. Each process generating IDs needs a unique machine id to ensure that ID's don't collide when generated in the same 10 msec tick.