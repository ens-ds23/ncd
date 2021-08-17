# Introduction

ncd is a dbm-like file designed for serverless remote access. It implements a bytes/bytes key/value store in the
context of a remote static resource. It is designed for one-off generation, not for incremental update and for infrequent queries focusing on read latency rather than banwidth.

That is the access pattern is expected to:
  * have a very high latency;
  * have a moderate bandwidth;
  * relatively large amounts of CPU available;
  * have a complete client-determined request offset and size;
  * not have any cacheing implications beyond the requested data.

An example would be byte-range requests to an HTTP resource. The format aims to minimise the number of accesses to the data (typically to one) configurably offset against wasted space within the file.

ncd is based on Bernstein's cdm format, itself derived from the many dbm formats.

# Format

The algorithm is hash based. A file contains a sequence of pages. A hash of the key is prepared and from that the correct page for the data is determined. The page contains two sections, the heap ant the table. The table is a sub-hashtable which uses further bits from the hash of the key, stored using open addressing. These entries point to data in the heap. 

The entry in the heap contains one of two value types. Internal data comprises the length of the key (plus one) in lesqlite2 format followed by the length of the value, and then the bytes of the key and value. External data comprises the value 0 as a lesqlite2 integer followed by an eight byte offset and an eight byte size for the data. This points to the data in the file which is formatted as for internal values at that location.

The heap comes first in a page and then the table. The page size is limited to 2**32 bytes. Values in the table are fixed four-byte pointers relative to the page start. The all-ones value is used to mean "empty" in the table and this is guaranteed to be in the table part of the page and therefore invalid as a pointer.

The size of the table and heap per page are separately ocnfigurable as is the total number of pages. The values chosen go into the header. The header is the frist few bytes of the file and so overlaps with the start of the heap of the first page which therefore is reserved and unavailable to the table of the first page for data.

```
File:
+------------------------+
| Page 0          heap 0 |
|                 .......|
|                table 0 |
+------------------------+
| Page 1          heap 1 |
|                 .......|
|                table 1 |
+------------------------+
| Page 2          heap 2 |
|                 .......|
|                table 2 |
+------------------------+
            .
            .
            .
+------------------------+
| further space available|
| for large external     |
| values                 |


page:

+---------------------------------------+
| entry 0 | entry 1 | ...               |
+---------------------------------------+
entries are single 4-byte pointers relative to page. 0xFFFFFFF is unused.

internal heap entry at offset X within page:

          X
+---------------------------------------+
| ..... |key-len+1|value-len| key bytes >
<>    | value bytes  | ....             |
+---------------------------------------+

external heap entry at offset X within page:

          X
+---------------------------------------+
| .....  | len=0 | 8-byte-offset     |  >
< 8-byte-size     |                     |
+---------------------------------------+

header:

heap 0:
+--------------------------------------+
| header     | first bytes avaiable    >
< as heap  ...                         |
+--------------------------------------+

header:
+--------------------------------------+
| magic num (4)    | version/flags (4) |
| number of pages (8)                  |
| heap size byt (4)| table size enty(4)|
+--------------------------------------+
```

The hash function used is the x64 variant of 128-bit murmur3. All storage is little-endian.

# Construction

Three configuration parameters determine the appropriate size of the heaps/tables and so the number of pages.

1. an acceptable "small change" value (in bytes) which is a sensible amount of data blow which gains in transmission time are negligible acrossthe net (eg 8-64kB)
2. maximum "wasted space" in the file: proportionate size excess over the minimum required to store the data.
3. maximum ratio of external data (two reuests) to internal (one request).

The small-change value determines the combined size of a page directly. What remains is how many strings to store per page. After applying a hash-table load factor this directly determines the table size, leaving the rest for the heap. By adding values smallest-first the number which must be stored externally is calculated. External storage referecnes and free space both count as "wasted". External strings count for "double" references. The maximum number of strings which can be stored in a page is determined by the space in the heap for external references.
