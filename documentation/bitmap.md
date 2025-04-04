# Internal bitmap block design

Use 16 bit block counters to track pending writes to each "chunk".
The 2 high order bits are special-purpose, the first is a flag indicating
whether a resync is needed.  The second is a flag indicating whether a
resync is active. This means that the counter is actually 14 bits:

| resync_needed | resync_active |   counter |
|     :----:    |     :----:    |   :----:  |
|     (0-1)     |      (0-1)    | (0-16383) |

The `resync_needed` bit is set when:
- a `1` bit is read from storage at startup;
- a write request fails on some drives;
- a resync is aborted on a chunk with `resync_active` set;
- It is cleared (and `resync_active` set) when a resync starts across all drives of the chunk.

The `resync_active` bit is set when:
- a resync is started on all drives, and `resync_needed` is set.
- `resync_needed` will be cleared (as long as `resync_active` wasn't already set).
- It is cleared when a resync completes.

The counter counts pending write requests, plus the on-disk bit.
When the counter is `1` and the resync bits are clear, the on-disk
bit can be cleared as well, thus setting the counter to `0`.
When we set a bit, or in the counter (to start a write), if the fields is
`0`, we first set the disk bit and set the counter to `1`.

If the counter is `0`, the on-disk bit is clear and the stipe is clean
Anything that dirties the stipe pushes the counter to `2` (at least)
and sets the on-disk bit (lazily).
If a periodic sweep find the counter at `2`, it is decremented to `1`.
If the sweep find the counter at `1`, the on-disk bit is cleared and the
counter goes to `0`.

Also, we'll hijack the "map" pointer itself and use it as two 16 bit block
counters as a fallback when "page" memory cannot be allocated:

Normal case (page memory allocated):

page pointer (32-bit)

     [ ] ------+
               |
               +-------> [   ][   ]..[   ] (4096 byte page == 2048 counters)
                          c1   c2    c2048

 Hijacked case (page memory allocation failed):

     hijacked page pointer (32-bit)

     [		  ][		  ] (no page memory allocated)
      counter #1 (16-bit) counter #2 (16-bit)


## Notes:
1. bitmap_super_s->events counter is updated before the event counter in the md superblock;
   When a bitmap is loaded, it is only accepted if this event counter is equal
   to, or one greater than, the event counter in the superblock.
2. bitmap_super_s->events is updated when the other one is `if` and `only if` the
   array is not degraded.  As bits are not cleared when the array is degraded,
   this represents the last time that any bits were cleared. If a device is being
   added that has an event count with this value or higher, it is accepted
   as conforming to the bitmap.
3. bitmap_super_s->chunksize is the number of sectors represented by the bitmap,
   and is the range that  resync happens across.  For raid1 and raid5/6 it is the
   size of individual devices.  For raid10 it is the size of the array.
