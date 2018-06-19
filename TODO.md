### Bug / Issue

- Set primary key on insert
- Primary key at insertion is wrong, always create new key
- Load relative / Eager loading
- Not checking present of `__key__`
- Not checking `SoftDelete`

# Breaking Changes

- Dropped function `Count`
- Dropped function `Union`
- Dropped function single `Update`
- Dropped function `SetDebug(boolean)`
  <!-- - Dropped tag option `unsigned` support -->
- Dropped datastore support
- `Delete` function using entity model instead of `*datastore.Key`
- Changed function `LockForUpdate` to `WLock`
- Changed function `LockForShared` to `RLock`
- No longer support mysql 5.6 and below (at least 5.7)

# New Features

- Introduced `Select` function
- Introduced `DistinctOn` function
- Introduced `Lock` function
- Introduced `Truncate` function
- Introduced package `db`
- Introduced package `qson` (Query JSON)
- Replaced statement debug using `LogHandler`
- Replaced single entity `Update` to `Save`
- Eager loading `Related` function
- Support unsigned integer, uint, uint8, uint16, uint32, uint64
- Support any pointer of base data type and struct
- Support **Postgres**

### Pending

- Create (ok)
- Upsert (ok)
- Save (ok)
- UpdateMulti (ok)
- Delete Multi (ok)
- Debug
- ParseQuery (ok)
- TestCase (5%)
- Logger (ok)
- Cursor

// TODO:

- add soft delete for get

Bugs :
Flatten struct childs values always null (fixed)
Flatten []struct childs values always null
Geolocation
