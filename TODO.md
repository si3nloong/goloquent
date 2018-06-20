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
- Changed params in function `RunInTransaction` from `*goloquent.Connection` to `*goloquent.DB`
- Changed function `LockForUpdate` to `WLock`
- Changed function `LockForShared` to `RLock`
- Changed function single entity `Update` to `Save`
- Change `Loader` interface `Load([]datastore.Property) error` to `Load() error`
- Change `Saver` interface `Save() ([]datastore.Property,error)` to `Save() error`
- No longer support mysql 5.6 and below (at least 5.7)
- Change second parameter **parentKey** `*datastore.Key` to optional on function `Create` nor `Upsert`

# New Features

- Introduced `Select` function
- Introduced `DistinctOn` function
- Introduced `Lock` function
- Introduced `Truncate` function
- Introduced `Flush` function
- Introduced package `db`
- Introduced package `qson` (Query JSON)
- Replaced statement debug using `LogHandler`
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
- ParseQuery (ok)
- Logger (ok)
- SoftDelete (ok)
- TestCase (5%)

// TODO:

- Cursor
- Filter json
- Filter geolocation

Bugs :
Flatten struct childs values always null (fixed)
Flatten []struct childs values always null
Geolocation
Selected fields should follow sequence
