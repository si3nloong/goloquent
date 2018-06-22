### Bug / Issue

- Set primary key on insert
- Primary key at insertion is wrong, always create new key
- Load relative / Eager loading
- Not checking present of `__key__`
- Not checking `SoftDelete`
- (2018-06-03) Fix empty table name when using `Flush` func.
- (2018-06-10) Fix panic occur on func `StringKey` when input parameter `*datastore.Key` is `nil`
- (2018-06-18) Primary key should omitted in operation `Upsert`
- (2018-06-18) Fix logger `String` func is output unexpected string when using `Postgres` driver
- (2018-06-19) Fix flatten struct bug, flatten column using root data type instead of the subsequent data type
- (2018-06-19) Fix primary key bug when using `WHERE $Key IN (?)`, key is not convert to primary key format
- (2018-06-21) Fix alter table character set and collation bug, change from `ALTER TABLE xxx CONVERT TO CHARACTER SET utf8` to `ALTER TABLE xxx CHARACTER SET utf8`
- (2018-06-21) Fix mysql panic even is 5.7 or above `eg: GAE return 5.7.14-google-log instead 5.7.14` will mismatch in the string comparison

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
- Change second parameter **parentKey** `*datastore.Key` to optional on function `Create` nor `Upsert`
- (2018-06-16) No longer support mysql 5.6 and below (at least 5.7)
- (2018-06-19) Table is now by default using `utf8mb4` encoding
- (2018-06-21) Support extra option `datatype`, `charset`, `collate` on struct property, but it only limited to datatype of `string`
- (2018-06-21) Allow `*` on func `Select`

# New Features

- Introduced `Select` func.
- Introduced `DistinctOn` func.
- Introduced `Lock` func.
- Introduced `Truncate` func.
- Introduced `Flush` func.
- Introduced package `db`.
- Introduced package `qson`. (Query JSON)
- Replaced statement debug using `LogHandler`.
- Support unsigned integer, uint, uint8, uint16, uint32, uint64
- Support any pointer of base data type and struct
- (2018-06-14) Support **Postgres**.
- (2018-06-18) Introduced `Scan` func.
- (2018-06-22) Introduced hard delete func `Destroy`.
