# Bug / Issue

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
- (2018-06-22) Fix `Paginate` bug, model slice is appending instead of get replace
- (2018-06-25) Fix struct property sequence bug
- (2018-06-27) Fix struct codec, func `Select` and func `DistinctOn`
- (2018-06-28) Fix `Postgres` update with limit clause bug. Only mysql support `UPDATE xxx SET xxx LIMIT 10`. Postgres instead will use `UPDATE xxx SET xxx WHERE key IN (SELECT xxx FROM xxx LIMIT 10)`.
- (2018-06-28) Fix `Paginate` bug, invalid cursor signature due to `qson` package didn't sort the filter fields
- (2018-07-02) Fix `panic: reflect: Field index out of range` on embeded struct, code paths is invalid
- (2018-07-02) Fix entity doesn't execute `Save` func even it implement `Saver` interface when it's not a pointer struct (eg: []Struct)
- (2018-07-05) Fix `Postgres` `GetColumns` bug, it return empty array even database have records
- (2018-07-11) Fix `Update` func bug. It doesn't marshal the map[string]interface nor []interface{} to string after normalization
- (2018-07-13) Fix func `Unmarshal` of data type `Date`. It suppose using `YYYY-MM-DD` format.
- (2018-07-17) Fix panic when value of `WhereIn` or `WhereNotIn` contains `nil` value.
- (2018-07-17) Fix `First` func bug. Entity value doesn't override if the result is empty.
- (2018-07-18) Fix panic when `Where` value is pointer of `int`, `int8`, `int16`, `int32`, `uint`, `uint8`, `uint16`, `uint32`, `float32`.
- (2018-07-23) Fix `DB` connection bug when passing empty port and postgres unable to establish connection thru unix socket.

# Breaking Changes

- Drop function `Count`
- Drop function `Union`
- Drop function single `Update`
- Drop function `SetDebug(boolean)`
- Drop datastore support
- `Delete` function using entity model instead of `*datastore.Key`
- Rename params in function `RunInTransaction` from `*goloquent.Connection` to `*goloquent.DB`
- Rename function `LockForUpdate` to `WLock`
- Rename function `LockForShared` to `RLock`
- Change function single entity `Update` to `Save`
- Change `Loader` interface `Load([]datastore.Property) error` to `Load() error`
- Change `Saver` interface `Save() ([]datastore.Property,error)` to `Save() error`
- Change second parameter **parentKey** `*datastore.Key` to optional on function `Create` nor `Upsert`
- (2018-06-16) No longer support mysql 5.6 and below (at least 5.7)
- (2018-06-19) Table is now by default using `utf8mb4` encoding
- (2018-06-20) Replace `Next` func in `Pagination` struct with `NextCursor`
- (2018-06-21) Support extra option `datatype`, `charset`, `collate` on struct property, but it only limited to datatype of `string`
- (2018-06-21) Allow `*` on func `Select`
- (2018-06-24) Replace offset pagination with cursor pagination
- (2018-07-05) Rename `WhereNe` with `WhereNotEqual`.
- (2018-07-08) Rename `WhereEq` with `WhereEqual`.
- (2018-07-08) Replace return parameter `Query` to `Table` on func `Table` of `goloquent.DB`
- (2018-07-17) Expose operator to public.

# New Features

- Introduce `Select` func.
- Introduce `DistinctOn` func.
- Introduce `Lock` func.
- Introduce `Truncate` func.
- Introduce `Flush` func.
- Introduce package `db`.
- Introduce package `qson`. (Query JSON)
- Replace statement debug using `LogHandler`.
- Support unsigned integer, uint, uint8, uint16, uint32, uint64
- Support any pointer of base data type and struct
- (2018-06-14) Support **Postgres**.
- (2018-06-18) Introduce `Scan` func.
- (2018-06-22) Introduce hard delete func `Destroy`.
- (2018-06-24) Introduce `Unscoped` func.
- (2018-07-05) Support **JSON** filter.
- (2018-07-05) Introduce `WhereJSONEqual` func.
- (2018-07-08) Introduce new struct `Table` with new func, such as `Exists`, `DropIfExists`, `Truncate`, `AddIndex`, `AddUniqueIndex`
- (2018-07-08) Introduce new data type `Date`.
- (2018-07-17) Enhance data type `Date`, add func `MarshalText` and `UnmarshalText`.
- (2018-07-18) Introduce JSON filtering func `WhereJSON`, `WhereJSONNotEqual`, `WhereJSONIn`, `WhereNotIn`, `WhereJSONContainAny`, `WhereJSONType`, `WhereJSONIsObject`, `WhereJSONIsArray`
- (2018-08-16) Introduce new func `AnyOfAncestor`.
