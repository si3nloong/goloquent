# Breaking Changes

* No longer support Union
* No longer support
* No longer support single `Update`
* Dropped SetDebug(boolean)
* Dropped unsigned
* Dropped datastore from roadmap
* Changed datatype
* Changed `LockForUpdate` to `WriteLock`
* Changed `LockForShared` to `ReadLock`

# New Features

* Introduced `Select` function
* Introduced `DistinctOn` function
* Introduced `Debug` function
* Replaced single entity `Update` to `Save`
* Eager loading `Related` function
* Support unsigned data type, uint, uint8

- Logger

### Pending

* Create (ok)
* Upsert (ok)
* SoftDelete
* Save (ok)
* UpdateMulti
* Delete Multi
* Debug
* ParseQuery (ok)
* TestCase
* Logger

// TODO:

* add soft delete for get
