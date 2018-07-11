### Pending

- [ok] Create
- [ok] Upsert
- [ok] Save
- [ok] UpdateMulti
- [ok] Delete Multi
- [ok] ParseQuery
- [ok] Logger
- [ok] SoftDelete
- [ok] - Support option tag shorthand `index`
- [fix] Sort struct properties in sequence (semantic)
- [fix] struct_codec.go (recheck on commit d0ef13f)
- [fix] Selected fields should follow sequence
- [fix] Flatten struct childs values always null
- TestCase (20%)

// TODO:

- Filter json
- Filter geolocation (structure with spatial or json?)
- Added cache mechanism

Bugs :
Flatten []struct childs values always null
Update with limit (POSTGRES) [ok]
