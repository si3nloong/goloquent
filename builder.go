package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

type builder struct {
	driver  string
	dbName  string
	db      Client
	dialect Dialect
	query   scope
	// logger  LogHandler
}

func newBuilder(query *Query) *builder {
	clone := query.db.clone()
	return &builder{
		driver:  clone.driver,
		dbName:  clone.name,
		db:      clone.conn,
		dialect: clone.dialect,
		query:   query.clone().scope,
		// logger:  clone.logger,
	}
}

func (b *builder) getTable(table string) string {
	return fmt.Sprintf("%s.%s",
		b.dialect.Quote(b.dbName),
		b.dialect.Quote(table))
}

func (b *builder) buildWhere(query scope, args ...interface{}) (*Stmt, error) {
	buf := new(bytes.Buffer)
	wheres := make([]string, 0)
	i := len(args) + 1
	for _, f := range query.filters {
		name := b.dialect.Quote(f.field)
		v, err := f.Interface()
		if err != nil {
			return nil, err
		}

		switch f.field {
		case keyFieldName:
			name = fmt.Sprintf("concat(%s,%q,%s)",
				b.dialect.Quote(parentColumn),
				keyDelimeter,
				b.dialect.Quote(keyColumn))
			v, err = interfaceKeyToString(f.value)
			if err != nil {
				return nil, err
			}
		case keyColumn:
			switch vi := f.value.(type) {
			case []byte:
				v = fmt.Sprintf(`'%s'`, strings.Trim(string(vi), `'`))
			case string:
				v = fmt.Sprintf(`'%s'`, strings.Trim(vi, `'`))
			}
		}

		op, vv := "=", b.dialect.Bind(i)
		switch f.operator {
		case equal:
			op = "="
			if v == nil {
				wheres = append(wheres, fmt.Sprintf("%s IS NULL", name))
				continue
			}
		case notEqual:
			op = "<>"
			if v == nil {
				wheres = append(wheres, fmt.Sprintf("%s IS NOT NULL", name))
				continue
			}
		case greaterThan:
			op = ">"
		case greaterEqual:
			op = ">="
		case lessThan:
			op = "<"
		case lessEqual:
			op = "<="
		case like:
			op = "LIKE"
		case notLike:
			op = "NOT LIKE"
		case in:
			op = "IN"
			x, isOk := v.([]interface{})
			if !isOk {
				x = append(x, v)
			}
			vv = fmt.Sprintf("(%s)", strings.TrimRight(
				strings.Repeat(b.dialect.Bind(0)+",", len(x)), ","))
			wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
			args = append(args, x...)
			continue
		case notIn:
			op = "NOT IN"
			x, isOk := v.([]interface{})
			if !isOk {
				x = append(x, v)
			}
			vv = fmt.Sprintf("(%s)", strings.TrimRight(
				strings.Repeat(b.dialect.Bind(0)+",", len(x)), ","))
			wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
			args = append(args, x...)
			continue
		}
		wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
		args = append(args, v)
		i++
	}

	for _, aa := range query.ancestors {
		wheres = append(wheres, fmt.Sprintf(
			"(%s = %s OR %s LIKE %s OR %s LIKE %s)",
			b.dialect.Quote(parentColumn),
			b.dialect.Bind(i+1),
			b.dialect.Quote(parentColumn),
			b.dialect.Bind(i+2),
			b.dialect.Quote(parentColumn),
			b.dialect.Bind(i+3)))
		k := stringifyKey(aa)
		args = append(args, k, "%/"+k, k+"/%")
	}

	if len(wheres) > 0 {
		buf.WriteString(" WHERE ")
		buf.WriteString(strings.Join(wheres, " AND "))
	}

	// __key__ sorting, filter
	if len(query.orders) > 0 {
		arr := make([]string, 0, len(query.orders))
		for _, o := range query.orders {
			name := b.dialect.Quote(o.field)
			if o.field == keyFieldName {
				name = fmt.Sprintf("concat(%s,%s)",
					b.dialect.Quote(parentColumn), b.dialect.Quote(keyColumn))
			}
			suffix := " ASC"
			if o.direction != ascending {
				suffix = " DESC"
			}
			arr = append(arr, name+suffix)
		}
		buf.WriteString(" ORDER BY " + strings.Join(arr, ","))
	}

	if query.limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}
	if query.offset > 0 {
		buf.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}

	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func consoleLog(b builder, stmt Stmt) {
	if b.db.logger != nil {
		b.db.logger(&stmt)
	}
}

func execStmt(db sqlCommon, stmt *Stmt) error {
	// go consoleLog(*, *stmt)
	conn, err := db.Prepare(stmt.Raw())
	if err != nil {
		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
	}
	defer conn.Close()
	result, err := conn.Exec(stmt.arguments...)
	if err != nil {
		return fmt.Errorf("goloquent: %v", err)
	}
	stmt.Result = result
	return nil
}

func (b *builder) execStmt(stmt *Stmt) error {
	go consoleLog(*b, *stmt)
	conn, err := b.db.Prepare(stmt.Raw())
	if err != nil {
		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
	}
	defer conn.Close()
	result, err := conn.Exec(stmt.arguments...)
	if err != nil {
		return fmt.Errorf("goloquent: %v", err)
	}
	stmt.Result = result
	return nil
}

func (b *builder) execQuery(stmt *Stmt) (*sql.Rows, error) {
	go consoleLog(*b, *stmt)
	var rows, err = b.db.Query(stmt.Raw(), stmt.arguments...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (b *builder) createTable(e *entity) error {
	return b.dialect.CreateTable(e.Name(), e.columns)
}

func (b *builder) alterTable(e *entity) error {
	return b.dialect.AlterTable(e.Name(), e.columns)
}

func (b *builder) migrate(models []interface{}) error {
	for _, mm := range models {
		e, err := newEntity(mm)
		if err != nil {
			return err
		}
		if b.dialect.HasTable(e.Name()) {
			if err := b.alterTable(e); err != nil {
				return err
			}
			continue
		}
		if err := b.createTable(e); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) getCommand(e *entity) (*Stmt, error) {
	query := b.query
	buf := new(bytes.Buffer)
	scope := "*"
	if len(query.projection) > 0 {
		scope = b.dialect.Quote(strings.Join(query.projection, b.dialect.Quote(",")))
	}
	if len(query.distinctOn) > 0 {
		scope = "DISTINCT " + b.dialect.Quote(strings.Join(query.distinctOn, b.dialect.Quote(",")))
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", scope, b.getTable(e.Name())))
	if e.hasSoftDelete() {
		query.filters = append(query.filters, Filter{
			field:    softDeleteColumn,
			operator: equal,
			value:    nil,
		})
	}
	cmd, err := b.buildWhere(query)
	if err != nil {
		return nil, err
	}
	buf.WriteString(cmd.Raw())
	switch query.lockMode {
	case ReadLock:
		buf.WriteString(" LOCK IN SHARE MODE")
	case WriteLock:
		buf.WriteString(" FOR UPDATE")
	}
	buf.WriteString(";")

	return &Stmt{
		statement: buf,
		arguments: cmd.arguments,
	}, nil
}

func (b *builder) run(table string, cmd *Stmt) (*Iterator, error) {
	var rows, err = b.execQuery(cmd)
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}

	it := Iterator{
		table:    table,
		position: -1,
		columns:  cols,
	}

	i := 0
	for rows.Next() {
		m := make([]interface{}, len(cols))
		for j := range cols {
			m[j] = &m[j]
		}

		if err := rows.Scan(m...); err != nil {
			return nil, err
		}

		for j, name := range cols {
			it.put(i, name, m[j])
		}
		it.mergeKey()
		i++
	}

	return &it, nil
}

func (b *builder) get(model interface{}, mustExist bool) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.getCommand(e)
	if err != nil {
		return err
	}

	it, err := b.run(e.Name(), cmd)
	if err != nil {
		return err
	}

	first := it.First()
	if mustExist && first == nil {
		return ErrNoSuchEntity
	}

	if first != nil {
		err = it.Scan(model)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) getMulti(model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.getCommand(e)
	if err != nil {
		return err
	}

	it, err := b.run(e.Name(), cmd)
	if err != nil {
		return err
	}

	v := reflect.Indirect(reflect.ValueOf(model))
	vv := reflect.MakeSlice(v.Type(), 0, 0)
	isPtr, t := checkMultiPtr(v)
	for it.Next() {
		vi := reflect.New(t)
		_, err = it.scan(vi.Interface())
		if err != nil {
			return err
		}

		if !isPtr {
			vi = vi.Elem()
		}
		vv = reflect.Append(vv, vi)
	}
	v.Set(vv)
	return nil
}

func (b *builder) paginate(p *Pagination, model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.getCommand(e)
	if err != nil {
		return err
	}

	it, err := b.run(e.Name(), cmd)
	if err != nil {
		return err
	}

	v := reflect.Indirect(reflect.ValueOf(model))
	isPtr, t := checkMultiPtr(v)
	i := uint(1)
	for it.Next() {
		vi := reflect.New(t)
		data, err := it.scan(vi.Interface())
		if err != nil {
			return err
		}
		pk, isOk := data[keyFieldName].(*datastore.Key)
		if !isOk || pk == nil {
			return fmt.Errorf("goloquent: missing primary key")
		}
		p.Cursor = pk.Encode()
		if i > p.Limit {
			continue
		}

		if !isPtr {
			vi = vi.Elem()
		}
		v.Set(reflect.Append(v, vi))
		i++
	}

	count := it.Count()
	if count <= p.Limit {
		p.Cursor = ""
	}
	p.count = count
	return nil
}

func (b *builder) putStmt(parentKey []*datastore.Key, e *entity) (*Stmt, error) {
	v := e.slice.Elem()
	isInline := (parentKey == nil && len(parentKey) == 0)
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	keys := make([]*datastore.Key, v.Len(), v.Len())
	if !isInline {
		for i := 0; i < len(keys); i++ {
			keys[i] = newPrimaryKey(e.Name(), parentKey[0])
		}
	}

	cols := e.Columns()
	buf.WriteString(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ",
		b.getTable(e.Name()),
		b.dialect.Quote(strings.Join(e.Columns(), b.dialect.Quote(",")))))

	for i := 0; i < v.Len(); i++ {
		f := v.Index(i)

		if x, isOk := f.Interface().(Saver); isOk {
			if err := x.Save(); err != nil {
				return nil, err
			}
		}
		props, err := SaveStruct(f.Interface())
		if err != nil {
			return nil, nil
		}

		pk := newPrimaryKey(e.Name(), keys[i])
		if isInline {
			kk, isOk := props[keyFieldName].Value.(*datastore.Key)
			if !isOk {
				return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
			}
			pk = newPrimaryKey(e.Name(), kk)
		}

		k, p := splitKey(pk)
		props[keyColumn] = Property{[]string{keyColumn}, typeOfPtrKey, k}
		props[parentColumn] = Property{[]string{parentColumn}, typeOfPtrKey, p}
		fv := mustGetField(f, e.field(keyFieldName))
		if !fv.IsValid() || fv.Type() != typeOfPtrKey {
			return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
		}
		fv.Set(reflect.ValueOf(pk))

		if i != 0 {
			buf.WriteString(",")
		}
		vals := make([]interface{}, len(cols), len(cols))
		for j, c := range cols {
			vv, err := props[c].Interface()
			if err != nil {
				return nil, err
			}
			vals[j] = vv
		}

		buf.WriteString("(")
		for j := 0; j < len(cols); j++ {
			buf.WriteString(b.dialect.Bind(i*len(cols)+j) + ",")
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString(")")
		args = append(args, vals...)
	}
	buf.WriteString(";")

	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) put(model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.putStmt(parentKey, e)
	if err != nil {
		return err
	}
	return b.execStmt(cmd)
}

func (b *builder) upsert(model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.putStmt(parentKey, e)
	if err != nil {
		return err
	}
	cols := e.Columns()
	omits := newDictionary(b.query.omits)
	for i, c := range cols {
		if !omits.has(c) {
			continue
		}
		cols = append(cols[:i], cols[i+1:]...)
	}
	cmd.statement.Truncate(cmd.statement.Len() - 1)
	buf := new(bytes.Buffer)
	buf.WriteString(cmd.Raw())
	buf.WriteString(" " + b.dialect.OnConflictUpdate(cols))
	buf.WriteString(";")
	cmd.statement = buf
	return b.execStmt(cmd)
}

func (b *builder) saveMutation(model interface{}) (*Stmt, error) {
	v := reflect.Indirect(reflect.ValueOf(model))
	if v.Len() <= 0 {
		return new(Stmt), nil
	}
	e, err := newEntity(model)
	if err != nil {
		return nil, err
	}
	e.setName(b.query.table)
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", b.getTable(e.Name())))
	f := v.Index(0)

	if x, isOk := f.Interface().(Saver); isOk {
		if err := x.Save(); err != nil {
			return nil, err
		}
	}
	props, err := SaveStruct(f.Interface())
	if err != nil {
		return nil, err
	}

	pk, isOk := props[keyFieldName].Value.(*datastore.Key)
	if !isOk {
		return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
	}
	delete(props, keyFieldName)
	if pk == nil || pk.Incomplete() {
		return nil, fmt.Errorf("goloquent: invalid key value, %v", pk)
	}

	omits := newDictionary(b.query.omits)
	j := int(1)
	for k, p := range props {
		if omits.has(k) {
			continue
		}
		it, err := p.Interface()
		if err != nil {
			return nil, err
		}
		buf.WriteString(fmt.Sprintf("%s = %s,",
			b.dialect.Quote(k),
			b.dialect.Bind(j)))
		args = append(args, it)
		j++
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(fmt.Sprintf(
		" WHERE %s = %s AND %s = %s;",
		b.dialect.Quote(keyColumn), b.dialect.Bind(len(args)),
		b.dialect.Quote(parentColumn), b.dialect.Bind(len(args))))
	k, p := splitKey(pk)
	args = append(args, k, p)

	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) save(model interface{}) error {
	v := reflect.ValueOf(model)
	vi := reflect.MakeSlice(reflect.SliceOf(v.Type()), 1, 1)
	vi.Index(0).Set(v)
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	cmd, err := b.saveMutation(vv.Interface())
	if err != nil {
		return err
	}
	if err := b.execStmt(cmd); err != nil {
		return err
	}
	v.Elem().Set(vi.Index(0).Elem())
	return nil
}

func (b *builder) updateWithMap(v reflect.Value) (*Stmt, error) {
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	for i, k := range v.MapKeys() {
		vv := v.MapIndex(k)
		if k.Kind() != reflect.String {
			return nil, fmt.Errorf("goloquent: invalid map key data type, %q", k.Kind())
		}
		kk := k.String()
		if kk == keyFieldName {
			return nil, fmt.Errorf("goloquent: update __key__ is not allow")
		}
		buf.WriteString(fmt.Sprintf(" %s = %s,",
			b.dialect.Quote(kk),
			b.dialect.Bind(i)))
		v, err := normalizeValue(vv.Interface())
		if err != nil {
			return nil, err
		}
		it, err := interfaceToValue(v)
		if err != nil {
			return nil, err
		}
		args = append(args, it)
	}
	buf.Truncate(buf.Len() - 1)
	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) updateWithStruct(model interface{}) (*Stmt, error) {
	vi := reflect.Indirect(reflect.ValueOf(model))
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	if err := checkSinglePtr(vv.Interface()); err != nil {
		return nil, err
	}

	cols := newDictionary(b.query.projection)
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	props, err := SaveStruct(vv.Interface())
	if err != nil {
		return nil, err
	}
	i := int(1)
	for _, p := range props {
		name := p.Name()
		if name == keyFieldName || (!cols.has(name) && p.isZero()) {
			continue
		}
		it, err := p.Interface()
		if err != nil {
			return nil, err
		}
		buf.WriteString(fmt.Sprintf("%s = %s,",
			b.dialect.Quote(p.Name()),
			b.dialect.Bind(i)))
		args = append(args, it)
		i++
	}
	buf.Truncate(buf.Len() - 1)
	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) updateMulti(v interface{}) error {
	vi := reflect.Indirect(reflect.ValueOf(v))
	args := make([]interface{}, 0)
	buf := new(bytes.Buffer)
	table := b.query.table
	if table == "" {
		table = vi.Type().Name()
	}
	if table == "" {
		return fmt.Errorf("goloquent: missing table name")
	}
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", b.getTable(table)))
	switch vi.Type().Kind() {
	case reflect.Map:
		if vi.IsNil() || vi.Len() == 0 {
			return nil
		}
		stmt, err := b.updateWithMap(vi)
		if err != nil {
			return err
		}
		buf.WriteString(stmt.Raw())
		args = append(args, stmt.arguments...)
	case reflect.Struct:
		stmt, err := b.updateWithStruct(v)
		if err != nil {
			return err
		}
		buf.WriteString(" " + stmt.Raw())
		args = append(args, stmt.arguments...)
	default:
		return fmt.Errorf("goloquent: unsupported data type %v on `Update`", vi.Type())
	}
	stmt, err := b.buildWhere(b.query)
	if err != nil {
		return err
	}
	buf.WriteString(stmt.Raw())
	buf.WriteString(";")
	return b.execStmt(&Stmt{
		statement: buf,
		arguments: append(args, stmt.arguments...),
	})
}

func (b *builder) concatKeys(e *entity) (*Stmt, error) {
	v := e.slice.Elem()
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	buf.WriteString("(")
	for i := 0; i < v.Len(); i++ {
		f := v.Index(i)
		if i != 0 {
			buf.WriteString(",")
		}
		kk, isOk := mustGetField(f, e.field(keyFieldName)).Interface().(*datastore.Key)
		if !isOk {
			return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
		}
		if kk.Incomplete() {
			return nil, fmt.Errorf("goloquent: entity %q has incomplete key", f.Type().Name())
		}
		k, p := splitKey(kk)
		buf.WriteString(b.dialect.Bind(i))
		args = append(args, p+keyDelimeter+k)
	}
	buf.WriteString(")")
	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) softDeleteStmt(e *entity) (*Stmt, error) {
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", b.getTable(e.Name())))
	buf.WriteString(fmt.Sprintf("%s = %s WHERE concat(%s) IN ",
		b.dialect.Quote(softDeleteColumn),
		b.dialect.Bind(1),
		fmt.Sprintf("%s,%q,%s",
			b.dialect.Quote(parentColumn),
			keyDelimeter,
			b.dialect.Quote(keyColumn))))
	args = append(args, time.Now().UTC().Format("2006-01-02 15:04:05"))
	stmt, err := b.concatKeys(e)
	if err != nil {
		return nil, err
	}
	buf.WriteString(stmt.Raw())
	buf.WriteString(";")
	return &Stmt{
		statement: buf,
		arguments: append(args, stmt.arguments...),
	}, nil
}

func (b *builder) deleteStmt(e *entity) (*Stmt, error) {
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	if e.hasSoftDelete() {
		return b.softDeleteStmt(e)
	}
	buf.WriteString(fmt.Sprintf(
		"DELETE FROM %s WHERE concat(%s) IN ",
		b.getTable(e.Name()),
		fmt.Sprintf("%s,%q,%s",
			b.dialect.Quote(parentColumn),
			keyDelimeter,
			b.dialect.Quote(keyColumn))))
	stmt, err := b.concatKeys(e)
	if err != nil {
		return nil, err
	}
	buf.WriteString(stmt.Raw())
	buf.WriteString(";")
	return &Stmt{
		statement: buf,
		arguments: append(args, stmt.arguments...),
	}, nil
}

func (b *builder) delete(model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(b.query.table)
	cmd, err := b.deleteStmt(e)
	if err != nil {
		return err
	}
	return b.execStmt(cmd)
}

func (b *builder) deleteByQuery(query *Query) error {
	table := query.table
	cmd, err := b.buildWhere(b.query)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("DELETE FROM %s", b.getTable(table)))
	buf.WriteString(cmd.Raw())
	buf.WriteString(";")
	cmd.statement = buf
	return b.execStmt(cmd)
}

func (b *builder) truncate(table string) error {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("TRUNCATE TABLE %s;", b.getTable(table)))
	cmd := new(Stmt)
	cmd.statement = buf
	return b.execStmt(cmd)
}

func (b *builder) runInTransaction(cb TransactionHandler) error {
	conn, isOk := b.db.sqlCommon.(*sql.DB)
	if !isOk {
		return fmt.Errorf("goloquent: unable to initiate transaction")
	}
	txn, err := conn.Begin()
	if err != nil {
		return fmt.Errorf("goloquent: unable to begin transaction, %v", err)
	}
	// if r := recover(); r != nil {
	// 	defer txn.Rollback()
	// }
	defer txn.Rollback()
	if err := cb(NewDB(b.driver, txn, b.dialect, b.db.logger)); err != nil {
		return err
	}
	return txn.Commit()
}

func interfaceKeyToString(it interface{}) (interface{}, error) {
	var v interface{}
	switch vi := it.(type) {
	case nil:
		v = vi
	case *datastore.Key:
		k, p := splitKey(vi)
		v = p + keyDelimeter + k
	case string:
		v = vi
	case []byte:
		v = string(vi)
	default:
		return nil, fmt.Errorf("goloquent: primary key has invalid data type %v", reflect.TypeOf(vi))
	}
	return v, nil
}
