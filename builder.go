package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/datastore"
)

// Builder :
type Builder struct {
	dbName  string
	db      sqlCommon
	dialect Dialect
	logger  LogHandler
}

func (s *Builder) getTable(table string) string {
	return fmt.Sprintf("%s.%s", s.dialect.Quote(s.dbName), s.dialect.Quote(table))
}

func (s *Builder) buildWhere(query *Query, args ...interface{}) (*Stmt, error) {
	buf := new(bytes.Buffer)
	wheres := make([]string, 0)
	i := len(args)
	for _, f := range query.filters {
		name := s.dialect.Quote(f.field)
		v, err := f.Interface()
		if err != nil {
			return nil, err
		}
		if f.field == keyFieldName {
			name = fmt.Sprintf("concat(%s,%q,%s)",
				s.dialect.Quote(parentColumn),
				keyDelimeter,
				s.dialect.Quote(keyColumn))
			v, err = interfaceKeyToString(f.value)
			if err != nil {
				return nil, err
			}
		}

		op, vv := "=", s.dialect.Bind(i)
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
				strings.Repeat(s.dialect.Bind(0)+",", len(x)), ","))
			wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
			args = append(args, x...)
			continue
		case notIn:
			op = "NOT IN"
			x, isOk := v.([]interface{})
			if !isOk {
				x = append(x, v)
			}
			vv = fmt.Sprintf("(%s)", vv)
		}
		wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
		args = append(args, v)
		i++
	}

	for _, aa := range query.ancestors {
		wheres = append(wheres, fmt.Sprintf(
			"(%s = %s OR %s LIKE %s OR %s LIKE %s)",
			s.dialect.Quote(parentColumn),
			s.dialect.Bind(i),
			s.dialect.Quote(parentColumn),
			s.dialect.Bind(i),
			s.dialect.Quote(parentColumn),
			s.dialect.Bind(i)))
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
			name := s.dialect.Quote(o.field)
			if o.field == keyFieldName {
				name = fmt.Sprintf("concat(%s,%s)",
					s.dialect.Quote(parentColumn), s.dialect.Quote(keyColumn))
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

func consoleLog(s Builder, stmt Stmt) {
	if s.logger != nil {
		s.logger(&stmt)
	}
}

func (s *Builder) execStmt(stmt *Stmt) error {
	sql, err := s.db.Prepare(stmt.Raw())
	if err != nil {
		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
	}
	result, err := sql.Exec(stmt.arguments...)
	if err != nil {
		return fmt.Errorf("goloquent: %v", err)
	}
	stmt.Result = result
	consoleLog(*s, *stmt)
	return nil
}

func (s *Builder) execQuery(stmt *Stmt) (*sql.Rows, error) {
	var rows, err = s.db.Query(stmt.Raw(), stmt.arguments...)
	if err != nil {
		return nil, err
	}
	consoleLog(*s, *stmt)
	return rows, nil
}

func (s *Builder) createTableCommand(e *entity) (*Stmt, error) {
	idx := make([]string, 0)
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (", s.getTable(e.Name())))
	for _, c := range e.columns {
		for _, ss := range s.dialect.GetSchema(c) {
			buf.WriteString(fmt.Sprintf("%s %s,",
				s.dialect.Quote(ss.Name),
				s.dialect.DataType(ss)))

			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", e.Name(), ss.Name, "Idx")
				buf.WriteString(fmt.Sprintf("INDEX %s (%s),",
					s.dialect.Quote(idx), s.dialect.Quote(ss.Name)))
			}
		}
	}

	if len(idx) > 0 {
		buf.WriteString(strings.Join(idx, ",") + ",")
	}
	buf.WriteString(fmt.Sprintf("PRIMARY KEY (%s,%s)",
		s.dialect.Quote(parentColumn), s.dialect.Quote(keyColumn)))
	buf.WriteString(fmt.Sprintf(") ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s;",
		utf8CharSet.Encoding, utf8CharSet.Collation))

	return &Stmt{
		statement: buf,
		arguments: nil,
	}, nil
}

func (s *Builder) createTable(e *entity) error {
	cmd, err := s.createTableCommand(e)
	if err != nil {
		return err
	}
	return s.execStmt(cmd)
}

func (s *Builder) alterTableCommand(e *entity) (*Stmt, error) {
	cols := newDictionary(s.dialect.GetColumns(e.Name()))
	idxs := newDictionary(s.dialect.GetIndexes(e.Name()))
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("ALTER TABLE %s", s.getTable(e.Name())))
	suffix := "FIRST"
	for _, c := range e.columns {
		for _, ss := range s.dialect.GetSchema(c) {
			action := "ADD"
			if cols.has(ss.Name) {
				action = "MODIFY"
			}
			buf.WriteString(fmt.Sprintf(" %s %s %s %s,",
				action, s.dialect.Quote(ss.Name), s.dialect.DataType(ss), suffix))
			suffix = fmt.Sprintf("AFTER %s", s.dialect.Quote(ss.Name))

			if ss.IsIndexed {
				idx := fmt.Sprintf("%s_%s_%s", e.Name(), ss.Name, "Idx")
				if idxs.has(idx) {
					idxs.delete(idx)
					continue
				}
				buf.WriteString(fmt.Sprintf(
					" ADD INDEX %s (%s),",
					s.dialect.Quote(idx),
					s.dialect.Quote(ss.Name)))
			}
			cols.delete(ss.Name)
		}
	}

	// for _, col := range cols.keys() {
	// 	buf.WriteString(fmt.Sprintf(
	// 		" DROP COLUMN %s,", s.dialect.Quote(col)))
	// }

	for _, idx := range idxs.keys() {
		buf.WriteString(fmt.Sprintf(
			" DROP INDEX %s,", s.dialect.Quote(idx)))
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(";")

	return &Stmt{
		statement: buf,
		arguments: nil,
	}, nil
}

func (s *Builder) alterTable(e *entity) error {
	cmd, err := s.alterTableCommand(e)
	if err != nil {
		return err
	}
	return s.execStmt(cmd)
}

func (s *Builder) migrate(models []interface{}) error {
	for _, mm := range models {
		e, err := newEntity(mm)
		if err != nil {
			return err
		}
		if s.dialect.HasTable(e.Name()) {
			if err := s.alterTable(e); err != nil {
				return err
			}
			continue
		}
		if err := s.createTable(e); err != nil {
			return err
		}
	}
	return nil
}

func (s *Builder) getCommand(e *entity, query *Query) (*Stmt, error) {
	buf := new(bytes.Buffer)
	scope := "*"
	if len(query.projection) > 0 {
		scope = s.dialect.Quote(strings.Join(query.projection, s.dialect.Quote(",")))
	}
	if len(query.distinctOn) > 0 {
		scope = "DISTINCT " + s.dialect.Quote(strings.Join(query.distinctOn, s.dialect.Quote(",")))
	}

	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", scope, s.getTable(e.Name())))
	cmd, err := s.buildWhere(query)
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
		table:     e.Name(),
		statement: buf,
		arguments: cmd.arguments,
	}, nil
}

func (s *Builder) run(cmd *Stmt) (*Iterator, error) {
	var rows, err = s.execQuery(cmd)
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("goloquent: %v", err)
	}

	it := Iterator{
		table:    cmd.table,
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
		i++
	}

	return &it, nil
}

func (s *Builder) get(query *Query, model interface{}, mustExist bool) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.getCommand(e, query)
	if err != nil {
		return err
	}

	it, err := s.run(cmd)
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

func (s *Builder) getMulti(query *Query, model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.getCommand(e, query)
	if err != nil {
		return err
	}

	it, err := s.run(cmd)
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

func (s *Builder) paginate(query *Query, p *Pagination, model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.getCommand(e, query)
	if err != nil {
		return err
	}

	it, err := s.run(cmd)
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

func (s *Builder) putCommand(parentKey []*datastore.Key, e *entity) (*Stmt, error) {
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
		s.getTable(e.Name()),
		s.dialect.Quote(strings.Join(e.Columns(), s.dialect.Quote(",")))))

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
			buf.WriteString(s.dialect.Bind(i*len(cols)+j) + ",")
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString(")")
		args = append(args, vals...)
	}
	buf.WriteString(";")

	return &Stmt{
		table:     e.Name(),
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Builder) put(query *Query, model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.putCommand(parentKey, e)
	if err != nil {
		return err
	}
	return s.execStmt(cmd)
}

func (s *Builder) upsert(query *Query, model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.putCommand(parentKey, e)
	if err != nil {
		return err
	}
	cols := e.Columns()
	omits := newDictionary(query.omits)
	fmt.Println("Omit :: ", omits)
	for i, c := range cols {
		if !omits.has(c) {
			continue
		}
		cols = append(cols[:i], cols[i+1:]...)
	}
	cmd.statement.Truncate(cmd.statement.Len() - 1)
	buf := new(bytes.Buffer)
	buf.WriteString(cmd.Raw())
	buf.WriteString(" " + s.dialect.OnConflictUpdate(cols))
	buf.WriteString(";")
	cmd.statement = buf
	return s.execStmt(cmd)
}

func (s *Builder) saveMutation(query *Query, model interface{}) (*Stmt, error) {
	v := reflect.Indirect(reflect.ValueOf(model))
	if v.Len() <= 0 {
		return new(Stmt), nil
	}
	e, err := newEntity(model)
	if err != nil {
		return nil, err
	}
	e.setName(query.table)
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", s.getTable(e.Name())))
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

	j := int(1)
	for k, p := range props {
		it, err := p.Interface()
		if err != nil {
			return nil, err
		}
		buf.WriteString(fmt.Sprintf("%s = %s,",
			s.dialect.Quote(k),
			s.dialect.Bind(j)))
		args = append(args, it)
		j++
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(fmt.Sprintf(
		" WHERE %s = %s AND %s = %s;",
		s.dialect.Quote(keyColumn), s.dialect.Bind(len(args)),
		s.dialect.Quote(parentColumn), s.dialect.Bind(len(args))))
	k, p := splitKey(pk)
	args = append(args, k, p)

	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Builder) save(query *Query, model interface{}) error {
	v := reflect.ValueOf(model)
	vi := reflect.MakeSlice(reflect.SliceOf(v.Type()), 1, 1)
	vi.Index(0).Set(v)
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	cmd, err := s.saveMutation(query, vv.Interface())
	if err != nil {
		return err
	}
	if err := s.execStmt(cmd); err != nil {
		return err
	}
	v.Elem().Set(vi.Index(0).Elem())
	return nil
}

func (s *Builder) updateWithMap(v reflect.Value) (*Stmt, error) {
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
			s.dialect.Quote(kk),
			s.dialect.Bind(i)))
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

func (s *Builder) updateWithStruct(query *Query, model interface{}) (*Stmt, error) {
	vi := reflect.Indirect(reflect.ValueOf(model))
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	if err := checkSinglePtr(vv.Interface()); err != nil {
		return nil, err
	}

	cols := newDictionary(query.projection)
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
			s.dialect.Quote(p.Name()),
			s.dialect.Bind(i)))
		args = append(args, it)
		i++
	}
	buf.Truncate(buf.Len() - 1)
	return &Stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Builder) updateMulti(query *Query, v interface{}) error {
	vi := reflect.Indirect(reflect.ValueOf(v))
	args := make([]interface{}, 0)
	buf := new(bytes.Buffer)
	table := strings.Replace(query.table, "", vi.Type().Name(), 1)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", s.getTable(table)))
	switch vi.Type().Kind() {
	case reflect.Map:
		if vi.IsNil() || vi.Len() == 0 {
			return nil
		}
		stmt, err := s.updateWithMap(vi)
		if err != nil {
			return err
		}
		buf.WriteString(stmt.Raw())
		args = append(args, stmt.arguments...)
	case reflect.Struct:
		stmt, err := s.updateWithStruct(query, v)
		if err != nil {
			return err
		}
		buf.WriteString(" " + stmt.Raw())
		args = append(args, stmt.arguments...)
	default:
		return fmt.Errorf("goloquent: unsupported data type %v on `Update`", vi.Type())
	}
	stmt, err := s.buildWhere(query)
	if err != nil {
		return err
	}
	args = append(args, stmt.arguments...)
	buf.WriteString(stmt.Raw())
	buf.WriteString(";")
	fmt.Println(buf.String())
	return s.execStmt(&Stmt{
		statement: buf,
		arguments: args,
	})
}

func (s *Builder) deleteCommand(e *entity) (*Stmt, error) {
	v := e.slice.Elem()
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf(
		"DELETE FROM %s WHERE concat(%s) in (",
		s.getTable(e.Name()),
		fmt.Sprintf("%s,%q,%s", s.dialect.Quote(parentColumn), keyDelimeter, s.dialect.Quote(keyColumn))))
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
		buf.WriteString(s.dialect.Bind(i))
		args = append(args, p+keyDelimeter+k)
	}
	buf.WriteString(");")

	return &Stmt{
		table:     e.Name(),
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Builder) delete(query *Query, model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.deleteCommand(e)
	if err != nil {
		return err
	}
	return s.execStmt(cmd)
}

func (s *Builder) deleteByQuery(query *Query) error {
	table := query.table
	cmd, err := s.buildWhere(query)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("DELETE FROM %s", s.getTable(table)))
	buf.WriteString(cmd.Raw())
	buf.WriteString(";")
	cmd.statement = buf
	return s.execStmt(cmd)
}

func (s *Builder) truncate(table string) error {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("TRUNCATE TABLE %s;", s.getTable(table)))
	cmd := new(Stmt)
	cmd.statement = buf
	return s.execStmt(cmd)
}

func (s *Builder) runInTransaction(cb TransactionHandler) error {
	// db, isOk := s.db.(*sql.DB)
	// if !isOk {
	// 	return fmt.Errorf("goloquent: invalid connection")
	// }
	// txn, err := db.Begin()
	// if err != nil {
	// 	return fmt.Errorf("goloquent: unable to begin transaction, %v", err)
	// }
	// tx := NewDB("", txn, s.dialect)
	// if r := recover(); r != nil {
	// 	defer txn.Rollback()
	// 	if err := cb(tx); err != nil {
	// 		return err
	// 	}
	// }
	// return txn.Commit()
	return nil
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
