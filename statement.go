package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/datastore"
)

// CommonError :
var (
	ErrNoSuchEntity = fmt.Errorf("goloquent: entity not found")
)

// Logger :
type Logger interface {
	Println(cmd *Command)
}

// Stmt :
type Stmt struct {
	dbName  string
	db      sqlCommon
	dialect Dialect
	logger  Logger
}

func (s *Stmt) getTable(table string) string {
	return fmt.Sprintf("%s.%s", s.dialect.Quote(s.dbName), s.dialect.Quote(table))
}

func (s *Stmt) buildWhere(query *Query, args ...interface{}) (*Command, error) {
	buf := new(bytes.Buffer)
	wheres := make([]string, 0)
	i := len(args)
	for _, f := range query.filters {
		name := s.dialect.Quote(f.field)
		v, err := f.Value()
		if err != nil {
			return nil, err
		}
		if f.field == keyFieldName {
			name = fmt.Sprintf("concat(%s,%q,%s)",
				s.dialect.Quote(parentColumn), "/", s.dialect.Quote(keyColumn))
			x, isOk := f.value.(*datastore.Key)
			if !isOk {

			}
			k, p := splitKey(x)
			v = p + "/" + k
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
			"(%s LIKE %s OR %s LIKE %s)",
			s.dialect.Quote(parentColumn),
			s.dialect.Bind(i),
			s.dialect.Quote(parentColumn),
			s.dialect.Bind(i)))
		k := stringifyKey(aa)
		args = append(args, "%"+k, k+"%")
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
				name = fmt.Sprintf("concat(%s,%q,%s)",
					s.dialect.Quote(parentColumn), "/", s.dialect.Quote(keyColumn))
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

	return &Command{
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Stmt) execCommand(cmd *Command) error {
	fmt.Println(strings.Repeat("-", 100))
	fmt.Println("SQL :: ", cmd.Statement())
	fmt.Println(strings.Repeat("-", 100))
	// s.logger.Println(cmd)
	ss := cmd.Statement()
	for i, aa := range cmd.arguments {
		ss = strings.Replace(ss, s.dialect.Bind(i), fmt.Sprintf("%q", aa), 1)
	}
	fmt.Println(ss)
	// fmt.Println("Arguments :: ", cmd.arguments)
	stmt, err := s.db.Prepare(cmd.Statement())
	if err != nil {
		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
	}
	if _, err := stmt.Exec(cmd.arguments...); err != nil {
		return fmt.Errorf("goloquent: %v", err)
	}
	return nil
}

func (s *Stmt) execQuery(cmd *Command) error {
	return nil
}

func (s *Stmt) createTableCommand(e *entity) (*Command, error) {
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

	return &Command{
		statement: buf,
		arguments: nil,
	}, nil
}

func (s *Stmt) createTable(e *entity) error {
	cmd, err := s.createTableCommand(e)
	if err != nil {
		return err
	}
	return s.execCommand(cmd)
}

func (s *Stmt) alterTableCommand(e *entity) (*Command, error) {
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

	return &Command{
		statement: buf,
		arguments: nil,
	}, nil
}

func (s *Stmt) alterTable(e *entity) error {
	cmd, err := s.alterTableCommand(e)
	if err != nil {
		return err
	}
	return s.execCommand(cmd)
}

func (s *Stmt) migrate(models []interface{}) error {
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

func (s *Stmt) getCommand(e *entity, query *Query) (*Command, error) {
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
	buf.WriteString(cmd.Statement())
	switch query.lockMode {
	case ReadLock:
		buf.WriteString(" LOCK IN SHARE MODE")
	case WriteLock:
		buf.WriteString(" FOR UPDATE")
	}
	buf.WriteString(";")

	return &Command{
		table:     e.Name(),
		statement: buf,
		arguments: cmd.arguments,
	}, nil
}

func (s *Stmt) run(cmd *Command) (*Iterator, error) {
	fmt.Println(strings.Repeat("-", 100))
	fmt.Println("SQL :: ", cmd.Statement(), cmd.arguments)
	fmt.Println(strings.Repeat("-", 100))
	var rows, err = s.db.Query(cmd.Statement(), cmd.arguments...)
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

func (s *Stmt) get(query *Query, model interface{}, mustExist bool) error {
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

func (s *Stmt) getMulti(query *Query, model interface{}) error {
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

func (s *Stmt) paginate(query *Query, p *Pagination, model interface{}) error {
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

func (s *Stmt) putCommand(parentKey []*datastore.Key, e *entity) (*Command, error) {
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

	// loop every entity
	for i := 0; i < v.Len(); i++ {
		f := v.Index(i)
		props, err := SaveStruct(f.Interface())
		if err != nil {
			return nil, nil
		}

		pk := newPrimaryKey(e.Name(), keys[i])
		if isInline {
			kk, isOk := props[keyFieldName].(*datastore.Key)
			if !isOk {
				return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
			}
			pk = newPrimaryKey(e.Name(), kk)
		}
		fmt.Println("pk ::: ", pk)
		props[keyColumn], props[parentColumn] = splitKey(pk)
		fv := mustGetField(f, e.field(keyFieldName))
		if fv.Type() != typeOfPtrKey {
			return nil, fmt.Errorf("goloquent: entity %q has no primary key property", f.Type().Name())
		}
		fv.Set(reflect.ValueOf(pk))

		if i != 0 {
			buf.WriteString(",")
		}
		vals := make([]interface{}, len(cols), len(cols))
		for j, c := range cols {
			vv, err := interfaceToValue(props[c])
			if err != nil {
				return nil, err
			}
			vv, err = marshal(vv)
			if err != nil {
				return nil, err
			}
			vals[j] = vv
		}

		if x, isOk := f.Interface().(Saver); isOk {
			if err := x.Save(); err != nil {
				return nil, err
			}
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

	return &Command{
		table:     e.Name(),
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Stmt) put(query *Query, model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.putCommand(parentKey, e)
	if err != nil {
		return err
	}
	return s.execCommand(cmd)
}

func (s *Stmt) upsert(query *Query, model interface{}, parentKey []*datastore.Key) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.putCommand(parentKey, e)
	if err != nil {
		return err
	}
	cmd.statement.Truncate(cmd.statement.Len() - 1)
	buf := new(bytes.Buffer)
	buf.WriteString(cmd.Statement())
	buf.WriteString(" " + s.dialect.OnConflictUpdate(e.Columns()))
	buf.WriteString(";")
	cmd.statement = buf
	return s.execCommand(cmd)
}

func (s *Stmt) updateMutation(query *Query, model interface{}) (*Command, error) {
	v := reflect.Indirect(reflect.ValueOf(model))
	if v.Len() <= 0 {
		return new(Command), nil
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
	data, err := SaveStruct(f.Interface())
	if err != nil {
		return nil, err
	}
	// pk, isOk := data[keyFieldName].(*datastore.Key)
	// if !isOk || pk == nil {
	// 	return nil, fmt.Errorf("goloquent: entity has no primary key")
	// }

	for k, v := range data {
		if k == keyFieldName {
			continue
		}
		it, err := interfaceToValue(v)
		if err != nil {
			return nil, err
		}
		it, _ = marshal(it)
		buf.WriteString(fmt.Sprintf("%s = %s,", s.dialect.Quote(k), s.dialect.Bind(0)))
		args = append(args, it)
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(fmt.Sprintf(
		" WHERE %s = %s AND %s = %s;",
		s.dialect.Quote(keyColumn), s.dialect.Bind(len(args)),
		s.dialect.Quote(parentColumn), s.dialect.Bind(len(args))))
	// k, p := splitKey(pk)
	// args = append(args, k, p)

	fmt.Println(buf.String(), args)

	return &Command{
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Stmt) updateWithMap(v reflect.Value) {
	for _, k := range v.MapKeys() {
		vv := v.MapIndex(k)
		fmt.Println(vv)
	}
}

func (s *Stmt) update(query *Query, model interface{}) error {
	v := reflect.ValueOf(model)
	vi := reflect.MakeSlice(reflect.SliceOf(v.Type()), 1, 1)
	vi.Index(0).Set(v)
	vv := reflect.New(vi.Type())
	vv.Elem().Set(vi)
	cmd, err := s.updateMutation(query, vv.Interface())
	if err != nil {
		return err
	}
	if err := s.execCommand(cmd); err != nil {
		return err
	}
	v.Elem().Set(vi.Index(0).Elem())
	return nil
}

func (s *Stmt) updateMulti(query *Query, v interface{}) error {
	fmt.Println("Debug Update " + strings.Repeat("-", 100))
	fmt.Println(v)
	vi := reflect.Indirect(reflect.ValueOf(v))
	switch vi.Type().Kind() {
	case reflect.Map:
		if vi.IsNil() || vi.Len() == 0 {
			return nil
		}
		s.updateWithMap(vi)
	case reflect.Struct:
		vs := reflect.MakeSlice(reflect.SliceOf(vi.Type()), 1, 1)
		vs.Index(0).Set(vi)
		vv := reflect.New(vs.Type())
		vv.Elem().Set(vs)
		_, err := s.updateMutation(query, vv.Interface())
		fmt.Println(err)
	default:
		return fmt.Errorf("goloquent: unsupported data type %v on `Update`", vi.Type())
	}
	return nil
}

func (s *Stmt) deleteCommand(e *entity) (*Command, error) {
	v := e.slice.Elem()
	buf, args := new(bytes.Buffer), make([]interface{}, 0)

	buf.WriteString(fmt.Sprintf(
		"DELETE FROM %s WHERE concat(%s) in (",
		s.getTable(e.Name()),
		fmt.Sprintf("%s,%q,%s", s.dialect.Quote(parentColumn), "/", s.dialect.Quote(keyColumn))))
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
		fmt.Println(p + "/" + k)
		args = append(args, p+"/"+k)
	}
	buf.WriteString(");")

	return &Command{
		table:     e.Name(),
		statement: buf,
		arguments: args,
	}, nil
}

func (s *Stmt) delete(query *Query, model interface{}) error {
	e, err := newEntity(model)
	if err != nil {
		return err
	}
	e.setName(query.table)
	cmd, err := s.deleteCommand(e)
	if err != nil {
		return err
	}
	return s.execCommand(cmd)
}

func (s *Stmt) deleteByQuery(query *Query) error {
	table := query.table
	cmd, err := s.buildWhere(query)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("DELETE FROM %s", s.getTable(table)))
	buf.WriteString(cmd.Statement())
	buf.WriteString(";")
	cmd.statement = buf
	return s.execCommand(cmd)
}

func (s *Stmt) truncate(table string) error {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("TRUNCATE TABLE %s;", s.getTable(table)))
	cmd := new(Command)
	cmd.statement = buf
	return s.execCommand(cmd)
}

func (s *Stmt) runInTransaction(cb TransactionHandler) error {
	db, isOk := s.db.(*sql.DB)
	if !isOk {
		return fmt.Errorf("goloquent: invalid connection")
	}
	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("goloquent: unable to begin transaction, %v", err)
	}
	tx := NewDB("", txn, s.dialect)
	if r := recover(); r != nil {
		defer txn.Rollback()
		if err := cb(tx); err != nil {
			return err
		}
	}
	return txn.Commit()
}
