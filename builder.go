package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

const variable = "??"

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

func (b *builder) buildSelect() {

}

func (b *builder) buildWhere(query scope) (*stmt, error) {
	buf := new(bytes.Buffer)
	wheres := make([]string, 0)
	args := make([]interface{}, 0)
	for _, f := range query.filters {
		name := b.dialect.Quote(f.field)
		v, err := f.Interface()
		if err != nil {
			return nil, err
		}

		switch f.field {
		case keyFieldName, pkColumn:
			name = b.dialect.Quote(pkColumn)
			v, err = interfaceKeyToString(f.value)
			if err != nil {
				return nil, err
			}
		}

		op, vv := "=", variable
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
				strings.Repeat(variable+",", len(x)), ","))
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
				strings.Repeat(variable+",", len(x)), ","))
			wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
			args = append(args, x...)
			continue
		}
		wheres = append(wheres, fmt.Sprintf("%s %s %s", name, op, vv))
		args = append(args, v)
	}

	for _, aa := range query.ancestors {
		wheres = append(wheres, fmt.Sprintf("%s LIKE %s", b.dialect.Quote(pkColumn), variable))
		args = append(args, fmt.Sprintf("%%%s/%%", stringifyKey(aa)))
	}

	if len(wheres) > 0 {
		sort.Strings(wheres)
		buf.WriteString(" WHERE ")
		buf.WriteString(strings.Join(wheres, " AND "))
	} else {
		buf.Reset()
	}

	return &stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) buildOrder(query scope) *stmt {
	buf := new(bytes.Buffer)

	// __key__ sorting, filter
	if len(query.orders) > 0 {
		arr := make([]string, 0, len(query.orders))
		for _, o := range query.orders {
			name := b.dialect.Quote(o.field)
			if o.field == keyFieldName {
				name = b.dialect.Quote(pkColumn)
			}
			suffix := " ASC"
			if o.direction != ascending {
				suffix = " DESC"
			}
			arr = append(arr, name+suffix)
		}
		buf.WriteString("ORDER BY " + strings.Join(arr, ","))
	}

	return &stmt{
		statement: buf,
	}
}

func (b *builder) buildStmt(query scope, args ...interface{}) (*stmt, error) {
	buf := new(bytes.Buffer)
	cmd, err := b.buildWhere(query)
	if err != nil {
		return nil, err
	}
	if !cmd.canSkip() {
		args = append(args, cmd.arguments...)
		buf.WriteString(cmd.string())
	}
	cmd = b.buildOrder(query)
	if !cmd.canSkip() {
		buf.WriteString(" " + cmd.string())
	}
	if query.limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", query.limit))
	}
	if query.offset > 0 {
		buf.WriteString(fmt.Sprintf(" OFFSET %d", query.offset))
	}
	return &stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func consoleLog(b builder, s *Stmt) {
	if b.db.logger != nil {
		b.db.logger(s)
	}
}

// func execStmt(db sqlCommon, stmt *Stmt) error {
// 	// go consoleLog(*, *stmt)
// 	conn, err := db.Prepare(stmt.Raw())
// 	if err != nil {
// 		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
// 	}
// 	defer conn.Close()
// 	result, err := conn.Exec(stmt.arguments...)
// 	if err != nil {
// 		return fmt.Errorf("goloquent: %v", err)
// 	}
// 	stmt.Result = result
// 	return nil
// }

func (b *builder) execStmt(s *stmt) error {
	ss := &Stmt{
		stmt:     *s,
		replacer: b.dialect,
	}
	go consoleLog(*b, ss)
	conn, err := b.db.Prepare(ss.Raw())
	if err != nil {
		return fmt.Errorf("goloquent: unable to prepare the sql statement: %v", err)
	}
	defer conn.Close()
	result, err := conn.Exec(ss.arguments...)
	if err != nil {
		return fmt.Errorf("goloquent: %v", err)
	}
	ss.Result = result
	return nil
}

func (b *builder) execQueryRows(s *stmt) *sql.Row {
	ss := &Stmt{
		stmt:     *s,
		replacer: b.dialect,
	}
	go consoleLog(*b, ss)
	return b.db.QueryRow(ss.Raw(), ss.arguments...)
}

func (b *builder) execQuery(s *stmt) (*sql.Rows, error) {
	ss := &Stmt{
		stmt:     *s,
		replacer: b.dialect,
	}
	go consoleLog(*b, ss)
	var rows, err = b.db.Query(ss.Raw(), ss.arguments...)
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

func (b *builder) getCommand(e *entity) (*stmt, error) {
	query := b.query
	buf := new(bytes.Buffer)
	scope := "*"
	if len(query.projection) > 0 {
		projection := make([]string, len(query.projection), len(query.projection))
		copy(projection, query.projection)
		for i := 0; i < len(query.projection); i++ {
			vv := projection[i]
			regex, _ := regexp.Compile(`\w+\(.+\)`)
			// vv = strings.Replace(vv, "`", (b.dialect.Quote(vv))[:1], -1)
			if !regex.MatchString(vv) {
				vv = b.dialect.Quote(vv)
			}
			projection[i] = vv
		}
		scope = strings.Join(projection, ",")
	}
	if len(query.distinctOn) > 0 {
		distinctOn := make([]string, len(query.distinctOn), len(query.distinctOn))
		copy(distinctOn, query.distinctOn)
		for i := 0; i < len(query.distinctOn); i++ {
			vv := distinctOn[i]
			regex, _ := regexp.Compile(`.+ as .+`)
			vv = strings.Replace(vv, "`", (b.dialect.Quote(vv))[:1], -1)
			if !regex.MatchString(vv) {
				vv = b.dialect.Quote(vv)
			}
			distinctOn[i] = vv
		}
		scope = "DISTINCT " + strings.Join(distinctOn, ",")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", scope, b.dialect.GetTable(e.Name())))
	if e.hasSoftDelete() {
		query.filters = append(query.filters, Filter{
			field:    softDeleteColumn,
			operator: equal,
			value:    nil,
		})
	}
	cmd, err := b.buildStmt(query)
	if err != nil {
		return nil, err
	}
	buf.WriteString(cmd.string())
	switch query.lockMode {
	case ReadLock:
		buf.WriteString(" LOCK IN SHARE MODE")
	case WriteLock:
		buf.WriteString(" FOR UPDATE")
	}
	buf.WriteString(";")

	return &stmt{
		statement: buf,
		arguments: cmd.arguments,
	}, nil
}

func (b *builder) run(table string, cmd *stmt) (*Iterator, error) {
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
		scope:    b.query,
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
		// it.mergeKey()
		it.patchKey()
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

	if p.Cursor != "" {
		c, err := DecodeCursor(p.Cursor)
		if err != nil {
			return err
		}

		offset := c.offset()
		if offset > 0 {
			b.query.offset = offset
			cmd.statement.Truncate(cmd.statement.Len() - 1)
			cmd.statement.WriteString(fmt.Sprintf(" OFFSET %d;", offset))
		}
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
		_, err := it.scan(vi.Interface())
		if err != nil {
			return err
		}
		// pk, isOk := data[keyFieldName].(*datastore.Key)
		// if !isOk || pk == nil {
		// 	return fmt.Errorf("goloquent: missing primary key")
		// }
		cc, _ := it.Cursor()
		p.nxtCursor = cc
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
		p.nxtCursor = Cursor{}
	}
	p.count = count
	return nil
}

func (b *builder) putStmt(parentKey []*datastore.Key, e *entity) (*stmt, error) {
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
		b.dialect.GetTable(e.Name()),
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

		// k, p := splitKey(pk)
		props[pkColumn] = Property{[]string{pkColumn}, typeOfPtrKey, stringPk(pk)}
		// props[keyColumn] = Property{[]string{keyColumn}, typeOfPtrKey, k}
		// props[parentColumn] = Property{[]string{parentColumn}, typeOfPtrKey, p}
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
		for j := 1; j <= len(cols); j++ {
			buf.WriteString(variable + ",")
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString(")")
		args = append(args, vals...)
	}
	buf.WriteString(";")

	return &stmt{
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
		if !omits.has(c) || c == pkColumn {
			continue
		}
		cols = append(cols[:i], cols[i+1:]...)
	}
	cmd.statement.Truncate(cmd.statement.Len() - 1)
	buf := new(bytes.Buffer)
	buf.WriteString(cmd.string())
	buf.WriteString(" " + b.dialect.OnConflictUpdate(e.Name(), cols))
	buf.WriteString(";")
	cmd.statement = buf
	return b.execStmt(cmd)
}

func (b *builder) saveMutation(model interface{}) (*stmt, error) {
	v := reflect.Indirect(reflect.ValueOf(model))
	if v.Len() <= 0 {
		return new(stmt), nil
	}
	e, err := newEntity(model)
	if err != nil {
		return nil, err
	}
	e.setName(b.query.table)
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", b.dialect.GetTable(e.Name())))
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
		buf.WriteString(fmt.Sprintf("%s = %s,", b.dialect.Quote(k), variable))
		args = append(args, it)
		j++
	}
	buf.Truncate(buf.Len() - 1)
	// buf.WriteString(fmt.Sprintf(
	// 	" WHERE %s = %s AND %s = %s;",
	// 	b.dialect.Quote(keyColumn), b.dialect.Bind(len(args)),
	// 	b.dialect.Quote(parentColumn), b.dialect.Bind(len(args))))
	buf.WriteString(fmt.Sprintf(" WHERE %s = %s;",
		b.dialect.Quote(pkColumn), variable))
	// k, p := splitKey(pk)
	args = append(args, stringPk(pk))

	return &stmt{
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

func (b *builder) updateWithMap(v reflect.Value) (*stmt, error) {
	buf := new(bytes.Buffer)
	args := make([]interface{}, 0)
	for _, k := range v.MapKeys() {
		vv := v.MapIndex(k)
		if k.Kind() != reflect.String {
			return nil, fmt.Errorf("goloquent: invalid map key data type, %q", k.Kind())
		}
		kk := k.String()
		if kk == keyFieldName {
			return nil, fmt.Errorf("goloquent: update __key__ is not allow")
		}
		buf.WriteString(fmt.Sprintf(" %s = %s,", b.dialect.Quote(kk), variable))
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
	return &stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) updateWithStruct(model interface{}) (*stmt, error) {
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
		buf.WriteString(fmt.Sprintf("%s = %s,", b.dialect.Quote(p.Name()), variable))
		args = append(args, it)
		i++
	}
	buf.Truncate(buf.Len() - 1)
	return &stmt{
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
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", b.dialect.GetTable(table)))
	switch vi.Type().Kind() {
	case reflect.Map:
		if vi.IsNil() || vi.Len() == 0 {
			return nil
		}
		cmd, err := b.updateWithMap(vi)
		if err != nil {
			return err
		}
		buf.WriteString(cmd.string())
		args = append(args, cmd.arguments...)
	case reflect.Struct:
		cmd, err := b.updateWithStruct(v)
		if err != nil {
			return err
		}
		buf.WriteString(" " + cmd.string())
		args = append(args, cmd.arguments...)
	default:
		return fmt.Errorf("goloquent: unsupported data type %v on `Update`", vi.Type())
	}
	cmd, err := b.buildStmt(b.query)
	if err != nil {
		return err
	}
	buf.WriteString(cmd.string())
	buf.WriteString(";")
	return b.execStmt(&stmt{
		statement: buf,
		arguments: append(args, cmd.arguments...),
	})
}

func (b *builder) concatKeys(e *entity) (*stmt, error) {
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
		// k, p := splitKey(kk)
		buf.WriteString(variable)
		args = append(args, stringPk(kk))
	}
	buf.WriteString(")")
	return &stmt{
		statement: buf,
		arguments: args,
	}, nil
}

func (b *builder) softDeleteStmt(e *entity) (*stmt, error) {
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", b.dialect.GetTable(e.Name())))
	buf.WriteString(fmt.Sprintf("%s = %s WHERE %s IN ",
		b.dialect.Quote(softDeleteColumn),
		variable,
		b.dialect.Quote(pkColumn)))
	args = append(args, time.Now().UTC().Format("2006-01-02 15:04:05"))
	ss, err := b.concatKeys(e)
	if err != nil {
		return nil, err
	}
	buf.WriteString(ss.string())
	buf.WriteString(";")
	return &stmt{
		statement: buf,
		arguments: append(args, ss.arguments...),
	}, nil
}

func (b *builder) deleteStmt(e *entity) (*stmt, error) {
	buf, args := new(bytes.Buffer), make([]interface{}, 0)
	if e.hasSoftDelete() {
		return b.softDeleteStmt(e)
	}
	buf.WriteString(fmt.Sprintf("DELETE FROM %s WHERE %s IN ",
		b.dialect.GetTable(e.Name()),
		b.dialect.Quote(pkColumn)))
	ss, err := b.concatKeys(e)
	if err != nil {
		return nil, err
	}
	buf.WriteString(ss.string())
	buf.WriteString(";")
	return &stmt{
		statement: buf,
		arguments: append(args, ss.arguments...),
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

func (b *builder) deleteByQuery() error {
	table := b.query.table
	cmd, err := b.buildStmt(b.query)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("DELETE FROM %s", b.dialect.GetTable(table)))
	buf.WriteString(cmd.string())
	buf.WriteString(";")
	cmd.statement = buf
	return b.execStmt(cmd)
}

func (b *builder) truncate(table string) error {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("TRUNCATE TABLE %s;", b.dialect.GetTable(table)))
	return b.execStmt(&stmt{
		statement: buf,
	})
}

func (b *builder) scan(dest ...interface{}) error {
	query := b.query
	table := query.table
	buf := new(bytes.Buffer)
	scope := "*"
	if len(query.projection) > 0 {
		projection := make([]string, len(query.projection), len(query.projection))
		copy(projection, query.projection)
		for i := 0; i < len(query.projection); i++ {
			vv := projection[i]
			regex, _ := regexp.Compile(`\w+\(.+\)`)
			// vv = strings.Replace(vv, "`", (b.dialect.Quote(vv))[:1], -1)
			if !regex.MatchString(vv) {
				vv = b.dialect.Quote(vv)
			}
			projection[i] = vv
		}
		scope = strings.Join(projection, ",")
	}
	if len(query.distinctOn) > 0 {
		distinctOn := make([]string, len(query.distinctOn), len(query.distinctOn))
		copy(distinctOn, query.distinctOn)
		for i := 0; i < len(query.distinctOn); i++ {
			vv := distinctOn[i]
			regex, _ := regexp.Compile(`.+ as .+`)
			vv = strings.Replace(vv, "`", (b.dialect.Quote(vv))[:1], -1)
			if !regex.MatchString(vv) {
				vv = b.dialect.Quote(vv)
			}
			distinctOn[i] = vv
		}
		scope = "DISTINCT " + strings.Join(distinctOn, ",")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", scope, b.dialect.GetTable(table)))
	ss, err := b.buildStmt(b.query)
	if err != nil {
		return err
	}
	buf.WriteString(ss.string())
	return b.execQueryRows(&stmt{
		statement: buf,
		arguments: ss.arguments,
	}).Scan(dest...)
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
		v = stringPk(vi)
	case string:
		v = vi
	case []byte:
		v = string(vi)
	case []*datastore.Key:
		arr := make([]interface{}, 0)
		for _, kk := range vi {
			arr = append(arr, stringPk(kk))
		}
		v = arr
	default:
		return nil, fmt.Errorf("goloquent: primary key has invalid data type %v", reflect.TypeOf(vi))
	}
	return v, nil
}
