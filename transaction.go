package goloquent

import "cloud.google.com/go/datastore"

// TransactionHandler :
type TransactionHandler func(*DB) error

// Transaction :
type Transaction struct {
	query *Query
}

// Commit :
func (tx *Transaction) Commit() error {
	return nil
}

// Migrate :
func (tx *Transaction) Migrate(model ...interface{}) error {
	// return newBuilder(tx.Query).Migrate(model)
	return nil
}

// Create :
func (tx *Transaction) Create(model interface{}, parentKey *datastore.Key) error {
	// return newBuilder(tx.Query).Create(model, parentKey)
	return nil
}

// Upsert :
func (tx *Transaction) Upsert(model interface{}) error {
	return nil
}

// Delete :
func (tx *Transaction) Delete(key ...*datastore.Key) error {
	return nil
}

// Update :
func (tx *Transaction) Update(value map[string]interface{}) error {
	return nil
}
