package postgres

import "gorm.io/gorm/clause"

type Query struct {
}

func NewQuery() *Query {
	return &Query{}
}

func (r *Query) LockForUpdate() clause.Expression {
	return clause.Locking{Strength: "UPDATE"}
}

func (r *Query) RandomOrder() string {
	return "RANDOM()"
}

func (r *Query) SharedLock() clause.Expression {
	return clause.Locking{Strength: "SHARE"}
}
