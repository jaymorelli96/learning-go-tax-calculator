package database

import (
	"database/sql"

	"github.com/jaymorelli96/learning-go-tax-calculator/internal/entity"
)

type OrderRepository struct {
	Db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{
		Db: db,
	}
}

func (r *OrderRepository) Save(order *entity.Order) error {
	_, err := r.Db.Exec(
		"INSERT into ORDERS (id, price, tax, final_price) Values(?, ?, ?, ?)",
		order.ID, order.Price, order.Tax, order.FinalPrice)

	if err != nil {
		return err
	}
	return nil
}

func (r *OrderRepository) GetTotal() (float64, error) {
	var total float64
	err := r.Db.QueryRow("SELECT COUNT(*) from ORDERS").Scan(&total)

	if err != nil {
		return 0, err
	}

	return total, nil
}
