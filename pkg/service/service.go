package service

import (
	"database/sql"
	"fmt"
	"log"
	"ssaurav/go-locker/pkg/model"
	"time"

	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "postgres"
)

var dbConn Database

type Database struct {
	*sql.DB
}

func init() {
	conn := fmt.Sprintf("host=%s port=%d user=%s password='%s'"+
		"dbname=%s sslmode=require", host, port, user, password, dbname)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("Error while connecting to db: ", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	log.Println("Db connected")

	dbConn.DB = db

}

func GetDbInstance() Database {
	return dbConn
}

// func (db Database) TransactionalStep() {

// 	tx, err := db.BeginTx(context.Background(), nil)
// 	if err != nil {
// 		log.Println("Could not start transaction! ", err)
// 		return
// 	}

// 	insertDynStmt := `insert into "stu"("name", "roll") values($1,$2)`
// 	_, err = tx.Exec(insertDynStmt, "Jane", 1)

// 	if err != nil {
// 		log.Println("Error while inserting ", err)
// 		tx.Rollback()
// 		return
// 	}

// 	if err := tx.Commit(); err != nil {
// 		log.Println("Error while commiting transaction")
// 		tx.Rollback()
// 		return
// 	}

// }

func (db Database) GetNextRuntime() int64 {

	fetchExecutionTimeStmt := `select timestamp from waitforit`

	var execTimeStamp int64

	err := db.QueryRow(fetchExecutionTimeStmt).Scan(&execTimeStamp)
	if err != nil {
		log.Fatal("Could not get next execution time ", err)
	}

	return execTimeStamp

}

// GetLock fetches the lock for the provided uuid
func (db Database) GetLock(uuid string) (*model.Lock, error) {

	getLockStmt := `select * from lock where uuid=$1`

	var lock model.Lock

	if err := db.QueryRow(getLockStmt, uuid).Scan(&lock.Uuid, &lock.Expiry); err != nil {
		return nil, err
	}

	return &lock, nil
}

// GetLock fetches the lock for the provided uuid
func (db Database) DeleteExpiredLock(uuid string) error {

	currTime := time.Now().Unix()

	deleteLockStmt := `delete from lock where uuid=$1 and expiry<=$2`

	res, err := db.Exec(deleteLockStmt, uuid, currTime)

	if err != nil {
		return err
	}

	val, err := res.RowsAffected()
	if err != nil {
		return err
	}

	// if row affected is 0 this means some other instance might have won over this and delted it
	if val == 0 {
		log.Println("Row was already deleted by another instance")
	}

	return nil
}

// GetLock fetches the lock for the provided uuid
func (db Database) DeleteLock(uuid string) error {

	deleteLockStmt := `delete from lock where uuid=$1`

	res, err := db.Exec(deleteLockStmt, uuid)

	if err != nil {
		return err
	}

	val, err := res.RowsAffected()
	if err != nil {
		return err
	}

	// if row affected is 0 this means some other instance might have won over this and delted it
	if val == 0 {
		log.Println("Row was already deleted by another instance")
	}

	return nil
}

// CreateLock inserts lock into table
func (db Database) CreateLock(lock model.Lock) error {

	insertStmt := `insert into lock values ($1,$2)`

	_, err := db.Exec(insertStmt, lock.Uuid, lock.Expiry)

	if err != nil {
		return err
	}

	return nil
}
