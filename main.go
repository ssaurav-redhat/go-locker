package main

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"log"
	"ssaurav/go-locker/pkg/model"
	"ssaurav/go-locker/pkg/service"
	"time"
)

func main() {
	db := service.GetDbInstance()

	h := sha1.New()
	h.Write([]byte("test-org"))

	uuid := hex.EncodeToString(h.Sum(nil))

	log.Println(uuid)

	//get current epoch timestamp
	currEpochTs := time.Now().UTC().Unix()

	//get next db runtime
	waitTs := db.GetNextRuntime()

	//waitforit interval
	waitForIt := waitTs - currEpochTs

	log.Println("Going to sleep before next execution wake up call")

	//sleep for the same
	time.Sleep(time.Duration(waitForIt) * time.Second)

	log.Println("Program back up")

	expiry := time.Now().Add(10 * time.Second).Unix()

	lock := model.Lock{
		Uuid:   uuid,
		Expiry: expiry,
	}

	//get lock instance if present
	existLock, err := db.GetLock(uuid)
	if err != nil && err != sql.ErrNoRows {
		log.Println("Could not acquire lock ", err)
		return
	}

	if existLock != nil {
		log.Println("Lock is found")
		// check if expired, if expired return error
		if !existLock.IsExpired() {
			log.Println("Lock is already existing in db, aborting job")
			return
		}

		//delete the lock from db if it is expired
		if err := db.DeleteExpiredLock(uuid); err != nil {
			log.Println("Error while deleting existing lock")
			return
		}

		log.Println("Lock deleted successfully")
	}

	//insert a new lock in db
	if err := db.CreateLock(lock); err != nil {
		log.Println("Could not acquire lock ", err)
		return
	}

	log.Println("Lock acquired successfully")

	//do some stuff with random error
	log.Println("Doing random stuff")

	//release lock
	log.Println("Releasing lock now")
	if err := db.DeleteLock(uuid); err != nil {
		log.Println("Error while deleting lock from db")
		return
	}

}
