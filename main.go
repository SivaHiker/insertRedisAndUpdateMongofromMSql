package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

var jobs chan UserInfo
var done chan bool
var counter int64
var c *mgo.Collection
var dbConn *sql.DB
var user platformUser

func main() {


	jobs = make(chan UserInfo, 10000)
	done = make(chan bool, 1)

	session, err := mgo.Dial("10.15.0.145")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c = session.DB("userlist").C("newuserdata")
	fmt.Println(c.Name)

	//MySql DB Connection
	dbConn = getDBConnection()
	dbConn.SetMaxOpenConns(500)

	defer dbConn.Close()
	err = dbConn.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}

	for w := 1; w <= 500; w++ {
		go workerPool()
	}

	item := UserInfo{}

	find := c.Find(bson.M{})

	items := find.Iter()

	for items.Next(&item) {
		jobs<- item
	}

	<-done
	fmt.Println("Total Updated Documents ",counter)
}

func workerPool() {
	for (true) {
		select {
		case item,ok := <-jobs:
			if ok {
				fmt.Println(item.UserData.UID)
				rows2,err1 := dbConn.Query("select * from platform_user where  hike_uid = '"+item.UserData.UID+"'")
				if(err1!=nil){
					fmt.Println(err1)
				}
				if(rows2.Next()){
					err := rows2.Scan(&user.ID,&user.HikeUID, &user.PlatformUID, &user.PlatformToken, &user.Msisdn,
						&user.HikeToken,&user.CreateTime,&user.UpdateTs, &user.Status)
					if(err!=nil) {
						fmt.Println(err.Error())
					}
					//updateerr := c.Update(item, bson.M{"$set": bson.M{"userdata.platformuid": user.PlatformUID}})
					fmt.Println(user.PlatformToken)
					updateerr2 := c.Update(item, bson.M{"$set": bson.M{"userdata.platformtoken": user.PlatformToken}})
					//fmt.Println(updateerr)
					fmt.Println(updateerr2)
					counter++
					fmt.Println("Migrated records till now --- >", counter)
					rows2.Close()
				}
			}
		case <-done:
			done<-true
		}
	}
}


func getDBConnection() *sql.DB{

	db, err := sql.Open("mysql", "platform:p1@tf0rmD1st@tcp(10.15.8.4:3306)/usersdb?parseTime=true")
	if(err!=nil){
		fmt.Println(err)
	}
	return db
}

type UserInfo struct {
	ID       bson.ObjectId `bson:"_id,omitempty"`
	UserData UserData `json:"UserData"`
	Flag   bool `json:"flag"`
	Active bool `json:"active"`
}

//type UserData struct {
//	Msisdn string `json:"msisdn"`
//	Token  string `json:"token"`
//	UID    string `json:"uid"`
//}

type UserData struct {
	Msisdn        string `json:"msisdn"`
	Token         string `json:"token"`
	UID           string `json:"uid"`
	Platformuid   string `json:"platformuid"`
	Platformtoken string `json:"platformtoken"`
}

type platformUser struct {
	CreateTime    time.Time  `json:"create_time"`
	HikeToken     string `json:"hike_token"`
	HikeUID       string `json:"hike_uid"`
	ID            int64    `json:"id"`
	Msisdn        string `json:"msisdn"`
	PlatformToken string `json:"platform_token"`
	PlatformUID   string `json:"platform_uid"`
	Status        sql.NullString `json:"status"`
	UpdateTs      time.Time `json:"update_ts"`
}