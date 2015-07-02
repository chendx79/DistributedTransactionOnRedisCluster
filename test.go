// test.go
package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/nu7hatch/gouuid"
	"gopkg.in/redis.v3"
)

var s = rand.NewSource(100)
var r = rand.New(s)

type ClusterClient struct {
	client *redis.ClusterClient
}

func InitClusterClient() *ClusterClient {
	return new(ClusterClient)
}

func (c *ClusterClient) Init() {
	c.client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"192.168.16.201:", "192.168.16.202:8202", "192.168.16.203:8203", "192.168.16.204:8204", "192.168.16.205:8205", "192.168.16.206:8206"},
	})
	c.client.Ping()
}

func (c *ClusterClient) InitTestData() {
	c.client.Set("player:1:gold", "100", 0)
	c.client.Set("player:2:item:1", "10", 0)
}

func (c *ClusterClient) getLock(key string) bool {
	//fmt.Println(c.client)
	lock, err := c.client.Incr(key).Result()
	defer c.client.Expire(key, time.Second*1)

	if err != nil {
		panic(err)
	}
	if lock != 1 {
		fmt.Println(key, " is locked, transaction canceled.")
		return false
	}

	return true
}

func (c *ClusterClient) unlock(key string) {
	c.client.Del(key)
}

func GetUUID() string {
	u, _ := uuid.NewV4()
	return strings.Replace(u.String(), "-", "", 4)
}

func (c *ClusterClient) doTransaction(AKey string, AChange int64, BKey string, BChange int64) bool {
	//Get all locks
	if !c.getLock("lock:" + AKey) {
		return false
	}
	defer c.unlock("lock:" + AKey)

	if !c.getLock("lock:" + BKey) {
		return false
	}
	defer c.unlock("lock:" + BKey)

	//Get key values
	AString, err := c.client.Get(AKey).Result()
	if err != nil {
		panic(err)
	}
	AVal, _ := strconv.Atoi(AString)
	fmt.Println(AKey, AVal)
	if int64(AVal)+AChange < 0 {
		return false
	}

	BString, err := c.client.Get(BKey).Result()
	if err != nil {
		panic(err)
	}
	BVal, _ := strconv.Atoi(BString)
	fmt.Println(BKey, BVal)
	if int64(BVal)+BChange < 0 {
		return false
	}
	//Save values before transaction begin
	recordKey := GetUUID()
	c.client.HMSet(recordKey, AKey, AString, BKey, BString)
	c.client.SAdd("1_records", recordKey)

	//Write new values
	ANewVal, err := c.client.IncrBy(AKey, AChange).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(AKey, ANewVal)

	time.Sleep(time.Millisecond * time.Duration(r.Intn(300)))

	BNewVal, err := c.client.IncrBy(BKey, BChange).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(BKey, BNewVal)
	fmt.Println("-------------------")

	//Remove old value record
	c.client.SRem("1_records", recordKey)
	c.client.Del(recordKey)

	time.Sleep(time.Millisecond * time.Duration(r.Intn(10)))

	return true
}

func (c *ClusterClient) Restore() {
	r, _ := c.client.SMembers("1_records").Result()
	if len(r) == 0 {
		return
	}

	log.Println("Found broken transaction", r)
	for i := 0; i < len(r); i++ {
		h, _ := c.client.HGetAll(r[i]).Result()
		for j := 0; j <= len(h)/2; j += 2 {
			c.client.Set(h[j], h[j+1], 0)
		}
		c.client.SRem("1_records", r[i])
		c.client.Del(r[i])
	}
	time.Sleep(time.Second * 5)
}

func worker(AKey string, AChange int64, BKey string, BChange int64) {
	c := InitClusterClient()
	c.Init()
	for true {
		c.doTransaction(AKey, AChange, BKey, BChange)
		time.Sleep(time.Millisecond * time.Duration(r.Intn(100)))
	}
}

func main() {
	c := InitClusterClient()
	c.Init()
	c.Restore()
	go worker("player:1:gold", 10, "player:2:gold", -10)
	go worker("player:1:gold", -10, "player:2:gold", 10)
	for true {
		time.Sleep(time.Second)
	}
}
