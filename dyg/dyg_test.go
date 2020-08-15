package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/DynamoGraph/db"
)

func TestEq(t *testing.T) {
	t0 := time.Now()

	uid := []byte("5lFOnTStSYWqmi8S6FDFDQ==")
	at, err := db.FetchData(uid, "A#")
	if err != nil {
		t.Error(err)
	}
	t1 := time.Now()

	fmt.Println("DB Access: ", t1.Sub(t0))
	ty := at.GetType()
	if len(ty) == 0 {
		t.Fatal("Type not found")
	}
	fmt.Println("ty: ", ty)
	{
		t0 := time.Now()
		lv := int64(62)
		uid_, err := eqUID(ty, "Siblings", "Age", lv, at)
		fmt.Println("uid_ ", uid_, err)
		f1 := eq(0, uid_)
		f2 := eq(1, uid_)

		t1 := time.Now()
		fmt.Println("DB Access: ", t1.Sub(t0))

		fmt.Println("f = ", f1, f2)
	}
	{
		t0 := time.Now()
		lv := "Ian Payne"
		uid_, err := eqUID(ty, "Siblings", "Name", lv, at)
		fmt.Println("uid_ ", uid_, err)
		f1 := eq(0, uid_)
		f2 := eq(1, uid_)
		t1 := time.Now()
		fmt.Println("DB Access: ", t1.Sub(t0))
		fmt.Println("f = ", f1, f2)
	}
	{
		t0 := time.Now()
		lv := "Paul Payne"
		uid_, err := eqUID(ty, "Siblings", "Name", lv, at)
		fmt.Println("uid_ ", uid_, err)
		// 		f1 := eq(0, uid_)
		// 		f2 := eq(1, uid_)
		f3 := eq(0, uid_) || eq(1, uid_)
		t1 := time.Now()
		fmt.Println("DB Access: ", t1.Sub(t0))

		fmt.Println("f = ", f3)
	}
	{
		t0 := time.Now()
		lv := "Ross Payne"
		uid_, err := eqUID(ty, "Siblings", "Name", lv, at)
		fmt.Println("uid_ ", uid_, err)
		f1 := eq(0, uid_)
		f2 := eq(1, uid_)
		t1 := time.Now()
		fmt.Println("DB Access: ", t1.Sub(t0))

		fmt.Println("f = ", f1, f2)
	}
}
