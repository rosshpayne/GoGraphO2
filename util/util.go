package util

import (
	"encoding/base64"
	"log"

	"github.com/satori/go.uuid"
)

type UIDb64s = string

type UID []byte

func MakeUID() (UID, error) {
	u := uuid.Must(uuid.NewV4())
	uuibin, err := u.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return uuibin, nil
}

// convert UID to base64 string
func (u UID) String() string {
	return base64.StdEncoding.EncodeToString(u)
}

func (uid UID) Encodeb64() UIDb64 {
	if len(uid) == 16 {
		u := make([]byte, len(uid)*3/2, len(uid)*3/2)
		base64.StdEncoding.Encode(u, uid)
		return u
	}
	// already encoded as base64
	return UIDb64(uid)

}

func (uid UID) Encodeb64_() UIDb64 {

	u := make([]byte, len(uid)*3/2, len(uid)*3/2)
	base64.StdEncoding.Encode(u, uid)
	return u

}

type UIDb64 []byte

func MakeUIDb64() UIDb64 {
	u := uuid.Must(uuid.NewV4())
	uuibin, err := u.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%d %s %d\n", uuibin, u.String(), len(u.String()))
	b := make(UIDb64, len(uuibin)*3/2, len(uuibin)*3/2)
	base64.StdEncoding.Encode(b, uuibin)
	return b
}

func (u UIDb64) String() UIDb64s {
	return string(u)
}

func (ub64 UIDb64) Decode() UID {
	//return UID(ub64)
	u := make(UID, len(ub64)*2/3)
	base64.StdEncoding.Decode(u, ub64)
	return u
}

func (ub64 UIDb64) Decode2() UID {
	u := make(UID, len(ub64)*2/3)
	base64.StdEncoding.Decode(u, ub64)
	return u
}

// func Encodeb64(b UIDb64, u UID) {
// 	base64.StdEncoding.Encode(b, u)
// }
// func Decodeb64(u UID, ub64 UIDb64) {
// 	base64.StdEncoding.Decode(u, ub64)
// }
