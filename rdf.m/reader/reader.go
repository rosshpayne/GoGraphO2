package reader

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"
	"text/scanner"
	"unicode"

	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

const (
	Director uint8 = 1
	Actor          = 2
)

// internal identifier
type IID string

type PersonT struct {
	Id   IID
	Uid  util.UID
	Ty   uint8
	Name string
	sync.Mutex
}
type PersonMap map[IID]*PersonT

type GenreT struct {
	Id   IID
	Uid  util.UID
	Name string
	sync.Mutex
}
type GenreMap map[IID]*GenreT

type GenreMvMap map[string][]*MovieT // genre->movie

type CharacterT struct {
	Id   IID
	Uid  util.UID
	Name string
}
type CharacterMap map[IID]CharacterT

type PerformanceT struct {
	Id        IID
	Uid       util.UID
	Film      IID
	Actor     IID
	Character *CharacterT
}
type performanceMap map[IID]PerformanceT

type MovieT struct {
	Id   IID
	Uid  util.UID
	Name []string
	Ird  string
	//	Ird         time.Time. - ignore time datatype for loading purposes. treat as Time only when required otherwise stick to string.
	Genre       []IID
	Director    []IID
	Performance []*PerformanceT
}
type MovieMap map[IID]*MovieT

// transition structs between RDF tuple and dynamodb.AttributeValue

type Decoder struct {
	r     io.Reader
	batch uint // number of tuples in batch
}

func NewRDFloader(f io.Reader) *Decoder {
	return &Decoder{r: f}
}

func syslog(s string) {
	slog.Log("RDFreader: ", s)
}

type ndShortNm = string

// func openRDFfile(fname string) *os.File {
// 	f, err := os.Open(fname)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return f
// }
//
// input rdf data - rdf file data. Sent on verify channel to be consumed by verification process
//

//ype RdfNodes

type RDFReader struct {
	bs   *bufio.Scanner
	ts   scanner.Scanner
	line int
}

type Reader interface {
	Read() error
}

//
// pkg rdf
//

var (
	Movie       MovieMap
	Person      PersonMap
	Genre       GenreMap
	GenreMovies GenreMvMap
)

func init() {
	Movie = make(MovieMap)
	Person = make(PersonMap)
	Genre = make(GenreMap)
	GenreMovies = make(GenreMvMap)
}

func New(f io.Reader) Reader {

	rdf := new(RDFReader)
	//
	// create bufio (split file into lines) and text scanners (split lines into tokens)
	//
	rdf.bs = bufio.NewScanner(f)
	var s scanner.Scanner
	// treat leading `_:` as part of an identifier
	s.IsIdentRune = func(ch rune, i int) bool {
		return ch == '<' && i == 0 || ch == '>' && i > 0 || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0 || ch == '-' || ch == '.' || ch == '_'
	}
	rdf.ts = s
	//
	return rdf
}

type cur struct {
	obj, subj, pred string
	set             bool
}

// Read read rdf file and bundles common tuples into nodes upto len(n) nodes and passes back via n.
// number of nodes in n is returnd plus error if any.
func (rn *RDFReader) Read() error {

	var (
		l string
	)

	for rn.bs.Scan() {

		rn.line++

		l = strings.Trim(rn.bs.Text(), " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		if len(l) == 0 {
			continue
		}
		if l[0] == '#' {
			fmt.Printf("found  %q\n", l)
			l = strings.Trim(l[1:], " -")
			l = strings.Trim(l, "-")
			l = strings.Trim(l, " ")
		}

		switch l[:5] {

		case "direc":
			rn.loadDirectors()

		case "actor":
			rn.loadActors()

		case "genre":
			rn.loadGenre()

		case "movie":
			rn.loadMovies()
			// not interested in contents of file after movie rdfs as they are all type rdfs only
			fmt.Println("GenreMovies: len ", len(GenreMovies))
			for k, v := range GenreMovies {
				fmt.Printf("GenreMovies: %s  len %d", k, len(v))
				for _, vv := range v {
					fmt.Println("vv.Name: ", vv.Uid, vv.Name)
				}
			}
			return nil

		default:
			return fmt.Errorf("Inconsistency: expected movie genre, actor, director got %s", l[:5])
		}

	}
	return nil
}

func (rn *RDFReader) loadDirectors() {
	var (
		i               = 0
		l               string
		subj, pred, obj string
		dot             string
	)
	for rn.bs.Scan() {
		i++
		l = strings.Trim(l, " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		if len(l) == 0 {
			continue
		}
		if l[0] == '#' {
			break
		}
		rn.ts.Init(strings.NewReader(l))

		//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display

		subj, pred, obj, dot = "", "", "", ""

		for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

			//fmt.Println("In text/scanner: ", rn.ts.TokenText())
			switch i {
			case 0:
				subj = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 1:
				pred = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 2:
				obj = rn.ts.TokenText()
				switch obj[0] {
				case '"':
					obj = strings.Trim(obj, string('"'))
				case '<':
					obj = strings.TrimLeft(strings.TrimRight(obj, ">"), "<")
				}
			default:
				dot = rn.ts.TokenText()
			}
			i++
		}
		if dot != "." {
			fmt.Println("Errored: expected . got ", dot)
		}
		uid, err := util.MakeUID()
		if err != nil {
			panic(err)
		}
		if pred == "name" {
			Person[IID(subj)] = &PersonT{Id: IID(subj), Name: obj, Uid: uid, Ty: 1}
		} else {
			panic(fmt.Errorf("LoadDirectors: expected a predicate of name got %s", pred))
		}

	}

	syslog(fmt.Sprintf("Persons: %d", len(Person)))

}

func (rn *RDFReader) loadActors() {
	var (
		i               = 0
		l               string
		subj, pred, obj string
		dot             string
	)

	for rn.bs.Scan() {
		i++
		l = strings.Trim(l, " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		//fmt.Printf("rn.line %d, %q\n", i, l)
		if len(l) == 0 {
			continue
		}
		//	fmt.Printf("rn.line %d, %q\n", i, l)
		if l[0] == '#' {
			break
		}
		//fmt.Printf("init  %q\n", l)
		rn.ts.Init(strings.NewReader(l))

		//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display

		subj, pred, obj, dot = "", "", "", ""

		for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

			//fmt.Println("In text/scanner: ", rn.ts.TokenText())
			switch i {
			case 0:
				subj = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 1:
				pred = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 2:
				obj = rn.ts.TokenText()
				switch obj[0] {
				case '"':
					obj = strings.Trim(obj, string('"'))
				case '<':
					obj = strings.TrimLeft(strings.TrimRight(obj, ">"), "<")
				}
			default:
				dot = rn.ts.TokenText()
			}
			i++
		}
		if dot != "." {
			fmt.Println("Errored: expected . but got ", dot)
		}
		uid, err := util.MakeUID()
		if err != nil {
			panic(err)
		}
		if pred == "name" {
			if v, ok := Person[IID(subj)]; ok {
				// person is a director
				v.Ty |= Actor
			} else {
				Person[IID(subj)] = &PersonT{Id: IID(subj), Name: obj, Uid: uid, Ty: 1 << (Actor - 1)}
			}
		} else {
			panic(fmt.Errorf("LoadDirectors: expected a predicate of name got %s", pred))
		}

	}
	// count actors who are also diectors
	var m, j, k int
	for _, v := range Person {
		switch v.Ty {
		case 1: // directors only
			k++
		case 2: // actors
			j++
		case 3: // actors & directors
			m++
		}
		if v.Ty != 2 {
			i++
		}
	}
	syslog(fmt.Sprintf("Verification: Persons: %d", len(Person)))
	syslog(fmt.Sprintf("Verification: Directors only: %d", k))
	syslog(fmt.Sprintf("Verification: Actors only: %d", j))
	syslog(fmt.Sprintf("Verification: actors & directors: %d", m))

}

func (rn *RDFReader) loadGenre() {
	var (
		i               = 0
		l               string
		subj, pred, obj string
		dot             string
	)

	for rn.bs.Scan() {
		i++
		l = strings.Trim(l, " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		//fmt.Printf("rn.line %d, %q\n", i, l)
		if len(l) == 0 {
			continue
		}
		//	fmt.Printf("rn.line %d, %q\n", i, l)
		if l[0] == '#' {
			break
		}
		//fmt.Printf("init  %q\n", l)
		rn.ts.Init(strings.NewReader(l))

		//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display

		subj, pred, obj, dot = "", "", "", ""

		for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

			//fmt.Println("In text/scanner: ", rn.ts.TokenText())
			switch i {
			case 0:
				subj = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 1:
				pred = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 2:
				obj = rn.ts.TokenText()
				switch obj[0] {
				case '"':
					obj = strings.Trim(obj, string('"'))
				case '<':
					obj = strings.TrimLeft(strings.TrimRight(obj, ">"), "<")
				}
			default:
				dot = rn.ts.TokenText()
			}
			i++
		}
		if dot != "." {
			fmt.Println("Errored: expected . but got ", dot)
		}
		uid, err := util.MakeUID()
		if err != nil {
			panic(err)
		}
		if pred == "name" {
			Genre[IID(subj)] = &GenreT{Id: IID(subj), Name: obj, Uid: uid}
		} else {
			panic(fmt.Errorf("LoadDirectors: expected a predicate of name got %s", pred))
		}

	}
	syslog(fmt.Sprintf("Verification: Genres: %d", len(Genre)))
}

func (rn *RDFReader) loadMovies() error {
	var (
		l               string
		subj, pred, obj string
		dot             string
		newMovie        *MovieT
		newPerformance  *PerformanceT
		nameWillbeNew   bool = true
	)
	i := 0
	for rn.bs.Scan() {

		l = strings.Trim(l, " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		//fmt.Printf("rn.line %d, %q\n", i, l)
		//		fmt.Println("loadMovies: l=", l)
		if len(l) == 0 {
			continue
		}
		//	fmt.Printf("rn.line %d, %q\n", i, l)
		if l[0] == '#' {
			fmt.Println("loadMovies: break ")
			break
		}

		//		fmt.Printf("Loadmovies: %q\n", l)
		rn.ts.Init(strings.NewReader(l))

		//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display

		subj, pred, obj, dot = "", "", "", ""

		for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

			//fmt.Println("In text/scanner: ", rn.ts.TokenText())
			switch i {
			case 0:
				subj = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 1:
				pred = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 2:
				obj = rn.ts.TokenText()
				switch obj[0] {
				case '"':
					obj = strings.Trim(obj, string('"'))
				case '<':
					obj = strings.TrimLeft(strings.TrimRight(obj, ">"), "<")
				}
			default:
				dot = rn.ts.TokenText()
			}
			i++
		}
		if dot != "." {
			fmt.Println("Errored: expected . but got ", dot)
		}
		//	fmt.Println("Movies: i,subj,pred,obj: ", i, subj, pred, obj)
		//

		switch pred {

		case "name": // film name - TODO: shoud be slice to hold multi values
			if nameWillbeNew {
				//
				// Load ALL movies
				//
				i++
				if i > 120 {
					return nil
				}
				fmt.Println("LoadMovie: ", i)
				uid, err := util.MakeUID()
				if err != nil {
					panic(err)
				}
				nameWillbeNew = false
				newMovie = &MovieT{Uid: uid, Id: IID(subj)}
				newMovie.Name = append(newMovie.Name, obj)
				//
				Movie[IID(subj)] = newMovie
				//	fmt.Printf("%s\n", strings.Repeat("_", 65))
			} else {
				newMovie.Name = append(newMovie.Name, obj)
			}

		case "initial_release_date":
			nameWillbeNew = true
			newMovie.Ird = obj

		case "director.film":
			nameWillbeNew = true
			newMovie.Director = append(newMovie.Director, IID(subj))

		case "genre":
			nameWillbeNew = true
			newMovie.Genre = append(newMovie.Genre, IID(obj))
			GenreMovies[obj] = append(GenreMovies[obj], newMovie)

		case "starring":
			nameWillbeNew = true
			uid, _ := util.MakeUID()
			newPerformance = &PerformanceT{Id: IID(obj), Uid: uid, Film: newMovie.Id}
			newMovie.Performance = append(newMovie.Performance, newPerformance)
			newPerformance.loadPerformance(rn, newMovie.Id)

		default:
			fmt.Errorf("predicate %s not expected ", pred)
		}

	}
	// fmt.Printf("newMovie.performance: %#v\n", newMovie.performance)
	//	rn.bs.Scan()
	// type MovieT struct {
	// Id          IID
	// Uid         util.UID
	// Name        []string
	// Ird         string
	// Genre       []IID
	// Director    []IID
	// Performance []*PerformanceT

	syslog(fmt.Sprintf("Verification: Movies(films): %d", len(Movie)))
	return nil
}

func (p *PerformanceT) loadPerformance(rn *RDFReader, movieId IID) error {

	var (
		i               = 0
		l               string
		subj, pred, obj string
		dot             string
	)
	var ii int8
	for rn.bs.Scan() {
		i++
		l = strings.Trim(l, " ")
		l = strings.Trim(rn.bs.Text(), string(9))
		//fmt.Printf("rn.line %d, %q\n", i, l)
		if len(l) == 0 {
			continue
		}
		//	fmt.Printf("rn.line %d, %q\n", i, l)
		if l[0] == '#' {
			break
		}
		//		fmt.Printf("loadPerformance:  %q\n", l)
		rn.ts.Init(strings.NewReader(l))

		//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display

		subj, pred, obj, dot = "", "", "", ""

		for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

			//fmt.Println("In text/scanner: ", rn.ts.TokenText())
			switch i {
			case 0:
				subj = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 1:
				pred = strings.TrimLeft(strings.TrimRight(rn.ts.TokenText(), ">"), "<")
			case 2:
				obj = rn.ts.TokenText()
				switch obj[0] {
				case '"':
					obj = strings.Trim(obj, string('"'))
				case '<':
					obj = strings.TrimLeft(strings.TrimRight(obj, ">"), "<")
				}
			default:
				dot = rn.ts.TokenText()
			}
			i++
		}
		if dot != "." {
			fmt.Println("Errored: expected . but got ", dot)
		}
		//		fmt.Println("Movies: ", i, subj, pred, obj)
		//
		ii++
		switch pred {

		case "name":
			p.Character.Name = obj // character name - presuming it is the last performance entry - need to find a better way to terminate loadPerformance.

		case "performance.film":
			p.Film = IID(obj)
			if p.Film != movieId {
				fmt.Errorf("Expected film id of %q got %q", p.Id, subj)
			}

		case "performance.actor":
			if v, ok := Person[IID(obj)]; !ok {
				fmt.Errorf("Actor does not exist")
			} else {
				//tr:=v.ty & 2
				if v.Ty&2 != 2 {
					fmt.Errorf("Expected actor but got director ", p.Id, subj)
				}
			}

			if IID(subj) != p.Id {
				fmt.Errorf("Expected performance id of %q got %q", p.Id, subj)
			}
			p.Actor = IID(obj)

		case "actor.film":
			// check valid
			if p.Actor != IID(subj) {
				fmt.Errorf("Expected actor id of %q got %q", p.Actor, subj)
			}
			if movieId != IID(obj) {
				fmt.Errorf("Expected film id of %q got %q", p.Actor, subj)
			}

		case "performance.character":
			uid, _ := util.MakeUID()
			p.Character = &CharacterT{Id: IID(obj), Uid: uid}

		default:
			break
		}
		if ii == 5 {
			//fmt.Println("FInished loadPerformance  " + strings.Repeat("-", 55))
			break
		}

	}

	return nil
}
