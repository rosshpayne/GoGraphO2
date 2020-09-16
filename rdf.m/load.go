package rdf

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DynamoGraph/cache"
	"github.com/DynamoGraph/client"
	"github.com/DynamoGraph/db"
	"github.com/DynamoGraph/rdf.m/reader"
	"github.com/DynamoGraph/rdf.m/result"
	"github.com/DynamoGraph/rdf/grmgr"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"
)

type moviesT []*reader.MovieT
type genresT []*reader.GenreT

var (
	movies moviesT
	genres genresT
)

func syslog(s string) {
	slog.Log("rdfLoader: ", s)
}

// uid PKey of the sname-UID pairs - consumed and populated by the SaveRDFNode()

func Load(f io.Reader) error {
	//
	// context - used to shutdown goroutines that are not part fo the pipeline
	//
	ctx, cancel := context.WithCancel(context.Background())
	//
	var (
		wpStart sync.WaitGroup
		ctxEnd  sync.WaitGroup
		err     error
	)
	//
	// check tables exist
	//
	tables := []string{"DyGraph", "DyGEvent", "DyGTypes"}
	err = db.CheckTableExist(tables)
	if err != nil {
		return err
	}
	//)
	wpStart.Add(2)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(2)
	//
	// start autonomous goroutines
	go grmgr.PowerOn(ctx, &wpStart, &ctxEnd)
	go result.PowerOn(ctx, &wpStart, &ctxEnd)
	//
	// wait for processes to start
	//
	wpStart.Wait()
	//
	// create rdf reader
	//
	rdr := reader.New(f)
	//
	slog.On()
	t0 := time.Now()
	err = rdr.Read()
	if err != nil {
		return err
	}
	t1 := time.Now()
	syslog(fmt.Sprintf("Read file into maps: %s", t1.Sub(t0)))
	slog.Off()
	//
	// create nodes
	//
	var (
		bSize = 50 // batchSize
		wg    sync.WaitGroup
	)
	//  Movie Slice
	movies = make(moviesT, len(reader.Movie), len(reader.Movie))
	var i = 0
	for _, v := range reader.Movie {
		movies[i] = v
		i++
	}
	// persons slice
	persons := make([]*reader.PersonT, len(reader.Person), len(reader.Person))
	i = 0
	for _, v := range reader.Person {
		persons[i] = v
		i++
	}
	// genres slice
	genres = make(genresT, len(reader.Genre), len(reader.Genre))
	i = 0
	for _, v := range reader.Genre {
		genres[i] = v
		i++
	}
	//
	// get cpu count
	//
	cpus := runtime.NumCPU() * 2
	//
	// set concurrent goruntime limit
	//
	golimiter := grmgr.New("batch", cpus)
	//
	//
	// save Movies
	//
	//slog.On()
	{
		t0 = time.Now()
		ty, err := cache.FetchType("Film")
		if err != nil {
			return err
		}
		fmt.Printf("Type is: %#v\n", ty)
		for i := 0; i < len(movies)-1; i += bSize {
			ii := i

			golimiter.Ask()
			<-golimiter.RespCh()

			wg.Add(1)
			hw := ii + bSize
			if ii+bSize > len(movies) {
				hw = len(movies)
			}

			syslog(fmt.Sprintf("batch range for movies: %d - %d ", i, hw))
			go db.SaveMovies(movies[ii:hw], ty, "Film", golimiter, &wg)

		}
	}
	wg.Wait()
	t1 = time.Now()
	slog.On()
	syslog(fmt.Sprintf("Save time for Movies : %s\n", t1.Sub(t0)))
	slog.Off()
	//
	syslog(fmt.Sprintf("Concurrent Goroutine limit set to: %d", cpus))
	slog.Off()
	{
		fmt.Printf("\nSavePersons - %d\n", len(persons))
		//
		// save Persons
		//
		t0 := time.Now()
		ty, err := cache.FetchType("Person2")
		if err != nil {
			return err
		}
		bsizeOrig := bSize
		bSize = 100
		//for c, i := 0, 0; i < len(persons)-1; i += bSize {
		for i := 0; i < len(persons)-1; i += bSize {

			golimiter.Ask()
			<-golimiter.RespCh()

			hw := i + bSize
			if i+bSize > len(persons) {
				hw = len(persons)
			}

			//if math.Mod(float64(c), 4) == 0 {
			wg.Add(1)
			fmt.Printf("Save range for persons: %d - %d \n", i, hw)
			go db.SavePersons(persons[i:hw], ty, "Person2", golimiter, &wg)
			// }
			// c++
		}
		wg.Wait()
		t1 = time.Now()
		slog.On()
		syslog(fmt.Sprintf("Save time for Persons: %s\n", t1.Sub(t0)))
		bSize = bsizeOrig
		slog.Off()
		t0 = time.Now()
		ty, err = cache.FetchType("Genre")
		if err != nil {
			return err
		}
		db.SaveGenres(ty, "Genre")
		t1 = time.Now()
		slog.On()
		syslog(fmt.Sprintf("Save time for Genre (%d): %s", len(reader.Genre), t1.Sub(t0)))
		slog.Off()
		t0 = time.Now()
		ty, err = cache.FetchType("Character")
		if err != nil {
			return err
		}
		//
		// save Characters
		//
		for i := 0; i < len(movies)-1; i += bSize {

			ii := i
			golimiter.Ask()
			<-golimiter.RespCh()

			wg.Add(1)
			hw := ii + bSize
			if ii+bSize > len(movies) {
				hw = len(movies)
			}

			syslog(fmt.Sprintf("batch range for characters: %d - %d ", i, hw))
			go db.SaveCharacters(movies[ii:hw], ty, "Character", golimiter, &wg)

		}
		wg.Wait()
		t1 = time.Now()
		slog.On()
		syslog(fmt.Sprintf("Save time for Characters: %s\n", t1.Sub(t0)))
		slog.Off()
		t0 = time.Now()
		ty, err = cache.FetchType("Performance")
		if err != nil {
			return err
		}
		//
		// save Performances
		//
		for i := 0; i < len(movies)-1; i += bSize {

			ii := i
			golimiter.Ask()
			<-golimiter.RespCh()

			wg.Add(1)
			hw := ii + bSize
			if ii+bSize > len(movies) {
				hw = len(movies)
			}

			syslog(fmt.Sprintf("batch range for performances: %d - %d ", i, hw))
			go db.SavePerformances(movies[ii:hw], ty, "Performance", golimiter, &wg)

		}
		wg.Wait()

		t1 = time.Now()
		slog.On()
		syslog(fmt.Sprintf("Save time for Performances: %s\n", t1.Sub(t0)))
		slog.Off()

	}

	//
	// atttach nodes. ==========================================================
	//

	syslog("Start attaching nodes")

	// for i := 0; i < len(movies)-1; i += bSize {

	// 	AttachMovie2Director__(movies[i : i+bSize])
	// }

	// t1 = time.Now()
	// syslog(fmt.Sprintf("XX Finished attaching Film-Director nodes,  duration : %s", t1.Sub(t0)))

	//slog.On()
	t0 = time.Now()
	for i := 0; i < len(movies)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(movies) {
			hw = len(movies)
		}
		syslog(fmt.Sprintf("batch range for director: %d - %d ", i, hw))
		go AttachMovie2Director_(movies[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	slog.Off()
	t1 = time.Now()
	slog.On()
	syslog(fmt.Sprintf("Finished AttachMovie2Director_,  duration : %s", t1.Sub(t0)))
	slog.Off()

	t0 = time.Now()
	for i := 0; i < len(movies)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(movies) {
			hw = len(movies)
		}
		syslog(fmt.Sprintf("batch range for movie (genres): %d - %d ", i, hw))
		go AttachMovie2Genres(movies[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	t1 = time.Now()
	slog.On()
	syslog(fmt.Sprintf("Finished AttachMovie2Genres,  duration : %s", t1.Sub(t0)))
	slog.Off()

	bsizeorig := bSize
	bSize = 20
	t0 = time.Now()
	for i := 0; i < len(genres)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(genres) {
			hw = len(genres)
		}
		syslog(fmt.Sprintf("batch range for genre (movies): %d - %d ", i, hw))
		go AttachGenre2Movies(genres[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	t1 = time.Now()
	slog.On()
	syslog(fmt.Sprintf("Finished AttachGenre2Movies,  duration : %s", t1.Sub(t0)))
	slog.Off()
	bSize = bsizeorig

	t0 = time.Now()
	for i := 0; i < len(movies)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(movies) {
			hw = len(movies)
		}
		syslog(fmt.Sprintf("batch range for performance: %d - %d ", i, hw))
		go AttachMovie2Performances(movies[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	t1 = time.Now()

	slog.On()
	syslog(fmt.Sprintf("Finished AttachMovie2Performance,  duration : %s", t1.Sub(t0)))
	slog.Off()
	t0 = time.Now()
	for i := 0; i < len(movies)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(movies) {
			hw = len(movies)
		}
		syslog(fmt.Sprintf("batch range for performance: %d - %d ", i, hw))
		go AttachPerformance2Character(movies[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	t1 = time.Now()
	slog.On()
	syslog(fmt.Sprintf("Finished AttachPerformance2Character duration : %s", t1.Sub(t0)))
	slog.Off()
	t0 = time.Now()
	for i := 0; i < len(movies)-1; i += bSize {
		ii := i
		golimiter.Ask()
		<-golimiter.RespCh()

		wg.Add(1)
		hw := ii + bSize
		if ii+bSize > len(movies) {
			hw = len(movies)
		}
		syslog(fmt.Sprintf("batch range for performance: %d - %d ", i, hw))
		go AttachPerformance2Actor(movies[ii:hw], &wg, golimiter, i)
	}

	wg.Wait()
	t1 = time.Now()

	slog.On()
	syslog(fmt.Sprintf("Finished AttachPerformance2Actor  duration : %s", t1.Sub(t0)))

	result.Print <- struct{}{}

	syslog(fmt.Sprintf("Finished,  duration : %s", t1.Sub(t0)))

	cancel()
	ctxEnd.Wait()

	return nil
}

func AttachMovie2Director_(batch moviesT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {
	var (
		t0, t1 time.Time
		errs   []error
		errLen int
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	t00 := time.Now()

	logr := slog.New("director", "AttachMovie2Director", i)
	resfd := result.New("Film->Director")
	resdf := result.New("Director->Film")

	for _, v := range batch {
		// AttachNode(cUID, pUID, sortk)
		fmt.Println("movie directors: ", v.Name[0], len(v.Director))

		for _, d := range v.Director {

			logr.Log(strings.Repeat("=", 80))

			errLen = len(errs)
			if len(v.Id) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empy for movie %s", v.Name[0])))
			}
			if _, ok := reader.Person[d]; !ok {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("director (%s) is not defined in Persons for movie %s", d, v.Name[0])))
			}
			if len(v.Uid) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Uid is empy for movie %s", v.Name[0])))
			}
			if len(errs) == errLen {

				//
				// attach director -> movie
				//
				t0 = time.Now()
				var err []error
				err = client.AttachNode2(reader.Person[d].Uid, v.Uid, "A#G#:fd")

				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error director->film for film %s  director %s error: %q", v.Name, d, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed for director->film.  Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					resfd.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded for director->film Duration: %s ", t1.Sub(t0)))
				}
				//
				// attach film -> director
				//
				t0 = time.Now()
				err = client.AttachNode2(v.Uid, reader.Person[d].Uid, "A#G#:df")
				t1 = time.Now()

				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error film->director for film %s  director %s error: %q", v.Name, d, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed for film->director. Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					resdf.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded film->directors. Duration: %s ", t1.Sub(t0)))
				}
			}
		}
	}
	t01 := time.Now()

	result.Log <- resfd
	result.Log <- resdf

	logr.Log(fmt.Sprintf("Movie-Direct (%d): Duration: %s", resfd.Cnt+resdf.Cnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Director data inconsistencies: %s ", e.Error()), slog.Force)
	}

}

func AttachMovie2Genres(batch moviesT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {

	var (
		t0, t1 time.Time
		errs   []error
		errLen int
		iCnt   int
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	logr := slog.New("genre", "AttachMovie2Genres", i)

	resfg := result.New("Film->Genre")

	t00 := time.Now()

	for _, v := range batch {

		// AttachNode(cUID, pUID, sortk)
		fmt.Println("movie directors: ", v.Name[0], len(v.Director))

		for _, gId := range v.Genre {
			logr.Log(strings.Repeat("=", 80))

			errLen = len(errs)
			if len(v.Id) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empty for movie %s", v.Name[0])))
			}
			if _, ok := reader.Genre[gId]; !ok {
				//errs = append(errs, fmt.Errorf(fmt.Sprintf("movie genre %s is not defined for movie %s", reader.Genre[gId], v.Name[0])))
				errs = append(errs, fmt.Errorf(fmt.Sprintf("movie genre %s is not defined for movie %s", reader.Genre[gId].Name, v.Name[0])))
			}
			if len(errs) == errLen {

				logr.Log(fmt.Sprintf("AttachNode2: film->genre   GenreUID:  %s   Film.Uid:%s ", reader.Genre[gId].Uid, v.Uid))
				t0 = time.Now()
				var err []error

				//reader.Genre[gId].Lock()

				err = client.AttachNode2(reader.Genre[gId].Uid, v.Uid, "A#G#:fg")

				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error genre->film for film %s  director %s error: %q", v.Name, gId, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed genre->film . Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					resfg.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded genre->film. Duration: %s ", t1.Sub(t0)))
				}
			}
		}
		//
		// attach genre -> film - this will (and does) deadlock
		//
		// for _, gId := range v.Genre {
		// 	logr.Log(strings.Repeat("=", 80))

		// 	errLen = len(errs)
		// 	if len(v.Id) == 0 {
		// 		errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empty for movie %s", v.Name[0])))
		// 	}
		// 	if _, ok := reader.Genre[gId]; !ok {
		// 		errs = append(errs, fmt.Errorf(fmt.Sprintf("movie genre %s is not defined for movie %s", reader.Genre[gId], v.Name[0])))
		// 	}
		// 	if len(errs) == errLen {

		// 		logr.Log(fmt.Sprintf("AttachNode2: genre->film  genreID  %s    genreUID   %s.  FilmUID %s", reader.Genre[gId].Id, v.Id, reader.Genre[gId].Uid, v.Uid))
		// 		t0 = time.Now()
		// 		var err []error

		// 		err = client.AttachNode2(v.Uid, reader.Genre[gId].Uid, "A#G#:F")

		// 		t1 = time.Now()
		// 		if err != nil {
		// 			for _, e := range err {
		// 				logr.Log(fmt.Sprintf("AttachNode2 error film->genre for film %s  director %s error: %q", v.Name, gId, e.Error()), slog.Force)
		// 			}
		// 			logr.Log(fmt.Sprintf("AttachNode2 failed film->genre. Duration: %s ", t1.Sub(t0)), slog.Force)
		// 		} else {
		// 			resgf.Cnt++
		// 			logr.Log(fmt.Sprintf("AttachNode2 succeeded film->genre. Duration: %s ", t1.Sub(t0)))
		// 		}
		// 	}
		// }
	}
	t01 := time.Now()
	logr.On()

	result.Log <- resfg

	logr.Log(fmt.Sprintf("Movie-Genre edges (%d): Duration: %s", iCnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Movie data inconsistencies: %s ", e.Error()), slog.Force)
	}

}

func AttachGenre2Movies(batch genresT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {

	var (
		t0, t1 time.Time
		errs   []error
		iCnt   int
	)
	defer wg.Done()
	lmtr.StartR()
	defer lmtr.EndR()

	logr := slog.New("genre", "AttachGenre2Movies", i)

	resgf := result.New("Genre->Film")

	t00 := time.Now()

	// type GenreMvMap map[string(IID)][]*MovieT // genre->movie
	for _, v := range batch {

		fmt.Printf("genre batch item: %s\n", v.Name)

		for _, movie := range reader.GenreMovies[string(v.Id)] {

			logr.Log(strings.Repeat("=", 80))

			t0 = time.Now()
			var err []error
			logr.Log(fmt.Sprintf("AttachNode2: film->genre    GenreUID:  %s   Film.Uid:  %s ", reader.Genre[v.Id].Uid, movie.Uid))

			err = client.AttachNode2(movie.Uid, reader.Genre[v.Id].Uid, "A#G#:gf")

			t1 = time.Now()
			if err != nil {
				for _, e := range err {
					logr.Log(fmt.Sprintf("AttachNode2 error genre->film for film %s, Genre %s error: %q", movie.Name, reader.Genre[v.Id].Name, e.Error()), slog.Force)
				}
				logr.Log(fmt.Sprintf("AttachNode2 failed genre->film . Duration: %s ", t1.Sub(t0)), slog.Force)
			} else {
				resgf.Cnt++
				logr.Log(fmt.Sprintf("AttachNode2 succeeded genre->film. Duration: %s ", t1.Sub(t0)))
			}

		}
	}
	t01 := time.Now()
	logr.On()
	result.Log <- resgf

	logr.Log(fmt.Sprintf("Genre-Movies edges (%d): Duration: %s", iCnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Movie data inconsistencies: %s ", e.Error()), slog.Force)
	}

}

func AttachMovie2Performances(batch moviesT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {

	var (
		t0, t1 time.Time
		errs   []error
		errLen int
		iCnt   int
	)
	defer wg.Done()
	lmtr.StartR()

	defer lmtr.EndR()
	respf := result.New("Film->Performance")

	logr := slog.New("Perf", "AttachMovie2Perforamnces", i)

	t00 := time.Now()

	for _, v := range batch {

		for _, p := range v.Performance {

			logr.Log(strings.Repeat("=", 80))

			errLen = len(errs)
			if len(v.Id) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empty for movie %s", v.Name[0])))
			}
			if len(v.Uid) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Uid is empty for movie %s", v.Name[0])))
			}
			if p.Uid == nil {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("p.Uid is nil for movie %s performance %s", v.Name[0], p.Id)))
			}
			if len(errs) == errLen {

				t0 = time.Now()
				iCnt++
				var err []error
				err = client.AttachNode2(p.Uid, v.Uid, "A#G#:fp")
				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error for performance->film film %s  director %s error: %q", v.Name, p.Uid, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed performance->film. Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					respf.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded performance->film. Duration: %s ", t1.Sub(t0)))
				}
			}
		}
	}
	t01 := time.Now()
	logr.On()
	result.Log <- respf
	logr.Log(fmt.Sprintf("Movie-Performance edges (%d): Duration: %s", iCnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Performance data inconsistencies: %s ", e.Error()), slog.Force)
	}
	return
}

func AttachPerformance2Character(batch moviesT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {

	var (
		t0, t1 time.Time
		errs   []error
		errLen int
		iCnt   int
	)
	defer wg.Done()
	lmtr.StartR()

	defer lmtr.EndR()
	respc := result.New("Performance->Character")

	logr := slog.New("Perf", "AttachPerforamnce2Character", i)

	t00 := time.Now()

	for _, v := range batch {

		for _, p := range v.Performance {

			logr.Log(strings.Repeat("=", 80))

			errLen = len(errs)
			if len(v.Id) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empty for movie %s", v.Name[0])))
			}
			if p.Character == nil {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("p.Character is is nil for movie %s, performane %s", v.Name[0], p.Id)))
			}
			if len(errs) == errLen {

				t0 = time.Now()
				iCnt++
				var err []error
				err = client.AttachNode2(p.Character.Uid, p.Uid, "A#G#:pc")
				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error for performance->film film %s  director %s error: %q", v.Name, p.Uid, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed performance->film. Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					respc.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded performance->film. Duration: %s ", t1.Sub(t0)))
				}
			}
		}
	}
	t01 := time.Now()
	logr.On()
	result.Log <- respc
	logr.Log(fmt.Sprintf("Movie-Performance edges (%d): Duration: %s", iCnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Performance data inconsistencies: %s ", e.Error()), slog.Force)
	}
	return
}

func AttachPerformance2Actor(batch moviesT, wg *sync.WaitGroup, lmtr grmgr.Limiter, i int) {

	var (
		t0, t1 time.Time
		errs   []error
		errLen int
		iCnt   int
	)
	defer wg.Done()
	lmtr.StartR()

	defer lmtr.EndR()
	respc := result.New("Performance->Actor")
	resap := result.New("Actor->Performance")

	logr := slog.New("Perf", "AttachPerformance2Actor", i)

	t00 := time.Now()

	for _, v := range batch {

		for _, p := range v.Performance {

			logr.Log(strings.Repeat("=", 80))

			errLen = len(errs)
			if len(v.Id) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("v.Id is empty for movie %s", v.Name[0])))
			}
			if len(p.Actor) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("p.Actor is is nil for movie->performane  %s -> %s", v.Name[0], p.Id)))
			}
			if len(p.Actor) == 0 {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("p.Actor is is nil for movie->performane  %s -> %s", v.Name[0], p.Id)))
			}
			if _, ok := reader.Person[p.Actor]; !ok {
				errs = append(errs, fmt.Errorf(fmt.Sprintf("p.Actor IID  %q does not exist in Person map", p.Actor)))
			}

			if len(errs) == errLen {
				t0 = time.Now()
				iCnt++
				var err []error
				reader.Person[p.Actor].Lock()
				err = client.AttachNode2(reader.Person[p.Actor].Uid, p.Uid, "A#G#:pa")
				reader.Person[p.Actor].Unlock()

				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error for performance->actor performance->actor  %s  %s  %q %s Error: %s", v.Name, p.Uid, p.Actor, reader.Person[p.Actor].Uid, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed performance->actor. Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					respc.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded performance->actor. Duration: %s ", t1.Sub(t0)))
				}

				t0 = time.Now()
				iCnt++
				reader.Person[p.Actor].Lock()
				err = client.AttachNode2(p.Uid, reader.Person[p.Actor].Uid, "A#G#:ap")
				reader.Person[p.Actor].Unlock()

				t1 = time.Now()
				if err != nil {
					for _, e := range err {
						logr.Log(fmt.Sprintf("AttachNode2 error for actor->performance  %s  %s  %q %s Error: %s", v.Name, p.Uid, p.Actor, reader.Person[p.Actor].Uid, e.Error()), slog.Force)
					}
					logr.Log(fmt.Sprintf("AttachNode2 failed  actor->performance. Duration: %s ", t1.Sub(t0)), slog.Force)
				} else {
					resap.Cnt++
					logr.Log(fmt.Sprintf("AttachNode2 succeeded  actor->performance. Duration: %s ", t1.Sub(t0)))
				}
			}
		}
	}
	logr.On()
	t01 := time.Now()
	result.Log <- respc
	result.Log <- resap

	logr.Log(fmt.Sprintf("Movie-Performance edges (%d): Duration: %s", iCnt, t01.Sub(t00)), slog.Force)
	for _, e := range errs {
		logr.Log(fmt.Sprintf("Performance data inconsistencies: %s ", e.Error()), slog.Force)
	}
	return
}

func Attach() {
	// "fiN4O0iiQnyWzdXiBz5fJw==" to "AYn+rLELT2CRqz3pjG2W5g=="
	// 	Sin City                 zZzBODJyRI26u3/b/hFjmw==
	// Quinton Torintino bI5MrgBoTb6Qqt5uf7gz6A==

	cUID := util.UIDb64("bI5MrgBoTb6Qqt5uf7gz6A==").Decode()
	pUID := util.UIDb64("zZzBODJyRI26u3/b/hFjmw==").Decode()
	errs := client.AttachNode2(cUID, pUID, "A#G#:fd")

	for _, e := range errs {
		fmt.Println(e.Error())
	}
}
