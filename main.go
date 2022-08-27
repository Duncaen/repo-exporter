package main

import (
	"context"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "repo"
)

type cacheEntry struct {
	lastModified string
	etag         string
}

type Mirror struct {
	cacheOriginTime        cacheEntry
	cacheRepoSyncStartTime cacheEntry
	cacheRepoSyncEndTime   cacheEntry

	originTime    prometheus.Gauge
	syncStartTime prometheus.Gauge
	syncEndTime   prometheus.Gauge
}

var mirrors = map[string]*Mirror{}

type Repo struct {
	cacheRepoData cacheEntry

	checksum prometheus.Gauge
	staged   prometheus.Gauge
}

type repoKey struct {
	target string
	arch   string
}

var repos = map[repoKey]*Repo{}

var client *http.Client

func get(url string, cache *cacheEntry) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	if cache.etag != "" {
		req.Header.Add("If-None-Match", cache.etag)
	}
	if cache.lastModified != "" {
		req.Header.Add("If-Modified-Since", cache.lastModified)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		io.Copy(ioutil.Discard, resp.Body)
		return nil, resp.StatusCode, err
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		cache.lastModified = resp.Header.Get("Last-Modified")
		cache.etag = resp.Header.Get("ETag")
	}
	return bytes, resp.StatusCode, err
}

func head(url string) (int, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	return resp.StatusCode, err
}

func probeMirror(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	if target == "" {
		http.Error(w, "Target parameter is missing", http.StatusBadRequest)
		return
	}
	u, err := url.Parse(target)
	if err != nil {
		http.Error(w, "Target parameter invalid", http.StatusBadRequest)
		return
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	target = u.String()

	var mirror *Mirror
	var ok bool
	if mirror, ok = mirrors[target]; !ok {
		mirror = &Mirror{
			originTime: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: prometheus.BuildFQName(namespace, "", "origin_time"),
					Help: "A Unix Timestamp updated every minute on the origin",
				},
			),
			syncStartTime: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: prometheus.BuildFQName(namespace, "", "sync_start_time"),
					Help: "A Unix timestamp written by the mirror when it last started a sync",
				},
			),
			syncEndTime: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: prometheus.BuildFQName(namespace, "", "sync_end_time"),
					Help: "A Unix timestamp written by the mirror when it last finished a sync",
				},
			),
		}
		mirrors[target] = mirror
	}

	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		otimes, c, err := get(target+"/otime", &mirror.cacheOriginTime)
		if err != nil {
			return err
		}
		if c == http.StatusOK {
			// If this fails it will just stay at zero; acceptable.
			otime, err := strconv.ParseFloat(strings.TrimSpace(string(otimes)), 64)
			if err != nil {
				log.Println("Error parsing otime", err)
			}
			mirror.originTime.Set(otime)
		} else if c == http.StatusNotFound {
			mirror.originTime.Set(0)
		}
		return nil
	})
	var stimeStart float64
	g.Go(func() error {
		stimeStarts, c, err := get(target+"/stime-start", &mirror.cacheRepoSyncStartTime)
		if err != nil {
			return err
		}
		if c == http.StatusOK {
			// If this fails it will just stay at zero; acceptable.
			stimeStart, err = strconv.ParseFloat(strings.TrimSpace(string(stimeStarts)), 64)
			if err != nil {
				log.Println("Error parsing stimeStart", err)
			}
			mirror.syncStartTime.Set(stimeStart)
		} else if c == http.StatusNotFound {
			mirror.syncStartTime.Set(0)
		}
		return nil
	})
	var stimeEnd float64
	g.Go(func() error {
		stimeEnds, c, err := get(target+"/stime-end", &mirror.cacheRepoSyncEndTime)
		if err != nil {
			return err
		}
		if c == http.StatusOK {
			// If this fails it will just stay at zero; acceptable.
			stimeEnd, err = strconv.ParseFloat(strings.TrimSpace(string(stimeEnds)), 64)
			if err != nil {
				log.Println("Error parsing stimeEnd", err)
			}
			mirror.syncEndTime.Set(stimeEnd)
		} else if c == http.StatusNotFound {
			mirror.syncEndTime.Set(0)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		log.Println("Error: %s", err)
		http.Error(w, "Error: "+err.Error(), http.StatusPreconditionFailed)
		return
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(mirror.originTime)
	if stimeStart > 0 && stimeEnd > 0 {
		registry.Register(mirror.syncStartTime)
		registry.Register(mirror.syncEndTime)
	}
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func probeRepo(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	if target == "" {
		http.Error(w, "Target parameter is missing", http.StatusBadRequest)
		return
	}
	u, err := url.Parse(target)
	if err != nil {
		http.Error(w, "Target parameter invalid", http.StatusBadRequest)
		return
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	target = u.String()
	arch := r.URL.Query().Get("arch")
	if arch == "" {
		http.Error(w, "Arch parameter is missing", http.StatusBadRequest)
		return
	}
	var repo *Repo
	var ok bool
	if repo, ok = repos[repoKey{target, arch}]; !ok {
		repo = &Repo{
			checksum: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: prometheus.BuildFQName(namespace, "", "repodata_checksum"),
					Help: "CRC32 of the repodata",
				},
			),
			staged: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: prometheus.BuildFQName(namespace, "", "is_staged"),
					Help: "Non-zero if a stagedata file is present on the repo",
				},
			),
		}
		repos[repoKey{target, arch}] = repo
	}

	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		repodata, c, err := get(target+"/"+arch+"-repodata", &repo.cacheRepoData)
		if err != nil {
			return err
		}
		if c == http.StatusOK {
			repo.checksum.Set(float64(crc32.ChecksumIEEE(repodata)))
		} else if c == http.StatusNotFound {
			repo.checksum.Set(0)
		}
		return nil
	})

	g.Go(func() error {
		stagedataStatusCode, err := head(target + "/" + arch + "-stagedata")
		if err != nil {
			return err
		}
		if stagedataStatusCode == http.StatusOK {
			repo.staged.Set(1)
		} else if stagedataStatusCode == http.StatusNotFound {
			repo.staged.Set(0)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		log.Println("Error: %s", err)
		http.Error(w, "Error: "+err.Error(), http.StatusPreconditionFailed)
		return
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(repo.checksum, repo.staged)
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func doProbe(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("arch") {
		probeRepo(w, r)
	} else {
		probeMirror(w, r)
	}
}

func main() {
	client = &http.Client{Timeout: time.Second * 10}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/probe", doProbe)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>
                <head><title>XBPS Repo Exporter</title></head>
                <body>
                <h1>XBPS Repo Exporter</h1>
                </body>
                </html>`))
	})

	http.ListenAndServe(":1234", nil)
}
