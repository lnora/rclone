package reddit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/reddit/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep      = 1 * time.Second
	maxSleep      = 5 * time.Second
	decayConstant = 1
	rootURL       = "https://api.pushshift.io"
)

var (
	errorReadOnly = errors.New("reddit remotes are read only")
	timeUnset     = time.Unix(0, 0)
)

func init() {
	fsi := &fs.RegInfo{
		Name:  "reddit",
		NewFs: NewFs,

		MetadataInfo: &fs.MetadataInfo{
			System: map[string]fs.MetadataHelp{
				"author": {
					Type: "string",
					ReadOnly: true,
				},
			},
		},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Subreddit string `config:"subreddit"`
	Author    string `config:"author"`
	NoHead    bool   `config:"no_head"`
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name     string
	root     string
	features *fs.Features   // optional features
	opt      Options        // options for this backend
	ci       *fs.ConfigInfo // global config
	//endpoint    *url.URL
	//endpointURL string // endpoint as a string
	srv        *rest.Client
	pacer      *fs.Pacer
	cache      map[string]string
	httpClient *http.Client
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs          *Fs
	remote      string
	size        int64
	modTime     time.Time
	contentType string
	id          string // ID of the object
	remoteUrl   string
	etag        string
}

// statusError returns an error if the res contained an error
/* func statusError(res *http.Response, err error) error {
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		_ = res.Body.Close()
		return fmt.Errorf("HTTP Error: %s", res.Status)
	}
	return nil
} */

// getFsEndpoint decides if url is to be considered a file or directory,
// and returns a proper endpoint url to use for the fs.
/* func getFsEndpoint(ctx context.Context, client *http.Client, url string, opt *Options) (string, bool) {
	// If url ends with '/' it is already a proper url always assumed to be a directory.
	if url[len(url)-1] == '/' {
		return url, false
	}

	// If url does not end with '/' we send a HEAD request to decide
	// if it is directory or file, and if directory appends the missing
	// '/', or if file returns the directory url to parent instead.
	createFileResult := func() (string, bool) {
		fs.Debugf(nil, "If path is a directory you must add a trailing '/'")
		parent, _ := path.Split(url)
		return parent, true
	}
	createDirResult := func() (string, bool) {
		fs.Debugf(nil, "To avoid the initial HEAD request add a trailing '/' to the path")
		return url + "/", false
	}

	// If HEAD requests are not allowed we just have to assume it is a file.
	if opt.NoHead {
		fs.Debugf(nil, "Assuming path is a file as --http-no-head is set")
		return createFileResult()
	}

	// Use a client which doesn't follow redirects so the server
	// doesn't redirect http://host/dir to http://host/dir/
	noRedir := *client
	noRedir.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		fs.Debugf(nil, "Assuming path is a file as HEAD request could not be created: %v", err)
		return createFileResult()
	}
	addHeaders(req, opt)
	res, err := noRedir.Do(req)

	if err != nil {
		fs.Debugf(nil, "Assuming path is a file as HEAD request could not be sent: %v", err)
		return createFileResult()
	}
	if res.StatusCode == http.StatusNotFound {
		fs.Debugf(nil, "Assuming path is a directory as HEAD response is it does not exist as a file (%s)", res.Status)
		return createDirResult()
	}
	if res.StatusCode == http.StatusMovedPermanently ||
		res.StatusCode == http.StatusFound ||
		res.StatusCode == http.StatusSeeOther ||
		res.StatusCode == http.StatusTemporaryRedirect ||
		res.StatusCode == http.StatusPermanentRedirect {
		redir := res.Header.Get("Location")
		if redir != "" {
			if redir[len(redir)-1] == '/' {
				fs.Debugf(nil, "Assuming path is a directory as HEAD response is redirect (%s) to a path that ends with '/': %s", res.Status, redir)
				return createDirResult()
			}
			fs.Debugf(nil, "Assuming path is a file as HEAD response is redirect (%s) to a path that does not end with '/': %s", res.Status, redir)
			return createFileResult()
		}
		fs.Debugf(nil, "Assuming path is a file as HEAD response is redirect (%s) but no location header", res.Status)
		return createFileResult()
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		// Example is 403 (http.StatusForbidden) for servers not allowing HEAD requests.
		fs.Debugf(nil, "Assuming path is a file as HEAD response is an error (%s)", res.Status)
		return createFileResult()
	}

	fs.Debugf(nil, "Assuming path is a file as HEAD response is success (%s)", res.Status)
	return createFileResult()
} */

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	client := fshttp.NewClient(ctx)

	ci := fs.GetConfig(ctx)
	f := &Fs{
		name:       name,
		root:       root,
		ci:         ci,
		opt:        *opt,
		httpClient: client,
		srv:        rest.NewClient(client).SetRoot(rootURL),
		pacer:      fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		cache:      map[string]string{},
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	return f, nil
}

// Name returns the configured name of the file system
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root for the filesystem
func (f *Fs) Root() string {
	return f.root
}

// String returns the URL for the filesystem
func (f *Fs) String() string {
	return "/r/18_19"
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision is the remote http file system's modtime precision, which we have no way of knowing. We estimate at 1s
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// NewObject creates a new remote http file object
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	err := o.stat(ctx)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Errors returned by parseName
/*var (
	errURLJoinFailed     = errors.New("URLJoin failed")
	errFoundQuestionMark = errors.New("found ? in URL")
	errHostMismatch      = errors.New("host mismatch")
	errSchemeMismatch    = errors.New("scheme mismatch")
	errNotUnderRoot      = errors.New("not under root")
	errNameIsEmpty       = errors.New("name is empty")
	errNameContainsSlash = errors.New("name contains /")
)*/

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}

	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}


func getGfyCat(f *Fs, ctx context.Context, name string) (u string) {
	type GfyCat struct {
		GfyItem struct {
			Mp4URL             string        `json:"mp4Url"`
		} `json:"gfyItem"`
	}

	var (
		gfycat GfyCat
		resp *http.Response
		err error
	)

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/v1/gfycats/"+name,
		RootURL:    "https://api.redgifs.com",
	}

	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &gfycat)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		fmt.Printf("%+v\n", err)
		return ""
	}
	return gfycat.GfyItem.Mp4URL
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	var (
		data *api.ListItems
		resp *http.Response
	)

	if err != nil {
		return nil, err
	}

	values := url.Values{}
	if f.opt.Subreddit != "" {		
		values.Set("subreddit", f.opt.Subreddit)
	}
	if f.opt.Author != "" {
		values.Set("author", f.opt.Author)
	}
	values.Set("size", "100")
	values.Set("sort", "desc")

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/reddit/search/submission",
		Parameters: values,
	}

	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &data)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		fmt.Printf("...Error %v\n", err)
		return nil, err
	}

	var (
		entriesMu sync.Mutex // to protect entries
		wg        sync.WaitGroup
		checkers  = f.ci.Checkers
		in        = make(chan api.Item, checkers)
	)

	add := func(entry fs.DirEntry) {
		entriesMu.Lock()
		entries = append(entries, entry)
		entriesMu.Unlock()
	}
	for i := 0; i < checkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remote := range in {
				u, _ := url.Parse(remote.Url)
				file := &Object{
					remote:    strings.Trim(u.Path, "/"),
					fs:        f,
					id:        remote.Id,
					remoteUrl: remote.Url,
				}
				switch err := file.stat(ctx); err {
				case nil:
					_, ok := f.cache[file.etag]
					if !ok {
						add(file)
						f.cache[file.etag] = ""
					}
				default:
					fs.Debugf(remote, "skipping because of error: %v", err)
				}
			}
		}()
	}

	for _, e := range data.Data {
		fmt.Printf("%+v\n", e)
		if e.Domain == "i.redd.it" || e.Domain == "i.imgur.com" {
			in <- e
		}
		if e.Domain == "redgifs.com" || e.Domain ==  "gfycat.com" {
			u, _ := url.Parse(e.Url)
			v := path.Base(u.Path)
			_, ok := f.cache[v] 
			if !ok {
				e.Url = getGfyCat(f, ctx, v)
				fmt.Printf("%v\n", e.Url)
				f.cache[v] = ""
				in <- e				
			}
		}
	}
	close(in)
	wg.Wait()
	return entries, nil
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// Fs is the filesystem this remote http file object is located within
func (o *Object) Fs() fs.Info {
	return o.fs
}

// statusError returns an error if the res contained an error
func statusError(res *http.Response, err error) error {
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		_ = res.Body.Close()
		return fmt.Errorf("HTTP Error: %s", res.Status)
	}
	return nil
}

// String returns the URL to the remote HTTP file
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote the name of the remote HTTP file, relative to the fs root
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	if o.etag != "" {
		return o.etag, nil
	}
	return "", nil
}

// Size returns the size in bytes of the remote http file
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the remote http file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// url returns the native url of the object
func (o *Object) url() string {
	return o.remoteUrl
}

// parse s into an int64, on failure return def
func parseInt64(s string, def int64) int64 {
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil {
		return def
	}
	return n
}

// stat updates the info field in the Object
func (o *Object) stat(ctx context.Context) error {
	if o.fs.opt.NoHead {
		o.size = -1
		o.modTime = timeUnset
		o.contentType = fs.MimeType(ctx, o)
		return nil
	}
	url := o.url()
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("stat failed: %w", err)
	}
	res, err := o.fs.httpClient.Do(req)
	if err == nil && res.StatusCode == http.StatusNotFound {
		return fs.ErrorObjectNotFound
	}
	err = statusError(res, err)
	if err != nil {
		return fmt.Errorf("failed to stat: %w", err)
	}
	t, err := http.ParseTime(res.Header.Get("Last-Modified"))
	if err != nil {
		t = timeUnset
	}
	o.size = parseInt64(res.Header.Get("Content-Length"), -1)
	o.modTime = t
	o.contentType = res.Header.Get("Content-Type")
	o.etag = res.Header.Get("etag")
	return nil
}

// SetModTime sets the modification and access time to the specified time
//
// it also updates the info field
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return errorReadOnly
}

// Storable returns whether the remote http file is a regular file (not a directory, symbolic link, block device, character device, named pipe, etc.)
func (o *Object) Storable() bool {
	return true
}

// Open a remote http file object for reading. Seek is supported
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	url := o.url()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}

	// Add optional headers
	for k, v := range fs.OpenOptionHeaders(options) {
		req.Header.Add(k, v)
	}

	// Do the request
	res, err := o.fs.httpClient.Do(req)
	err = statusError(res, err)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}
	return res.Body, nil
}

// Hashes returns hash.HashNone to indicate remote hashing is unavailable
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// Mkdir makes the root directory of the Fs object
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Remove a remote http file object
func (o *Object) Remove(ctx context.Context) error {
	return errorReadOnly
}

// Rmdir removes the root directory of the Fs object
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errorReadOnly
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.contentType
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)
