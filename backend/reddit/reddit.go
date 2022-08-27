package reddit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
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
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep        = 1 * time.Second
	maxSleep        = 5 * time.Second
	decayConstant   = 1
	rootURL         = "https://api.pushshift.io"
	userPrefix      = "u/"
	subredditPrefix = "r/"
)

var (
	errorReadOnly  = errors.New("reddit remotes are read only")
	errorMinBefore = errors.New("minBefore")
	timeUnset      = time.Unix(0, 0)
	itemsMu        sync.Mutex
	etagsMu        sync.Mutex
)

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func init() {
	fsi := &fs.RegInfo{
		Name:        "reddit",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Options: []fs.Option{
			{
				Name: "author",
			},
			{
				Name: "subreddit",
			}, {
				Name:     "no_head",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "ps_size",
				Default:  25,
				Advanced: true,
			},
			{
				Name:     "small_pics",
				Default:  -1,
				Advanced: true,
			},
			{
				Name:     "mkdirs",
				Default:  false,
				Advanced: true,
			}},

		MetadataInfo: &fs.MetadataInfo{
			System: map[string]fs.MetadataHelp{
				"author": {
					Type:     "string",
					ReadOnly: true,
				},
			},
		},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Subreddit  string               `config:"subreddit"`
	Author     string               `config:"author"`
	NoHead     bool                 `config:"no_head"`
	Checkpoint string               `config:"checkpoint"`
	PsSize     int                  `config:"ps_size"`
	SmallPics  int                  `config:"small_pics"`
	Mkdirs     bool                 `config:"mkdirs"`
	Enc        encoder.MultiEncoder `config:"encoding"`
}

type PushshiftQuery struct {
	currentBefore int64
	minBefore     int64
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
	cache      map[string]bool
	httpClient *http.Client
	dirCache   *dircache.DirCache // Map of directory path to directory id
	//usersData  map[string]fs.DirEntries
	dirTree   dirtree.DirTree
	psQueries map[string]PushshiftQuery
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs          *Fs
	remote      string
	size        int64
	modTime     time.Time
	contentType string
	id          string // ID of the object
	remoteURL   string
	etag        string
	sourceURL   string
	rawData     *json.RawMessage
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
		cache:      map[string]bool{},
		dirTree:    dirtree.New(),
		psQueries:  map[string]PushshiftQuery{},
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: false,
		ReadMetadata:            true,
		WriteMetadata:           true,
	}).Fill(ctx, f)

	f.dirCache = dircache.New(root, root, f)

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
	return fmt.Sprintf("root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision of the ModTimes in this Fs
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
		return nil, fs.ErrorObjectNotFound
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

func getGfyCat(ctx context.Context, f *Fs, name string) (u string) {
	type GfyCat struct {
		Gif struct {
			Urls struct {
				Poster     string `json:"poster"`
				Vthumbnail string `json:"vthumbnail"`
				Thumbnail  string `json:"thumbnail"`
				Sd         string `json:"sd"`
				Hd         string `json:"hd"`
			} `json:"urls"`
		} `json:"gif"`
	}

	var (
		gfycat GfyCat
		resp   *http.Response
		err    error
	)

	opts := rest.Opts{
		Method:  "GET",
		Path:    "/v2/gifs/" + name,
		RootURL: "https://api.redgifs.com",
	}

	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &gfycat)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		fmt.Printf("%+v\n", err)
		return ""
	}

	if gfycat.Gif.Urls.Hd != "" {
		return gfycat.Gif.Urls.Hd
	}

	return gfycat.Gif.Urls.Sd
}

func refreshDir(ctx context.Context, f *Fs, pathID string) error {
	var (
		subreddit, author string
		before, after     int64
	)
	dirID, fileID := path.Split(pathID)
	switch dirID {
	case userPrefix:
		author = fileID
	case subredditPrefix:
		subreddit = fileID
	}

	itemsMu.Lock()
	psBefore, psBeforeOk := f.psQueries[pathID]
	itemsMu.Unlock()
	if psBeforeOk {
		before = psBefore.currentBefore

		if before != 0 && before <= psBefore.minBefore {
			return errorMinBefore
		}
	} else {
		psBefore = PushshiftQuery{}
		createdUtc, err := f.psGetRange(ctx, subreddit, author)

		if err == nil {
			psBefore.minBefore = createdUtc
			itemsMu.Lock()
			f.psQueries[pathID] = psBefore
			itemsMu.Unlock()
		}

		before = math.MaxInt64
		after = math.MinInt64
		itemsMu.Lock()
		f.dirTree[pathID].ForObject(func(obj fs.Object) {
			o, ok := obj.(*Object)
			if !ok {
				log.Trace(o, "internal error: not a reddit object")
			}
			createdUtc := o.modTime.Unix()
			before = min(before, createdUtc)
			after = max(after, createdUtc)
		})
		itemsMu.Unlock()
		if before == math.MaxInt64 {
			before = time.Now().Unix()
		}
		if after == math.MinInt64 {
			after = time.Now().Unix()
		}
	}

	fetch := func(isBefore bool) (err error) {
		var data []api.Item
		if isBefore {
			data, err = f.callPushshift(ctx, subreddit, author, before, 0)
		} else if after != 0 {
			data, err = f.callPushshift(ctx, subreddit, author, 0, after)
		}

		if len(data) > 0 {
			err = f.list(ctx, pathID, data)
		}
		return err
	}

	err := fetch(true)
	if err != nil {
		return err
	}
	err = fetch(false)

	return err
}

func (f *Fs) psGetRange(ctx context.Context, subreddit, author string) (createdUtc int64, err error) {
	var (
		data *api.ListItems
		resp *http.Response
	)

	values := url.Values{}
	if subreddit != "" {
		values.Set("subreddit", subreddit)
	}
	if author != "" {
		values.Set("author", author)
	}
	values.Set("size", "1")
	values.Set("sort", "asc")
	values.Set("fields", "created_utc")

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
		return 0, err
	}

	if len(data.Data) > 0 {
		return data.Data[0].CreatedUtc, err
	}
	return 0, err
}

func (f *Fs) callPushshift(ctx context.Context, subreddit, author string, before int64, after int64) (items []api.Item, err error) {
	defer log.Trace(f, "subreddit=%v,author=%v,before=%v,after=%v", subreddit, author, before, after)
	var (
		data *api.ListItems
		resp *http.Response
	)

	values := url.Values{}
	if subreddit != "" {
		values.Set("subreddit", subreddit)
	}
	if author != "" {
		values.Set("author", author)
	}
	values.Set("size", strconv.Itoa(f.opt.PsSize))
	values.Set("sort", "desc")
	if before != 0 {
		values.Set("before", strconv.FormatInt(before, 10))
	}
	if after != 0 {
		values.Set("after", strconv.FormatInt(after, 10))
	}

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

	return data.Data, err
}

func (f *Fs) list(ctx context.Context, dirID string, data []api.Item) error {
	var (
		wg       sync.WaitGroup
		checkers = f.ci.Checkers
		in       = make(chan api.Item, checkers)
	)

	add := func(entry fs.DirEntry) {
		itemsMu.Lock()
		defer itemsMu.Unlock()
		f.dirTree.AddEntry(entry)
	}
	for i := 0; i < checkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for remote := range in {
				u, _ := url.Parse(remote.URL)
				base := path.Base(u.Path)
				if strings.HasPrefix(base, "DASH_1080.mp4") {
					_, file := path.Split(path.Dir(u.Path))
					base = file + "_" + base
				}
				jsonIn, _ := json.Marshal(&remote)
				byteArr := json.RawMessage(jsonIn)
				file := &Object{
					remote:    path.Join(dirID, f.opt.Enc.ToStandardName(base)),
					fs:        f,
					id:        remote.ID,
					remoteURL: remote.URL,
					rawData:   &byteArr,
				}
				if len(remote.Preview.Images) > 0 {
					file.sourceURL = html.UnescapeString(remote.Preview.Images[0].Source.URL)
				}
				switch err := file.stat(ctx); err {
				case nil:
					ok := false
					if file.etag != "" {
						etagsMu.Lock()
						ok = f.cache[file.etag]
						etagsMu.Unlock()
					}
					if !ok {
						file.modTime = time.Unix(remote.CreatedUtc, 0)
						add(file)
						if file.etag != "" {
							etagsMu.Lock()
							f.cache[file.etag] = true
							etagsMu.Unlock()
						}

						newFile := *file

						addNewFile := func(dir string) {
							newFile.remote = path.Join(dir, f.opt.Enc.ToStandardName(base))
							add(&newFile)
						}

						if f.opt.Mkdirs {
							if strings.HasPrefix(dirID, subredditPrefix) {
								addNewFile(userPrefix + remote.Author)
							}
							if strings.HasPrefix(dirID, userPrefix) {
								addNewFile(subredditPrefix + remote.Subreddit)
							}
						}
					} else {
						fs.Debugf(remote, "skipping because etag=%v matches", file.etag)
					}
				default:
					fs.Debugf(remote, "skipping because of error: %v", err)
				}
			}
			//entriesMu.Lock()
			//f.items[dirID] = dirItem
			//entriesMu.Unlock()
		}()
	}

	var created int64
	for _, e := range data {
		var (
			id       string
			ok       bool
			thumbURL string
		)

		if len(e.Preview.Images) > 0 {
			smallPics := f.opt.SmallPics
			id = e.Preview.Images[0].ID
			if smallPics != -1 && len(e.Preview.Images[0].Resolutions) > 0 {
				var i int = len(e.Preview.Images[0].Resolutions) - 1
				if smallPics <= i {
					i = smallPics
					thumbURL = html.UnescapeString(e.Preview.Images[0].Resolutions[i].URL)
				} else {
					thumbURL = html.UnescapeString(e.Preview.Images[0].Source.URL)
				}
			}
			etagsMu.Lock()
			ok = f.cache[id]
			etagsMu.Unlock()
		}
		if created == 0 || created > e.CreatedUtc {
			created = e.CreatedUtc
		}

		switch e.Domain {
		case "i.redd.it", "i.imgur.com":

			if !ok {
				if strings.HasSuffix(e.URL, ".gifv") {
					e.URL = strings.Replace(e.URL, ".gifv", ".mp4", -1)
				}
				etagsMu.Lock()
				f.cache[id] = true
				etagsMu.Unlock()
				if f.opt.SmallPics > -1 {
					switch e.PostHint {
					case "image":
						if !strings.HasSuffix(e.URL, ".gif") {
							e.URL = thumbURL
						}
					case "rich:video":
						fallbackURL := e.Preview.RedditVideoPreview.FallbackURL
						if fallbackURL != "" {
							e.URL = html.UnescapeString(fallbackURL)
						}
					}
				}
				in <- e
			} else {
				fs.Debugf(f, "skipping %v", e.URL)
			}

		case "redgifs.com", "gfycat.com":
			u, _ := url.Parse(e.URL)
			v := path.Base(u.Path)
			etagsMu.Lock()
			ok = f.cache[v]
			etagsMu.Unlock()
			if !ok {
				if f.opt.SmallPics != -1 {
					fallbackURL := e.Preview.RedditVideoPreview.FallbackURL
					if fallbackURL != "" {
						e.URL = html.UnescapeString(fallbackURL)
					} else {
						fallbackURL = e.Media.Oembed.ThumbnailURL
						if fallbackURL != "" {
							if strings.HasSuffix(fallbackURL, "-mobile.jpg") {
								fallbackURL = strings.Replace(fallbackURL, "-mobile.jpg", ".mp4", -1)
							}
							e.URL = fallbackURL
						} else {
							e.URL = getGfyCat(ctx, f, v)
						}
					}
				} else {
					e.URL = getGfyCat(ctx, f, v)
				}
				etagsMu.Lock()
				f.cache[v] = true
				etagsMu.Unlock()
				in <- e
			} else {
				fs.Debugf(f, "skipping %v", e.URL)
			}

		default:
			fs.Debugf(f, "ignoring %+v", e)
		}
	}
	itemsMu.Lock()
	psQ, ok := f.psQueries[dirID]
	itemsMu.Unlock()
	if !ok {
		psQ = PushshiftQuery{}
	}
	psQ.currentBefore = created
	itemsMu.Lock()
	f.psQueries[dirID] = psQ
	itemsMu.Unlock()
	//f.dirCache.Put(f.opt.Subreddit, f.opt.Subreddit)
	//d = fs.NewDir(f.opt.Subreddit, time.Unix(created, 0))
	close(in)
	wg.Wait()
	return nil
}

var commandHelp = []fs.CommandHelp{
	{
		Name:  "refresh",
		Short: "Refresh directory contents of subreddit/author specified.",
	},
}

// Command the backend to run a named command
//
// The command run is name
// args may be used to read arguments from
// opts may be used to read optional arguments from
//
// The result should be capable of being JSON encoded
// If it is a string or a []string it will be shown to the user
// otherwise it will be JSON encoded and shown to the user like that
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out interface{}, err error) {
	switch name {
	case "dump":
		out := make(map[string]string)
		if filePath, ok := opt["filePath"]; ok {
			itemsMu.Lock()
			_, entry := f.dirTree.Find(filePath)
			itemsMu.Unlock()
			if entry != nil {
				o := entry.(*Object)
				mapStructure := &map[string]string{}
				_ = json.Unmarshal(*o.rawData, mapStructure)
				out = *mapStructure
			}
		}
		return out, nil
	case "refresh":
		out := make(map[string]string)
		if user, ok := opt["user"]; ok {
			entries, _ := f.List(ctx, userPrefix+user)
			out["len_items"] = strconv.Itoa(len(entries))
		}
		return out, nil
	case "pushshift":
		if size, ok := opt["size"]; ok {
			f.opt.PsSize, err = strconv.Atoi(size)
		}
		return nil, err
	default:
		return nil, fs.ErrorCommandNotFound
	}
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
	defer fs.Debugf(f, "List(%v)", dir)
	var (
		remotePath string
	)

	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}

	switch directoryID {
	case "":
		for _, it := range []string{subredditPrefix, userPrefix} {
			remotePath = path.Join(dir, it)
			_, err := f.dirCache.FindDir(ctx, remotePath, false)
			if err == fs.ErrorDirNotFound {
				f.dirCache.Put(remotePath, remotePath)
				d := fs.NewDir(remotePath, time.Now()).SetID(it)
				itemsMu.Lock()
				f.dirTree.AddEntry(d)
				itemsMu.Unlock()
			} else if err != nil {
				return nil, err
			}
		}
		return f.dirTree[dir], nil
	}

	userDir, user := path.Split(dir)
	if userDir == "" {
		populateRootTypeDirs := func(itemType string, items string) error {
			for _, item := range strings.Split(items, ",") {
				if !strings.HasPrefix(item, itemType+"/") {
					item = itemType + "/" + item
				}
				_, err := f.dirCache.FindDir(ctx, item, false)

				if err == fs.ErrorDirNotFound {
					f.dirCache.Put(item, item)
					d := fs.NewDir(item, time.Time{}).SetID(item)
					itemsMu.Lock()
					f.dirTree.AddEntry(d)
					itemsMu.Unlock()
				}
			}
			return err
		}

		makeItems := func(user string, listStr string) error {
			itemsMu.Lock()
			keys := make([]string, 0, len(f.dirTree))
			itemsMu.Unlock()
			for k := range f.dirTree {
				if strings.HasPrefix(k, user+"/") {
					keys = append(keys, k)
				}
			}
			keysStr := strings.Join(keys, ",")
			dirs := listStr
			if keysStr != "" {
				dirs = strings.Join([]string{listStr, keysStr}, ",")
			}
			return populateRootTypeDirs(user, dirs)
		}

		switch user {
		case "u":
			err = makeItems(user, f.opt.Author)
		case "r":
			err = makeItems(user, f.opt.Subreddit)
		}
	} else {
		itemsMu.Lock()
		dirTreeLen := f.dirTree[dir].Len()
		itemsMu.Unlock()
		if dirTreeLen == 0 {
			err = refreshDir(ctx, f, dir)
		}
	}

	itemsMu.Lock()
	dirTree := f.dirTree[dir]
	_, entry := f.dirTree.Find(dir)

	switch entry.(type) {
	case fs.Directory:
		d := entry.(*fs.Dir)
		d.SetItems(int64(dirTree.Len()))
	}

	itemsMu.Unlock()
	if err == errorMinBefore {
		err = nil
	}
	return dirTree, err
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorNotImplemented
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorNotImplemented
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	if len(strings.Split(strings.Trim(pathID, "/"), "/")) < 1 {
		return pathID + leaf, true, nil
	}

	itemsMu.Lock()
	_, ok := f.dirTree[pathID+"/"+leaf]
	itemsMu.Unlock()
	if ok {
		return pathID + "/" + leaf, true, nil
	}
	return "", false, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	//fs.NewDir(pathID+leaf, time.Now()).SetID(leaf)
	return pathID + "/" + leaf, nil
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
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

// Metadata returns all file metadata provided by Internet Archive
func (o *Object) Metadata(ctx context.Context) (m fs.Metadata, err error) {
	if o.rawData == nil {
		return nil, nil
	}
	raw := make(map[string]json.RawMessage)
	err = json.Unmarshal(*o.rawData, &raw)
	if err != nil {
		// fatal: json parsing failed
		return
	}
	for k, v := range raw {
		items, err := listOrString(v)
		if len(items) == 0 || err != nil {
			// skip: an entry failed to parse
			continue
		}
		m.Set(k, items[0])
	}
	return
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

func listOrString(jm json.RawMessage) (rmArray []string, err error) {
	// rclone-metadata can be an array or string
	// try to deserialize it as array first
	err = json.Unmarshal(jm, &rmArray)
	if err != nil {
		// if not, it's a string
		dst := new(string)
		err = json.Unmarshal(jm, dst)
		if err == nil {
			rmArray = []string{*dst}
		}
	}
	return
}

// url returns the native url of the object
func (o *Object) url() string {
	return o.remoteURL
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
	//defer log.Trace(o, "o.url=%v", o.url())
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
	o.etag = strings.Trim(res.Header.Get("etag"), "\"")
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
	_, err := f.dirCache.FindDir(ctx, dir, false)
	if err == fs.ErrorDirNotFound {
		f.dirCache.Put(dir, dir)
		d := fs.NewDir(dir, time.Time{}).SetID(dir)
		itemsMu.Lock()
		f.dirTree.AddEntry(d)
		itemsMu.Unlock()
		err = nil
	}
	return err
}

// Remove a remote http file object
func (o *Object) Remove(ctx context.Context) error {
	return fs.ErrorNotImplemented
}

// Rmdir removes the root directory of the Fs object
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	itemsMu.Lock()
	err = f.dirTree.Prune(map[string]bool{dir: true})
	f.dirCache.FlushDir(dir)
	itemsMu.Unlock()
	return err
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return fs.ErrorNotImplemented
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.contentType
}

// ChangeNotify calls the passed function with a path that has had changes.
// If the implementation uses polling, it should adhere to the given interval.
//
// Automatically restarts itself in case of unexpected behaviour of the remote.
//
// Close the returned channel to stop being notified.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	go func() {
		var ticker *time.Ticker
		var tickerC <-chan time.Time
		for {
			select {
			case pollInterval, ok := <-pollIntervalChan:
				if !ok {
					if ticker != nil {
						ticker.Stop()
					}
					return
				}
				if pollInterval == 0 {
					if ticker != nil {
						ticker.Stop()
						ticker, tickerC = nil, nil
					}
				} else {
					ticker = time.NewTicker(pollInterval)
					tickerC = ticker.C
				}
			case <-tickerC:
				f.changeNotifyRunner(ctx, notifyFunc)
			}
		}
	}()
}

func (f *Fs) changeNotifyRunner(ctx context.Context, notifyFunc func(string, fs.EntryType)) {
	var (
		wg       sync.WaitGroup
		checkers = f.ci.Checkers
	)
	in := make(chan string, checkers)

	fs.Debugf(f, "Checking for changes on remote ")

	for i := 0; i < checkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for dir := range in {
				err := refreshDir(ctx, f, dir)
				if err == nil {
					notifyFunc(dir, fs.EntryDirectory)
				}
			}
		}()
	}

	itemsMu.Lock()
	dirs := f.dirTree.Dirs()
	itemsMu.Unlock()
	for _, dir := range dirs {
		itemsMu.Lock()
		parentPath, _ := f.dirTree.Find(dir)
		itemsMu.Unlock()
		if parentPath == "u" || parentPath == "r" {
			in <- dir
		}
	}
	close(in)
	wg.Wait()

	/*err = f.pacer.CallNoRetry(func() (bool, error) {
		for _, item := range strings.Split(f.opt.Author, ",") {
			go func(key string) {
				notifyFunc(key, fs.EntryDirectory)
			}(item)
		}
		for _, item := range strings.Split(f.opt.Subreddit, ",") {
			go func(key string) {
				notifyFunc(key, fs.EntryDirectory)
			}(item)
		}
		return false, err
	})*/
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)
