package api

type ListItems struct {
	Data []Item `json:"data"`
}

type Item struct {
	Author     string `json:"author"`
	Subreddit  string `json:"subreddit"`
	CreatedUtc int64  `json:"created_utc"`
	Id         string `json:"id"`
	Domain     string `json:"domain"`
	PostHint   string `json:"post_hint"`
	Url        string `json:"url"`
	Preview    struct {
		Images []struct {
			ID string `json:"id"`
		} `json:"images"`
	} `json:"preview"`
}
