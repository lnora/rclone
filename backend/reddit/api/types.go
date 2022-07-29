package api

import (
)

type ListItems struct {
	Data []Item `json:"data"`
}

type Item struct {
	Author     string `json:"author"`
	CreatedUtc int64  `json:"created_utc"`
	Id         string `json:"id"`
	PostHint   string `json:"post_hint"`
	Url        string `json:"url"`
}