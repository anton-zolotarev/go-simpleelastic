package simpleelastic

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/anton-zolotarev/go-simplejson"
)

type method int
type mode int
type action int

const (
	_REQ_GET method = iota + 1
	_REQ_POST
)

const (
	_M_SINGLE mode = iota + 1
	_M_MULTI
)

const (
	_ACT_SEARCH action = iota + 1
	_ACT_INDEX_OPEN
	_ACT_INDEX_CLOSE
)

type Conn struct {
	host   string
	inlog  io.Writer
	outlog io.Writer
	errlog io.Writer
}

type query struct {
	mode
	name   string
	js     *simplejson.Json
	req    *request
	parent *query
}

type request struct {
	method
	action
	scroll   *string
	isscroll bool
	limit    int
	index    []string
	conn     *Conn
	query
}

type responce struct {
	req     *request
	session *string
	total   int
	len     int
	count   int
	index   int
	curr    *simplejson.Json
	hits    *simplejson.Json
}
type Responce struct {
	resp *responce
}

type RecGet interface {
	Query() Query
	Index(val ...string) RecGet
	Scroll(val string, limit int) RecGet
	Do() (*Responce, error)
}

type Query interface {
	Size(size int) Query
	From(from int) Query
	Source(field ...string) Query
	Term(key string, val ...interface{}) Query
	Range(key string) Range
	Sort(key string) Sort
	Bool() Bool
	BoolNstd() Bool

	Query() Query
}

type Bool interface {
	Must() Query
	MustNot() Query
	Should() Query
	Filter() Query
	BoolNstd() Bool
}

type Range interface {
	Gte(val interface{}) Range
	Lte(val interface{}) Range
	Qt(val interface{}) Range
	Lt(val interface{}) Range
	Format(format string) Range
	TimeZone(val string) Range

	Range(key string) Range
	Bool() Bool
	Query() Query
}

type Sort interface {
	Order(val string) Sort
	Mode(val string) Sort

	Sort(key string) Sort
	Query() Query
}

func Open(host string) *Conn {
	return &Conn{host: host}
}

func (c *Conn) newRequest(mtd method, act action) *request {
	var req request
	req.conn = c
	req.method = mtd
	req.action = act
	req.mode = _M_SINGLE
	req.index = []string{"_all"}
	req.js = simplejson.New()
	req.query.name = "root"
	req.req = &req
	return &req
}

func (c *Conn) indexAct(act action, val ...string) error {
	rec := c.newRequest(_REQ_POST, _ACT_INDEX_OPEN)
	rec.index = val
	_, err := rec.Do()
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) IndexOpen(val ...string) error {
	return c.indexAct(_ACT_INDEX_OPEN, val...)
}

func (c *Conn) IndexClose(val ...string) error {
	return c.indexAct(_ACT_INDEX_CLOSE, val...)
}

func (c *Conn) Get() RecGet {
	return c.newRequest(_REQ_GET, _ACT_SEARCH)
}

func (c *Conn) SetInLog(log io.Writer) {
	c.inlog = log
}

func (c *Conn) SetOutLog(log io.Writer) {
	c.outlog = log
}

func (c *Conn) SetErrLog(log io.Writer) {
	c.errlog = log
}

func (r *Responce) Total() int {
	return r.resp.total
}

func (r *Responce) Len() int {
	return r.resp.len
}

func (r *Responce) Next() bool {
	for {
		if r.resp.index < r.resp.len && (r.resp.req.limit == 0 || r.resp.count < r.resp.req.limit) {
			r.resp.curr = r.resp.hits.GetIndex(r.resp.index)
			r.resp.index++
			r.resp.count++
			return true
		}

		if r.resp.session != nil && r.resp.count < r.resp.total && r.resp.count < r.resp.req.limit {
			rec := r.resp.req.conn.newRequest(r.resp.req.method, _ACT_SEARCH)
			rec.isscroll = true
			rec.limit = r.resp.req.limit
			rec.scroll = r.resp.req.scroll

			rec.js.Set("scroll", *rec.scroll)
			rec.js.Set("scroll_id", *r.resp.session)
			rs, err := rec.Do()

			if err != nil || rs.Len() == 0 {
				return false
			}

			rs.resp.count = r.resp.count
			rs.resp.total = r.resp.total
			r.resp = rs.resp
			continue
		}
		return false
	}
}

func (r *Responce) Source() *simplejson.Json {
	return r.resp.curr.Get("_source")
}

func (r *Responce) Scan() {

}

func (rt *request) String() string {
	d, _ := rt.js.EncodePretty()
	return string(d)
}

func (rt *request) Index(val ...string) RecGet {
	rt.index = val
	return rt
}

func (rt *request) Scroll(val string, limit int) RecGet {
	rt.scroll = &val
	rt.limit = limit
	return rt
}

func (q *query) Do() (*Responce, error) {
	req := q.req
	var url bytes.Buffer
	url.WriteString(req.conn.host)
	if !req.isscroll {
		url.WriteString("/")
		url.WriteString(strings.Join(req.index, ","))
	}

	switch req.action {
	case _ACT_SEARCH:
		url.WriteString("/_search")
	case _ACT_INDEX_OPEN:
		url.WriteString("/_open")
	case _ACT_INDEX_CLOSE:
		url.WriteString("/_close")
	}

	if req.isscroll {
		url.WriteString("/scroll")
	} else if req.scroll != nil {
		url.WriteString("?scroll=")
		url.WriteString(*req.scroll)
	}

	var body bytes.Buffer
	if req.action == _ACT_SEARCH {
		data, _ := q.js.Encode()
		body.Write(data)
	}

	if req.conn.outlog != nil {
		fmt.Fprintln(req.conn.outlog, url.String())
		fmt.Fprintln(req.conn.outlog, body.String())
	}

	var tp string
	switch req.method {
	case _REQ_POST:
		tp = "POST"
	default:
		tp = "GET"
	}

	cl := &http.Client{}
	rhttp, err := http.NewRequest(tp, url.String(), &body)
	if err != nil {
		if req.conn.errlog != nil {
			fmt.Fprintln(req.conn.outlog, err.Error())
		}
		return nil, err
	}

	elps := time.Now()
	res, err := cl.Do(rhttp)

	if req.conn.outlog != nil {
		fmt.Fprintln(req.conn.outlog, "query time: ", time.Now().Sub(elps))
	}

	if err != nil {
		if req.conn.errlog != nil {
			fmt.Fprintln(req.conn.outlog, err.Error())
		}
		return nil, err
	}
	defer res.Body.Close()

	js, err := simplejson.NewFromReader(res.Body)
	if err != nil {
		if req.conn.errlog != nil {
			fmt.Fprintln(req.conn.outlog, err.Error())
		}
		return nil, err
	}

	if req.conn.inlog != nil {
		out, _ := js.EncodePretty()
		fmt.Fprintln(req.conn.inlog, string(out))
	}

	if str, err := js.GetPath("error", "reason").String(); err == nil {
		if req.conn.errlog != nil {
			fmt.Fprintln(req.conn.errlog, str)
		}
		return nil, errors.New(str)
	}

	out := &Responce{resp: &responce{req: req, total: js.GetPath("hits", "total").MustInt(), hits: js.GetPath("hits", "hits")}}
	out.resp.len = len(out.resp.hits.MustArray())

	if id, err := js.Get("_scroll_id").String(); err == nil {
		out.resp.session = &id
	}

	return out, nil
}

func (q *query) String() string {
	d, _ := q.js.EncodePretty()
	return string(d)
}

func (q *query) itemAdd(key string, js *simplejson.Json) {
	if q.mode == _M_SINGLE {
		q.js.Set(key, js)
	} else if q.mode == _M_MULTI {
		b := simplejson.New()
		b.Set(key, js)
		q.js.AddArray(b)
	}
}

func (q *query) itemMap(name string) *query {
	item := query{js: simplejson.New(), mode: _M_SINGLE, name: name, req: q.req, parent: q}
	q.itemAdd(name, item.js)
	return &item
}

func (q *query) itemArray(name string, c int) *query {
	item := query{js: simplejson.NewArray(c), mode: _M_MULTI, name: name, req: q.req, parent: q}
	q.itemAdd(name, item.js)
	return &item
}

func (q *query) backByName(name string) (*query, bool) {
	for q.name != name && q.parent != nil {
		q = q.parent
	}
	if q.name == name {
		return q, true
	}
	return nil, false
}

func (q *query) Size(size int) Query {
	q.req.js.Set("size", size)
	return q
}

func (q *query) From(from int) Query {
	q.req.js.Set("from", from)
	return q
}

func (q *query) Source(field ...string) Query {
	js := simplejson.NewArray(len(field))
	for n := range field {
		js.AddArray(field[n])
	}
	q.req.js.Set("_source", js)
	return q
}

func (q *query) Query() Query {
	fn := "query"
	if item, f := q.backByName(fn); f {
		return item
	}
	return q.itemMap(fn)
}

func (q *query) Bool() Bool {
	fn := "bool"
	if item, f := q.backByName(fn); f {
		return item
	}
	return q.itemMap(fn)
}

func (q *query) BoolNstd() Bool {
	fn := "bool"
	return q.itemMap(fn)
}

func (q *query) Must() Query {
	return q.itemArray("must", 1)
}

func (q *query) MustNot() Query {
	return q.itemArray("must_not", 1)
}

func (q *query) Should() Query {
	return q.itemArray("should", 1)
}

func (q *query) Filter() Query {
	return q.itemArray("filter", 1)
}

func (q *query) Term(key string, val ...interface{}) Query {
	js := simplejson.New()

	var fn string
	if len(val) > 1 {
		fn = "terms"
		js.Set(key, val)
	} else {
		fn = "term"
		js.Set(key, val[0])
	}

	q.itemAdd(fn, js)
	return q
}

func (q *query) Range(key string) Range {
	fn := "range"
	item, f := q.backByName(fn)
	if !f {
		item = q.itemMap(fn)
	}
	return item.itemMap(key)
}

func (q *query) Gte(val interface{}) Range {
	q.js.Set("gte", val)
	return q
}

func (q *query) Lte(val interface{}) Range {
	q.js.Set("lte", val)
	return q
}

func (q *query) Qt(val interface{}) Range {
	q.js.Set("qt", val)
	return q
}

func (q *query) Lt(val interface{}) Range {
	q.js.Set("lt", val)
	return q
}

func (q *query) Format(format string) Range {
	q.js.Set("format", format)
	return q
}

func (q *query) TimeZone(val string) Range {
	q.js.Set("time_zone", val)
	return q
}

func (q *query) Sort(key string) Sort {
	fn := "sort"
	item, f := q.backByName(fn)
	if !f {
		item, _ = q.backByName("root")
		item = item.itemArray(fn, 1)
	}
	return item.itemMap(key)
}

func (q *query) Order(val string) Sort {
	q.js.Set("order", val)
	return q
}

func (q *query) Mode(val string) Sort {
	q.js.Set("mode", val)
	return q
}
