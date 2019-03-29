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
type group int

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
	_ACT_SCROLL
	_ACT_INDEX_OPEN
	_ACT_INDEX_CLOSE
	_ACT_INDEX_CHECK
)

const (
	_GR_ROOT group = (1 << iota)
	_GR_CHILD
	_GR_QUERY
	_GR_BOOL
	_GR_COND
	_GR_RANGE
	_GR_SORT
)

type Conn struct {
	host   string
	inlog  io.Writer
	outlog io.Writer
	errlog io.Writer
}

type query struct {
	mode
	group
	name   string
	js     *simplejson.Json
	req    *request
	parent *query
}

type request struct {
	method
	action
	scroll *string
	limit  int
	index  []string
	conn   *Conn
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
	DoRetry(timeout time.Duration, count int) (resp *Responce, err error)
}

type Query interface {
	Size(size int) Query
	From(from int) Query
	Source(field ...string) Query
	Sort(key string) Sort

	Cond
}

type Bool interface {
	Must() Cond
	MustNot() Cond
	Should() Cond
	Filter() Cond
}

type Term interface {
	Cond
	Bool
}

type Value interface {
	Value(val interface{}) Value
	Boost(val float32) Value
}

type Cond interface {
	Term(key string, val ...interface{}) Term
	Fuzzy(key string, val string) Term
	Regexp(key string, val string) Term
	Wildcard(key string, val string) Term
	Exists(key string) Term
	Range(key string) Range
	Bool() Bool

	BackQuery() Query
	BackBool() Bool
}

type Range interface {
	Gte(val interface{}) Range
	Lte(val interface{}) Range
	Qt(val interface{}) Range
	Lt(val interface{}) Range
	Format(format string) Range
	TimeZone(val string) Range

	Range(key string) Range

	BackQuery() Query
	BackBool() Bool
}

type Sort interface {
	Order(val string) Sort
	Mode(val string) Sort

	Sort(key string) Sort

	BackQuery() Query
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
	req.group = _GR_ROOT
	req.index = []string{"_all"}
	req.js = simplejson.New()
	req.query.name = "root"
	req.req = &req
	return &req
}

func (c *Conn) indexAct(act action, val ...string) (*Responce, error) {
	if len(val) == 0 {
		return nil, errors.New("indexAct error: empty index list")
	}

	mtd := _REQ_POST
	if act == _ACT_INDEX_CHECK {
		mtd = _REQ_GET
	}
	rec := c.newRequest(mtd, act)
	rec.index = val
	return rec.Do()
}

func (c *Conn) IndexOpen(val ...string) error {
	_, err := c.indexAct(_ACT_INDEX_OPEN, val...)
	return err
}

func (c *Conn) IndexClose(val ...string) error {
	_, err := c.indexAct(_ACT_INDEX_CLOSE, val...)
	return err
}

func (c *Conn) IndexCheck(val ...string) (*Responce, error) {
	return c.indexAct(_ACT_INDEX_CHECK, val...)
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

		if r.resp.req.conn.outlog != nil {
			fmt.Fprintln(r.resp.req.conn.outlog,
				"count:", r.resp.count,
				"limit:", r.resp.req.limit,
				"total:", r.resp.total,
				"\n")
		}

		if r.resp.session != nil && r.resp.count < r.resp.total && r.resp.count < r.resp.req.limit {
			rec := r.resp.req.conn.newRequest(r.resp.req.method, _ACT_SCROLL)
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
	if len(val) != 0 {
		rt.index = val
	}
	return rt
}

func (rt *request) Scroll(val string, limit int) RecGet {
	rt.scroll = &val
	rt.limit = limit
	return rt
}

func (q *query) DoRetry(timeout time.Duration, count int) (resp *Responce, err error) {
	iter := 0
	for {
		resp, err = q.Do()
		if err == nil || iter >= count {
			return
		}
		time.Sleep(timeout)
		iter++
	}
}

func (q *query) Do() (*Responce, error) {
	req := q.req
	var url bytes.Buffer
	var body bytes.Buffer

	url.WriteString(req.conn.host)

	if req.action == _ACT_INDEX_CHECK {
		url.WriteString("/_cluster/state/metadata")
	}

	if req.action != _ACT_SCROLL {
		url.WriteString("/")
		url.WriteString(strings.Join(req.index, ","))
	}

	switch req.action {
	case _ACT_SEARCH:
		url.WriteString("/_search")
		if req.scroll != nil {
			url.WriteString("?scroll=")
			url.WriteString(*req.scroll)
		}
	case _ACT_SCROLL:
		url.WriteString("/_search/scroll")
	case _ACT_INDEX_OPEN:
		url.WriteString("/_open")
	case _ACT_INDEX_CLOSE:
		url.WriteString("/_close")
	}

	if m, f := req.js.Map(); f == nil && len(m) > 0 {
		data, _ := req.js.Encode()
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

	out := &Responce{resp: &responce{req: req}}
	switch req.action {
	case _ACT_SCROLL:
		fallthrough
	case _ACT_SEARCH:
		out.resp.total = js.GetPath("hits", "total").MustInt()
		out.resp.hits = js.GetPath("hits", "hits")
		out.resp.len = len(out.resp.hits.MustArray())

		if id, err := js.Get("_scroll_id").String(); err == nil {
			out.resp.session = &id
		}
	case _ACT_INDEX_CHECK:
		if ind, err := js.GetPath("metadata", "indices").Map(); err == nil {
			out.resp.total = len(ind)
			for _, index := range ind {
				if simplejson.Wrap(index).Get("state").MustString() == "open" {
					out.resp.len++
				}
			}
		}
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

func (q *query) itemMap(name string, gr group) *query {
	item := query{js: simplejson.New(), mode: _M_SINGLE, group: gr, name: name, req: q.req, parent: q}
	q.itemAdd(name, item.js)
	return &item
}

func (q *query) itemArray(name string, gr group, c int) *query {
	item := query{js: simplejson.NewArray(c), mode: _M_MULTI, group: gr, name: name, req: q.req, parent: q}
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

func (q *query) backByGroup(gr group) (*query, bool) {
	for q.group&gr == 0 && q.parent != nil {
		q = q.parent
	}
	if q.group&gr > 0 {
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
	return q.itemMap("query", _GR_QUERY)
}

func (q *query) BackQuery() Query {
	if item, f := q.backByGroup(_GR_QUERY); f {
		return item
	}
	return q.Query()
}

func (q *query) Bool() Bool {
	return q.itemMap("bool", _GR_BOOL)
}

func (q *query) BackBool() Bool {
	if item, f := q.backByGroup(_GR_BOOL); f {
		return item
	}
	return q.Bool()
}

func (q *query) Must() Cond {
	q, f := q.backByGroup(_GR_BOOL)
	if !f {
		// return q
	}
	return q.itemArray("must", _GR_COND, 1)
}

func (q *query) MustNot() Cond {
	q, f := q.backByGroup(_GR_BOOL)
	if !f {
		return q
	}
	return q.itemArray("must_not", _GR_COND, 1)
}

func (q *query) Should() Cond {
	q, f := q.backByGroup(_GR_BOOL)
	if !f {
		return q
	}
	return q.itemArray("should", _GR_COND, 1)
}

func (q *query) Filter() Cond {
	q, f := q.backByGroup(_GR_BOOL)
	if !f {
		return q
	}
	return q.itemArray("filter", _GR_COND, 1)
}

func (q *query) Term(key string, val ...interface{}) Term {
	fn := "term"
	js := simplejson.New()
	if len(val) == 1 {
		js.Set(key, val[0])
	} else {
		fn = "terms"
		js.Set(key, val)
	}

	q.itemAdd(fn, js)
	return q
}

func (q *query) Fuzzy(key string, val string) Term {
	js := simplejson.New()
	js.Set(key, val)
	q.itemAdd("fuzzy", js)
	return q
}

func (q *query) Regexp(key string, val string) Term {
	js := simplejson.New()
	js.Set(key, val)
	q.itemAdd("regexp", js)
	return q
}

func (q *query) Wildcard(key string, val string) Term {
	js := simplejson.New()
	js.Set(key, val)
	q.itemAdd("wildcard", js)
	return q
}

func (q *query) Exists(key string) Term {
	js := simplejson.New()
	js.Set("field", key)
	q.itemAdd("exists", js)
	return q
}

func (q *query) Value(val interface{}) Value {
	q.js.Set("value", val)
	return q
}

func (q *query) Boost(val float32) Value {
	q.js.Set("boost", val)
	return q
}

func (q *query) Range(key string) Range {
	item, f := q.backByGroup(_GR_RANGE | _GR_COND | _GR_QUERY)
	if !f || item.group != _GR_RANGE {
		item = item.itemMap("range", _GR_RANGE)
	}
	return item.itemMap(key, _GR_CHILD)
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
	item, f := q.backByGroup(_GR_SORT | _GR_ROOT)
	if !f || item.group != _GR_SORT {
		item = item.itemArray("sort", _GR_SORT, 1)
	}
	return item.itemMap(key, _GR_CHILD)
}

func (q *query) Order(val string) Sort {
	q.js.Set("order", val)
	return q
}

func (q *query) Mode(val string) Sort {
	q.js.Set("mode", val)
	return q
}
