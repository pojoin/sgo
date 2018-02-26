package dbhpr

type Page struct {
	PageNo   int   `json:"pageNo"`
	PageSize int   `json:"pageSize"`
	Count    int64 `json:"count"`
	List     []Row `json:"list"`
}

func NewPage(pageNo, pageSize int) *Page {
	page := &Page{}
	if pageNo == 0 {
		page.PageNo = 1
	} else {
		page.PageNo = pageNo
	}

	if pageSize == 0 {
		page.PageSize = 15
	} else {
		page.PageSize = pageSize
	}
	return page
}

func (p *Page) StartRow() int {
	return p.PageSize * (p.PageNo - 1)
}

func (p *Page) PageCount() int {
	pageCount := 0
	if int(p.Count)%p.PageSize == 0 {
		pageCount = int(p.Count) / p.PageSize
	} else {
		pageCount = int(p.Count)/p.PageSize + 1
	}
	return pageCount
}
