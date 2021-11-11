package reporter

type TestCase struct {
	Markdown string
	Tables   map[string]*Table
	Pictures map[string]string
}

type Page struct {
}
