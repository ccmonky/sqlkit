package sqlkit

import (
	"database/sql"
	"net/http"

	"github.com/ccmonky/render"
)

type API struct {
	PathPrefix string `json:"path_prefix,omitempty"`
	DB         *sql.DB
}

func (api API) Stats(w http.ResponseWriter, r *http.Request) {
	stats := api.DB.Stats()
	render.OK(w, r, stats)
}

func (api API) Locks(w http.ResponseWriter, r *http.Request) {

}
