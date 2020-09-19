from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.get("/api/static_settings", name="api_static_settings")
def get_static_settings():
    search = [v for _, v in request.query.items()]

    # big nasty global list of every simple combo-boxable list in the entire
    # application.
    settings_map = {
        "account_types": ("atype_name, id", "hacc.accounttypes", "sort"),
        "journals": ("jrn_name, id", "hacc.journals", "jrn_name"),
    }

    results = api.Results()
    with app.dbconn() as conn:
        for v in search:
            columns, table, sort = settings_map[v]
            select = "select {} from {} order by {}".format(columns, table, sort)
            results.tables[v] = api.sql_tab2(conn, select)
    return results.json_out()
